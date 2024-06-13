// Author: Ming Zhang
// Copyright (c) 2022

#include "worker/worker.h"

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>

#include "allocator/buffer_allocator.h"
#include "connection/qp_manager.h"
#include "util/fast_random.h"

using namespace std::placeholders;

// All the functions are executed in each thread
std::mutex mux;

extern std::atomic<uint64_t> tx_id_generator;
extern std::atomic<uint64_t> connected_t_num;

__thread uint64_t seed;                       // Thread-global random seed
__thread FastRandom *random_generator = NULL; // Per coroutine random generator
__thread t_id_t thread_gid;
__thread t_id_t thread_local_id;
__thread t_id_t thread_num;
__thread uint64_t thread_kv_offset;
__thread int search_failed;

__thread MetaManager *meta_man;
__thread QPManager *qp_man;

__thread RDMABufferAllocator *rdma_buffer_allocator;

__thread coro_id_t coro_num;
__thread CoroutineScheduler *coro_sched; // Each transaction thread has a coroutine scheduler
__thread bool stop_run;

// Performance measurement (thread granularity)

__thread uint64_t stat_attempted_total = 0;    // Issued transaction number
__thread uint64_t stat_committed_tx_total = 0; // Committed transaction number
const coro_id_t POLL_ROUTINE_ID = 0;           // The poll coroutine ID
__thread Key_t *key_array;
__thread Value_t *value_array;
__thread char *method_array;
__thread size_t num_per_thread;
__thread size_t start_num;
__thread size_t kv_correct;
__thread size_t f_insert, s_insert;
__thread long *t_latency;

// Coroutine 0 in each thread does polling
void PollCompletion(coro_yield_t &yield)
{
  while (true)
  {
    coro_sched->PollCompletion(); // 获取ACK
    Coroutine *next = coro_sched->coro_head->next_coro;
    if (next->coro_id != POLL_ROUTINE_ID) // 若非本身则执行
    {
      // RDMA_LOG(DBG) << "Coro 0 yields to coro " << next->coro_id;
      coro_sched->RunCoroutine(yield, next); // 控制权转给next
    }
    if (stop_run)
      break;
  }
}

void InitDir(Directory *cache_dir, size_t depth)
{
  IndexMeta index_meta = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID);
  node_id_t remote_node_id = meta_man->GetPrimaryNodeID(DEFAULT_ID);
  RCQP *qp = qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
  size_t init_level = (pow(SEG_NUM, depth) - 1) / (SEG_NUM - 1);
  char *depth_buf = rdma_buffer_allocator->Alloc(sizeof(uint32_t));
  char *level_buf = rdma_buffer_allocator->Alloc(sizeof(Level));
RE_INIT:
  coro_sched->RDMAReadSync(0, qp, depth_buf, meta_man->dir_offset, sizeof(uint32_t));
  cache_dir->pool_used = *((uint32_t *)depth_buf);
  if (cache_dir->pool_used < init_level || cache_dir->pool_used > MAX_LEVEL)
  {
    goto RE_INIT;
  }

  for (uint64_t i = 0; i < cache_dir->pool_used; i++)
  {
    int re = 0;
  RE_TRY:
    coro_sched->RDMAReadSync(0, qp, level_buf, meta_man->dir_offset + 8 + i * sizeof(Level), sizeof(Level));
    memcpy((void *)(&cache_dir->level[i]), level_buf, sizeof(Level));
    if (memcmp((void *)&cache_dir->level[i], (void *)level_buf, sizeof(Level)) != 0)
    {
      if (re < 1000)
      {
        re++;
        goto RE_TRY;
      }
      else
      {
        cout << "error!" << endl;
        goto RE_TRY;
      }
    }
  }
  // if (cache_dir->global_depth != before_gd)
  //   goto RE_INIT;
}

void get_real_time_info(thread_params *params, bool *run)
{
  uint64_t before_finish_num = params->finish_num;
  uint64_t finished;
  while (*run)
  {
    usleep(250000);
    finished = 0;
    for (int i = 0; i < params[0].thread_num_per_machine; i++)
    {
      finished += params[i].finish_num;
    }
    finished = finished - before_finish_num;
    cout << finished << endl;
    before_finish_num += finished;
  }
}

void get_real_space_info(thread_params *params, bool *run)
{
  while (params->cache_dir == 0)
    ;
  uint64_t used = 0;
  // init used
  while (params->cache_dir->level[used].ls_point[0] != 0)
  {
    used++;
  }
  while (*run)
  {
    if (params->finish_num % 1000000UL == 0)
    {
      while (params->cache_dir->level[used].ls_point[0] != 0)
      {
        used++;
      }
      cout << "insert: " << params->finish_num << " used: " << params->cache_dir->pool_used << " used space: " << params->cache_dir->pool_used * sizeof(Level) << " B" << endl;
    }
  }
}

void get_predictor_info(thread_params *params, bool *run)
{
  if (params->finish_num == 0)
  {
#ifdef PARTIAL_SPLIT
    cout << "insert: " << params->finish_num << " partial-read size: " << params->partial_size << " partial-hit cnt: " << params->hit_cnt << " total kv cnt: " << params->total_cnt << endl;
#else
    cout << "insert: " << params->finish_num << " read size: " << params->full_size << " total kv cnt: " << params->total_cnt << endl;
#endif
  }
  while (*run)
  {
    if (params->finish_num % 1000000UL == 0 && params->finish_num != 0)
    {
#ifdef PARTIAL_SPLIT
      cout << "insert: " << params->finish_num << " partial-read size: " << params->partial_size << " partial-hit cnt: " << params->hit_cnt << " total kv cnt: " << params->total_cnt << endl;
#else
      cout << "insert: " << params->finish_num << " read size: " << params->full_size << " total kv cnt: " << params->total_cnt << endl;
#endif
    }
  }
}

void SetAndWaitOhterClient(size_t compute_node_id_num)
{
  IndexMeta index_meta = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID);
  node_id_t remote_node_id = meta_man->GetPrimaryNodeID(DEFAULT_ID);
  RCQP *qp = qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
RE_INIT:
  size_t NowSize = sizeof(uint64_t);
  char *data_buf = rdma_buffer_allocator->Alloc(NowSize);
  size_t cmp;
  do
  {
    cmp = *(size_t *)data_buf;
    cout << "retry:" << cmp << "compute_node_id" << compute_node_id_num << endl;
    coro_sched->RDMACASSync(0, qp, data_buf, meta_man->count_offset, cmp, cmp + 1);
    cout << "cmp:" << cmp << "  cmp+1:" << cmp + 1 << "   data_buf:" << *(size_t *)data_buf << " offset:" << meta_man->count_offset << endl;
  } while (*(size_t *)data_buf != cmp);
  // cout << cmp << "\t" << compute_node_id_num << endl;
  if (cmp == compute_node_id_num - 1)
  {
    cout << "success cmp:" << cmp << "compute_node_id" << compute_node_id_num << endl;
    return; // all start
  }
  do
  {
    coro_sched->RDMAReadSync(0, qp, data_buf, meta_man->count_offset, NowSize);
  } while (*(size_t *)data_buf != compute_node_id_num);
  // cout << "return" << *(size_t *)data_buf << endl;
}

void LoadRACE(coro_yield_t &yield, coro_id_t coro_id, Directory *cache_dir, size_t *local_level_lock, thread_params *params)
{

  // Each coroutine has a race: Each coroutine is a coordinator
  RACE *race = new RACE(meta_man,
                        qp_man,
                        thread_gid,
                        coro_id,
                        coro_sched,
                        rdma_buffer_allocator,
                        thread_kv_offset,
                        thread_num,
                        cache_dir,
                        local_level_lock);
  char r_value[32];
  race->update_size = &(params->update_size);
  race->cas_size = &(params->cas_size);
  race->read_size = &(params->read_size);
  race->write_size = &(params->write_size);

  race->partial_size = &(params->partial_size);
  race->full_size = &(params->full_size);
  race->hit_cnt = &(params->hit_cnt);
  race->total_cnt = &(params->total_cnt);

  struct timespec msr_start, msr_end;
  while (true)
  {
    int locate = start_num + stat_attempted_total++;
    // if (*local_level_lock == true)
    // if (locate % 10000000 == 0 && locate != start_num)
    // if (locate == 3780422)
    // {
    //   cout << "thread:" << thread_gid << "\tfinish_num:" << locate << endl;
    // }
#ifdef TEST_LATENCY
    clock_gettime(CLOCK_REALTIME, &msr_start);
#endif
    if (race->Insert(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num + kv_correct) == 2)
    {
      f_insert++;
    }
    else
    {
      s_insert++;
    }
#ifdef TEST_LATENCY
    clock_gettime(CLOCK_REALTIME, &msr_end);
    t_latency[locate] = (msr_end.tv_sec - msr_start.tv_sec) * 1000000000 + (msr_end.tv_nsec - msr_start.tv_nsec);
#endif
    if (stat_attempted_total >= num_per_thread)
    {
      break;
    }
    params->finish_num++;
  }
  coro_sched->finish++;
  if (coro_sched->finish == coro_num - 1)
  {

    stop_run = true;
  }
  coro_sched->FinishYield(yield, coro_id);
  return;
}

void RunRACE(coro_yield_t &yield, coro_id_t coro_id, Directory *cache_dir, size_t *local_level_lock, thread_params *params)
{

  // Each coroutine has a race: Each coroutine is a coordinator
  RACE *race = new RACE(meta_man,
                        qp_man,
                        thread_gid,
                        coro_id,
                        coro_sched,
                        rdma_buffer_allocator,
                        thread_kv_offset,
                        thread_num,
                        cache_dir,
                        local_level_lock);
  char r_value[VALUE_LENGTH];
  race->update_size = &(params->update_size);
  race->cas_size = &(params->cas_size);
  race->read_size = &(params->read_size);
  race->write_size = &(params->write_size);

  race->partial_size = &(params->partial_size);
  race->full_size = &(params->full_size);
  race->hit_cnt = &(params->hit_cnt);
  race->total_cnt = &(params->total_cnt);
  struct timespec msr_start, msr_end;
  while (true)
  {
    int locate = start_num + stat_attempted_total++;
    // if (locate % 100000 == 0)
    // if (locate == 3780422)
    // cout << "thread:" << thread_gid << "\tfinish_num:" << locate << endl;
    // race->Insert(yield, key_array[start_num + stat_attempted_total++], value_array[start_num + stat_attempted_total], KEY_LENGTH, VALUE_LENGTH, stat_attempted_total);
#ifdef TEST_LATENCY
    clock_gettime(CLOCK_REALTIME, &msr_start);
#endif
    switch (method_array[locate])
    {
    case 'i':
      race->Insert(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num + kv_correct);
      // if (strcmp(r_value, value_array[locate]) != 0) // pos,neg
      // {
      //   search_failed++;
      //   // cout << "search failed " << locate << "\t correct:" << value_array[locate] << "\t get value:" << r_value << "\tt_id:" << thread_gid << "\tstart num:" << start_num << endl;
      // }
      break;
    case 'u':
      // race->Update(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num + kv_correct);
      race->Update(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num + kv_correct); // pos,neg
        // cout << "search failed " << locate << "\t correct:" << value_array[locate] << "\t get value:" << r_value << "\tt_id:" << thread_gid << "\tstart num:" << start_num << endl;
      break;
    case 'r':
      if (!race->Search(yield, key_array[locate], KEY_LENGTH, r_value))
      {
        search_failed++;
      }
      // if (strcmp(r_value, value_array[locate]) != 0) // pos,neg
      // {
      //   search_failed++;
      //   cout << "search failed " << locate << "\t correct:" << value_array[locate] << "\t get value:" << r_value << "\tt_id:" << thread_gid << "\tstart num:" << start_num << endl;
      // }
      break;
    case 'd':
      race->Delete(yield, key_array[locate], KEY_LENGTH);
      break;
    default:
      break;
    }
    // race->Search(yield, key_array[locate], KEY_LENGTH, r_value);
#ifdef TEST_LATENCY
    clock_gettime(CLOCK_REALTIME, &msr_end);
    t_latency[locate] = (msr_end.tv_sec - msr_start.tv_sec) * 1000000000 + (msr_end.tv_nsec - msr_start.tv_nsec);
#endif

    if (stat_attempted_total >= num_per_thread)
    {
      break;
    }
  }
  coro_sched->finish++;
  if (coro_sched->finish == coro_num - 1)
  {
    //
    // std::cout << "speed: " << num_per_thread / msr_sec << "\t" << thread_gid << "\t" << stat_attempted_total << std::endl;
    stop_run = true;
  }
  coro_sched->FinishYield(yield, coro_id);
  return;
}

void load_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency) // num指要写多少
{
  auto bench_name = params->bench_name;
  key_array = _key_array;
  value_array = _value_array;
  num_per_thread = num;
  start_num = start;
  kv_correct = params->warm_num;

  stop_run = false;
  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_local_id;
  thread_num = params->thread_num_per_machine;
  meta_man = params->global_meta_man;
  coro_num = (coro_id_t)params->coro_num;
  coro_sched = new CoroutineScheduler(thread_gid, coro_num);
  thread_kv_offset = (MAX_KV / params->total_thread_num) * thread_gid * sizeof(Pair);
  f_insert = 0;
  s_insert = 0;

  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(thread_local_id);
  rdma_buffer_allocator = new RDMABufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);

  // alloc client dir and lock
  Directory *cache_dir = (Directory *)malloc(sizeof(Directory));

  params->cache_dir = cache_dir;
  memset(cache_dir, 0, sizeof(Directory));
  size_t local_level_lock[MAX_LEVEL];

  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;

  // Init coroutines
  for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++)
  {
    uint64_t coro_seed = static_cast<uint64_t>((static_cast<uint64_t>(thread_gid) << 32) | static_cast<uint64_t>(coro_i));
    random_generator[coro_i].SetSeed(coro_seed);
    coro_sched->coro_array[coro_i].coro_id = coro_i;
    // Bind workload to coroutine
    if (coro_i == POLL_ROUTINE_ID)
    {
      coro_sched->coro_array[coro_i].func = coro_call_t(bind(PollCompletion, _1));
    }
    else
    {
      if (bench_name == "swo")
      {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(LoadRACE, _1, coro_i, cache_dir, local_level_lock, params));
      }
    }
  }

  // Link all coroutines via pointers in a loop manner
  coro_sched->LoopLinkCoroutine(coro_num);

  // Build qp connection in thread granularity
  qp_man = new QPManager(thread_gid);
  qp_man->BuildQPConnection(meta_man); // 1 thread 1 qp

  // Sync qp connections in one compute node before running transactions
  connected_t_num += 1;
  while (connected_t_num != thread_num)
  {
    usleep(2000); // wait for all threads connections
  }

  // Init dir
  InitDir(cache_dir, meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).depth);

#ifdef TEST_LATENCY
  t_latency = latency;
#endif

  signal->operator++();
  if (thread_local_id == 0)
  {
    while (*signal != SET_REMOTE)
    {
      asm("nop");
    }
    SetAndWaitOhterClient(params->total_thread_num / params->thread_num_per_machine * (params->run_cnt++)); // client_num
    *signal = FINISH_SET;
  }
  while (*signal != START_RUN)
  {
    asm("nop");
  }

  struct timespec msr_start, msr_end;

  clock_gettime(CLOCK_REALTIME, &msr_start);

  // Start the first coroutine
  coro_sched->coro_array[0].func();

  clock_gettime(CLOCK_REALTIME, &msr_end);
  double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
  // std::cout << "Thread " << thread_gid << " Insert Speed: " << (num * params->total_thread_num) / msr_sec << "\t" << std::endl;

  // Clean
  if (random_generator)
    delete[] random_generator;
  delete coro_sched;
}

int run_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, char *_method_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency) // num指要写多少
{
  auto bench_name = params->bench_name;
  key_array = _key_array;
  value_array = _value_array;
  method_array = _method_array;
  num_per_thread = num;
  start_num = start;
  kv_correct = params->warm_num + params->load_num;

  stop_run = false;
  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_local_id;
  thread_num = params->thread_num_per_machine;
  meta_man = params->global_meta_man;
  coro_num = (coro_id_t)params->coro_num;
  coro_sched = new CoroutineScheduler(thread_gid, coro_num);
  thread_kv_offset = (MAX_KV / params->total_thread_num) * thread_gid * sizeof(Pair);

  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(thread_local_id);
  rdma_buffer_allocator = new RDMABufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);

  // alloc client dir and lock
  Directory *cache_dir = (Directory *)malloc(sizeof(Directory));
  memset(cache_dir, 0, sizeof(Directory));
  size_t local_level_lock[MAX_LEVEL];

  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;

  search_failed = 0;
  stat_attempted_total = 0; // reset count
  stop_run = false;         // reset stop_run
  for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++)
  {
    uint64_t coro_seed = static_cast<uint64_t>((static_cast<uint64_t>(thread_gid) << 32) | static_cast<uint64_t>(coro_i));
    random_generator[coro_i].SetSeed(coro_seed);
    coro_sched->coro_array[coro_i].coro_id = coro_i;
    // Bind workload to coroutine
    if (coro_i == POLL_ROUTINE_ID)
    {
      coro_sched->coro_array[coro_i].func = coro_call_t(bind(PollCompletion, _1));
    }
    else
    {
      if (bench_name == "swo")
      {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunRACE, _1, coro_i, cache_dir, local_level_lock, params));
      }
    }
  }

  // Link all coroutines via pointers in a loop manner
  coro_sched->LoopLinkCoroutine(coro_num);

  // Build qp connection in thread granularity
  qp_man = new QPManager(thread_gid);
  qp_man->BuildQPConnection(meta_man); // 1 thread 1 qp

  // Sync qp connections in one compute node before running transactions
  connected_t_num += 1;
  while (connected_t_num != thread_num)
  {
    usleep(2000); // wait for all threads connections
  }

  // Init dir
  InitDir(cache_dir, meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).depth);

#ifdef TEST_LATENCY
  t_latency = latency;
#endif

  signal->operator++();
  if (thread_local_id == 0)
  {
    while (*signal != SET_REMOTE)
    {
      asm("nop");
    }
    SetAndWaitOhterClient(params->total_thread_num / params->thread_num_per_machine * (params->run_cnt++)); // client_num * 2 next client
    *signal = FINISH_SET;
  }
  while (*signal != START_RUN)
  {
    asm("nop");
  }

  coro_sched->coro_array[0].func();

  if (search_failed != 0)
    cout << "Search Failed:" << search_failed << endl;

  // RDMA_LOG(DBG) << "Thread: " << thread_gid << ". Loop RDMA alloc times: " << rdma_buffer_allocator->loop_times;

  // Clean
  if (random_generator)
    delete[] random_generator;
  delete coro_sched;
  return search_failed;
}

void WarmRACE(coro_yield_t &yield, coro_id_t coro_id, Directory *cache_dir, size_t *local_level_lock, thread_params *params)
{
  // Each coroutine has a race: Each coroutine is a coordinator
  RACE *race = new RACE(meta_man,
                        qp_man,
                        thread_gid,
                        coro_id,
                        coro_sched,
                        rdma_buffer_allocator,
                        thread_kv_offset,
                        thread_num,
                        cache_dir,
                        local_level_lock);
  race->update_size = &(params->update_size);
  race->cas_size = &(params->cas_size);
  race->read_size = &(params->read_size);
  race->write_size = &(params->write_size);

  race->partial_size = &(params->partial_size);
  race->full_size = &(params->full_size);
  race->hit_cnt = &(params->hit_cnt);
  race->total_cnt = &(params->total_cnt);
  // if(coro_id == 1)
  // cout << "thread " << thread_gid << " " << race->kv_ptr << endl;
  while (true)
  {
    int locate = start_num + stat_attempted_total++;
    race->Insert(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num);
    if (stat_attempted_total >= num_per_thread)
    {
      break;
    }
    params->finish_num++;
  }

  coro_sched->finish++;
  if (coro_sched->finish == coro_num - 1)
  {

    stop_run = true;
  }
  coro_sched->FinishYield(yield, coro_id);
  return;
}

void warm_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency) // num指要写多少
{
  auto bench_name = params->bench_name;
  key_array = _key_array;
  value_array = _value_array;
  num_per_thread = num;
  start_num = start;

  stop_run = false;
  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_local_id;
  thread_num = params->thread_num_per_machine;
  meta_man = params->global_meta_man;
  coro_num = (coro_id_t)params->coro_num;
  coro_sched = new CoroutineScheduler(thread_gid, coro_num);
  thread_kv_offset = (MAX_KV / params->total_thread_num) * thread_gid * sizeof(Pair);

  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(thread_local_id);
  rdma_buffer_allocator = new RDMABufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);

  // alloc client dir and lock
  Directory *cache_dir = (Directory *)malloc(sizeof(Directory));
  memset(cache_dir, 0, sizeof(Directory));
  // params->cache_dir = cache_dir;
  size_t local_level_lock[MAX_LEVEL];

  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;

  // Init coroutines
  for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++)
  {
    uint64_t coro_seed = static_cast<uint64_t>((static_cast<uint64_t>(thread_gid) << 32) | static_cast<uint64_t>(coro_i));
    random_generator[coro_i].SetSeed(coro_seed);
    coro_sched->coro_array[coro_i].coro_id = coro_i;
    // Bind workload to coroutine
    if (coro_i == POLL_ROUTINE_ID)
    {
      coro_sched->coro_array[coro_i].func = coro_call_t(bind(PollCompletion, _1));
    }
    else
    {
      if (bench_name == "swo")
      {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(WarmRACE, _1, coro_i, cache_dir, local_level_lock, params));
      }
    }
  }

  // Link all coroutines via pointers in a loop manner
  coro_sched->LoopLinkCoroutine(coro_num);

  // Build qp connection in thread granularity
  qp_man = new QPManager(thread_gid);
  qp_man->BuildQPConnection(meta_man); // 1 thread 1 qp

  // Sync qp connections in one compute node before running transactions
  connected_t_num += 1;
  while (connected_t_num != thread_num)
  {
    usleep(2000); // wait for all threads connections
  }
  // Init dir
  InitDir(cache_dir, meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).depth);

  signal->operator++();
  if (thread_local_id == 0)
  {
    while (*signal != SET_REMOTE)
    {
      asm("nop");
    }
    SetAndWaitOhterClient(params->total_thread_num / params->thread_num_per_machine * (params->run_cnt++)); // client_num
    *signal = FINISH_SET;
  }
  while (*signal != START_RUN)
  {
    asm("nop");
  }

  // Start the first coroutine
  coro_sched->coro_array[0].func();

  // Clean
  if (random_generator)
    delete[] random_generator;
  delete coro_sched;
}