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

#define DELETE_NUM 10000000

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
__thread uint64_t delete_stat_attempted_total = 0;
__thread uint64_t stat_committed_tx_total = 0; // Committed transaction number
const coro_id_t POLL_ROUTINE_ID = 0;           // The poll coroutine ID
__thread Key_t *key_array;
__thread Value_t *value_array;
__thread char *method_array;
__thread size_t num_per_thread;
__thread size_t start_num;
__thread size_t kv_correct;

__thread size_t delete_start_num;
__thread size_t delete_num_per_thread;
__thread uint64_t t_finish_num;
__thread long *t_latency;
#ifdef UPDATE_SIZE
__thread uint64_t *update_size;
__thread uint64_t *update_cnt;
#endif

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

void InitDir(Directory *cache_dir)
{
  IndexMeta index_meta = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID);
  cache_dir->global_depth = index_meta.depth; // default 15
  node_id_t remote_node_id = meta_man->GetPrimaryNodeID(DEFAULT_ID);
  RCQP *qp = qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
  char *data_buf = rdma_buffer_allocator->Alloc(sizeof(uint64_t) + SegUnitSize * pow(2, MAX_DEPTH));
  uint64_t seg_lower_bound = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).dir_ptr + meta_man->seg_offset;
  uint64_t seg_upper_bound = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).dir_ptr + meta_man->seg_offset + MAX_SEG * sizeof(Segment);
RE_INIT:
  depth_t before_gd = cache_dir->global_depth;
  size_t NowSize = sizeof(uint64_t) + pow(2, before_gd) * SegUnitSize;
  coro_sched->RDMAReadSync(0, qp, data_buf, meta_man->dir_offset, NowSize);
  // memmove((void *)cache_dir, (void *)data_buf, NowSize);
  memcpy((void *)cache_dir, (void *)data_buf, NowSize); // 先拷贝depth
  
  // for(uint64_t i = 1; i <= pow(2, cache_dir->global_depth); i++)
  // {
  //   memcpy((void *)(cache_dir + 8 * i), (void *)(data_buf + 8 * i), SegUnitSize);
  //   if(cache_dir->seg[i - 1] > 6000000000000000UL)
  //   {
  //     cout << "init error!" << endl;
  //   }
  // }
  // memcpy(cache_dir, (void *)data_buf, NowSize);
  if(cache_dir->global_depth > MAX_DEPTH || cache_dir->global_depth < index_meta.depth){
    memcpy((void *)(cache_dir),data_buf,sizeof(depth_t));
  }
  for(int i = 0; i < pow(2, cache_dir->global_depth); i++)
  {
    if((uint64_t)GetAddr(cache_dir->seg[i]) < seg_lower_bound || (uint64_t)GetAddr(cache_dir->seg[i]) > seg_upper_bound)
    {
      memcpy((void *)((uint64_t)cache_dir + 8 * (i + 1)), data_buf + 8 *(i+1), SegSize);
    }
  } 
  if(cache_dir->global_depth > MAX_DEPTH || cache_dir->global_depth < index_meta.depth)
  {
    // cout << thread_local_id << " init over : " << cache_dir->global_depth << " " << before_gd << " " << index_meta.depth << " " << NowSize << " " << *(depth_t *)data_buf << endl;
    cache_dir->global_depth = index_meta.depth;
    goto RE_INIT;
  }
  if (cache_dir->global_depth != before_gd)
  {
    // cout << thread_local_id << " init fail : " << cache_dir->global_depth  << " " << before_gd << " " << index_meta.depth << " " << NowSize << " " << *(depth_t *)data_buf << endl;
    goto RE_INIT;
  }
}

void InitDirShare(Directory *cache_dir, t_id_t thread_id, uint64_t *symbol)
{
  if(thread_id == 0)
  {
    InitDir(cache_dir);
    *symbol = 1;
  }
  else
  {
    // wait thread 0 sync dir
    while(*symbol == 0)
      ;
  }
}

void get_real_time_info(thread_params *params, bool *run)
{
  uint64_t before_finish_num = params->finish_num;
  uint64_t finished;
  while(*run)
  {
    usleep(250000);
    finished = 0;
    for(int i = 0; i < params[0].thread_num_per_machine; i++)
    {
      finished += params[i].finish_num;
    }
    finished = finished - before_finish_num;
    cout <<  finished <<  endl;
    before_finish_num += finished;
  }
}

void SetAndWaitOhterClient(size_t compute_node_id_num)
{
  IndexMeta index_meta = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID);
  node_id_t remote_node_id = meta_man->GetPrimaryNodeID(DEFAULT_ID);
  RCQP *qp = qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
  size_t NowSize = sizeof(uint64_t);
  char *data_buf = rdma_buffer_allocator->Alloc(NowSize);
  coro_sched->RDMAReadSync(0, qp, data_buf, meta_man->count_offset, NowSize);
  if(*(size_t *)data_buf >= compute_node_id_num)
  {
    cout << "error!" << endl;
  }
  size_t cmp;
   do
  {
    cmp = *(size_t *)data_buf;
    cout << "retry:" << cmp <<  "compute_node_id" <<  compute_node_id_num <<endl;
    coro_sched->RDMACASSync(0, qp, data_buf, meta_man->count_offset, cmp, cmp + 1);
    cout << "cmp:" << cmp  << "  cmp+1:" << cmp+1 << "   data_buf:" << *(size_t *)data_buf << endl;
  } while (*(size_t *)data_buf != cmp);
  // cout << cmp << "\t" << compute_node_id_num << endl;
  if (cmp == compute_node_id_num - 1){
    cout << "success cmp:" << cmp << "compute_node_id" <<  compute_node_id_num << endl;
    return; // all start
  }
  do
  {
    coro_sched->RDMAReadSync(0, qp, data_buf, meta_man->count_offset, NowSize);
  } while (*(size_t *)data_buf != compute_node_id_num);
  // cout << "return" << *(size_t *)data_buf << endl;
}

void LoadRACE(coro_yield_t &yield, coro_id_t coro_id, Directory *cache_dir, size_t *local_dir_lock, thread_params *params)
{

  // Each coroutine has a race: Each coroutine is a coordinator
  RACE *race = new RACE(meta_man,
                        qp_man,
                        thread_gid,
                        coro_id,
                        coro_sched,
                        rdma_buffer_allocator,
                        thread_kv_offset,
                        cache_dir,
                        local_dir_lock);
  // if(coro_id == 1)
    // cout << "thread " << thread_gid << " " << race->kv_ptr << endl;
  char r_value[32];
  race->coro_lock_id = params->thread_global_id * (params->coro_num - 1) + coro_id;
  race->insert_blked = params->insert_blked;
  race->blk_cnt = params->blk_cnt;
  race->blk_lock = params->blk_lock;
  race->split_flag = false;

#ifdef UPDATE_SIZE
  race->update_size = 0;
  race->update_cnt = 0;
#endif

  struct timespec msr_start, msr_end;
  while (true)
  {
    int locate = start_num + stat_attempted_total++;
    // if (*local_dir_lock == true)
    // if (locate % 1000000 == 0)
    if (locate == 349839)
    {
      cout << "thread:" << thread_gid << "\tfinish_num:" << locate << "local_dir_lock:" << *local_dir_lock << endl;
    }
#ifdef TEST_LATENCY
    clock_gettime(CLOCK_REALTIME, &msr_start);
#endif
    race->Insert(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num + kv_correct);
    
#ifdef TEST_LATENCY
    clock_gettime(CLOCK_REALTIME, &msr_end);
    t_latency[locate] = (msr_end.tv_sec - msr_start.tv_sec) * 1000000000 + (msr_end.tv_nsec - msr_start.tv_nsec);
#endif
    params->finish_num ++;
    if (stat_attempted_total >= num_per_thread)
    {
      break;
    }
  }

  coro_sched->finish++;
  if (coro_sched->finish == coro_num - 1)
  {

    stop_run = true;
  }
  coro_sched->FinishYield(yield, coro_id);
  return;
}


// void GlobalLoadRACE(coro_yield_t &yield, coro_id_t coro_id, Directory *cache_dir, size_t *local_dir_lock, std::atomic<uint64_t> *global_dir_lock, thread_params *params)
void GlobalLoadRACE(coro_yield_t &yield, coro_id_t coro_id, Directory *cache_dir, size_t *local_dir_lock, thread_params *params)
{
  // Each coroutine has a race: Each coroutine is a coordinator
  RACE *race = new RACE(meta_man,
                        qp_man,
                        thread_gid,
                        coro_id,
                        coro_sched,
                        rdma_buffer_allocator,
                        thread_kv_offset,
                        cache_dir,
                        local_dir_lock);
  // if(coro_id == 1)
    // cout << "thread " << thread_gid << " " << race->kv_ptr << endl;
  race->global_dir_lock = params->global_dir_lock;
  race->global_split_lock = params->global_split_lock;
  char r_value[32];
  race->coro_lock_id = params->thread_global_id * (params->coro_num - 1) + coro_id;
  race->insert_blked = params->insert_blked;
  race->blk_cnt = params->blk_cnt;
  race->blk_lock = params->blk_lock;
  race->split_flag = false;

#ifdef UPDATE_SIZE
  race->update_size = 0;
  race->update_cnt = 0;
#endif

  struct timespec msr_start, msr_end;
  while (true)
  {
    int locate = start_num + stat_attempted_total++;
    // if (*local_dir_lock == true)
    // if (locate % 1000000 == 0)
    if (locate == 349839)
    {
      cout << "thread:" << thread_gid << "\tfinish_num:" << locate << "local_dir_lock:" << *local_dir_lock << endl;
    }
#ifdef TEST_LATENCY
    clock_gettime(CLOCK_REALTIME, &msr_start);
#endif
    race->Insert(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num + kv_correct);
#ifdef TEST_LATENCY
    clock_gettime(CLOCK_REALTIME, &msr_end);
    t_latency[locate] = (msr_end.tv_sec - msr_start.tv_sec) * 1000000000 + (msr_end.tv_nsec - msr_start.tv_nsec);
#endif
    params->finish_num ++;
    // if(params->finish_num % 1000000 == 0)
    //   cout << "thread: " << thread_gid << " finish: " <<  params->finish_num << endl;
    if (stat_attempted_total >= num_per_thread)
    {
      break;
    }
  }
  if(*local_dir_lock == coro_id)
    *local_dir_lock = 0;
  coro_sched->finish++;
  cout << "thread: " << thread_gid << " coro: " << coro_id << " end.." << endl;
#ifdef UPDATE_SIZE
  update_size[coro_id] = race->update_size;
  update_cnt[coro_id] = race->update_cnt;
#endif
  if (coro_sched->finish == coro_num - 1)
  {
    if((*(params->global_dir_lock)) == thread_gid)
      CAS(params->global_dir_lock, params->global_dir_lock, UNLOCK);
    stop_run = true;
  }
  coro_sched->FinishYield(yield, coro_id);
  return;
}
void DeleteRACE(coro_yield_t &yield, coro_id_t coro_id, Directory *cache_dir, size_t *local_dir_lock)
{
  // Each coroutine has a race: Each coroutine is a coordinator
  RACE *race = new RACE(meta_man,
                        qp_man,
                        thread_gid,
                        coro_id,
                        coro_sched,
                        rdma_buffer_allocator,
                        thread_kv_offset,
                        cache_dir,
                        local_dir_lock);
  char r_value[VALUE_LENGTH];
  struct timespec msr_start, msr_end;

  random_device seed;//硬件生成随机数种子
	ranlux48 engine(seed());//利用种子生成随机数引擎
  uniform_int_distribution<> distrib(start_num, start_num + delete_num_per_thread * 10 - 1);//设置随机数范围，并为均匀分布
  int random;//随机数
  // int tail_start_num = start_num + delete_num_per_thread * 9;
  while(true)
  {
    int del_loc = start_num + delete_stat_attempted_total++;
    random = distrib(engine);
    // random = distrib(engine);
    //删除前delete_num_per_thread 条
    race->Delete(yield, key_array[del_loc], KEY_LENGTH);
    if (delete_stat_attempted_total >= delete_num_per_thread)
      break;
  }
  coro_sched->finish++;
  if (coro_sched->finish == coro_num - 1)
  {

    stop_run = true;
  }
  coro_sched->FinishYield(yield, coro_id);
  return;
}

void UpdateRACE(coro_yield_t &yield, coro_id_t coro_id, Directory *cache_dir, size_t *local_dir_lock,thread_params *params)
{
  // Each coroutine has a race: Each coroutine is a coordinator
  RACE *race = new RACE(meta_man,
                        qp_man,
                        thread_gid,
                        coro_id,
                        coro_sched,
                        rdma_buffer_allocator,
                        thread_kv_offset,
                        cache_dir,
                        local_dir_lock);
  // if(coro_id == 1)
    // cout << "thread " << thread_gid << " " << race->kv_ptr << endl;
  char r_value[VALUE_LENGTH];
  struct timespec msr_start, msr_end;
  char *rand_str;
  string str;
    char c;
    for(int i = 0; i < VALUE_LENGTH; i++)
    {
      c = 'a' + rand() % 26;
      str.push_back(c);
    }
    rand_str = (char *)str.data();
  // uint64_t used = num_per_thread;
  
  while(true)
  {
    // string str;
    // char c;
    // for(int i = 0; i < VALUE_LENGTH; i++)
    // {
    //   c = 'a' + rand() % 26;
    //   str.push_back(c);
    // }
    // rand_str = (char *)str.data();
    int locate = start_num + stat_attempted_total++;
    race->Update(yield, key_array[locate],value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num + kv_correct);
    // race->Update(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num + kv_correct);
    // race->Search(yield, key_array[locate], KEY_LENGTH, r_value);
      // if (strncmp(r_value,value_array[locate], VALUE_LENGTH) != 0) // pos,neg
      // {
      //   search_failed++;
      //   // race->Search(yield, key_array[locate], KEY_LENGTH, r_value);
      //   // cout << "search failed " << locate << "\t correct:" << value_array[locate] << "\t get value:" << r_value << "\tt_id:" << thread_gid << "\tstart num:" << start_num << endl;
      //   // cout << "search failed " << locate << "\t correct:" << rand_str << "\t get value:" << r_value << "\tt_id:" << thread_gid << "\tstart num:" << start_num << endl;
      // }
    if (stat_attempted_total >= num_per_thread)
    {
      break;
    }
  }
  coro_sched->finish++;
  if (coro_sched->finish == coro_num - 1)
  {
    stop_run = true;
  }
  coro_sched->FinishYield(yield, coro_id);
  return;
}

void RunRACE(coro_yield_t &yield, coro_id_t coro_id, Directory *cache_dir, size_t *local_dir_lock, thread_params * params)
{

  // Each coroutine has a race: Each coroutine is a coordinator
  RACE *race = new RACE(meta_man,
                        qp_man,
                        thread_gid,
                        coro_id,
                        coro_sched,
                        rdma_buffer_allocator,
                        thread_kv_offset,
                        cache_dir,
                        local_dir_lock);
  char r_value[VALUE_LENGTH];
  struct timespec msr_start, msr_end;

#ifdef UPDATE_SIZE
  race->update_size = 0;
  race->update_cnt = 0;
#endif

  while (true)
  {
    int locate = start_num + stat_attempted_total++;
    // if (locate % 100000 == 0)
    // if (locate == 805011)
    //   cout << "thread:" << thread_gid << "\tfinish_num:" << locate << endl;
    // race->Insert(yield, key_array[start_num + stat_attempted_total++], value_array[start_num + stat_attempted_total], KEY_LENGTH, VALUE_LENGTH, stat_attempted_total);
#ifdef TEST_LATENCY
    clock_gettime(CLOCK_REALTIME, &msr_start);
#endif
    switch (method_array[locate])
    {
    case 'i':
      // race->Insert(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num + kv_correct);
      race->Insert(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num+ kv_correct);
      break;
    case 'u':
      // race->Update(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num + kv_correct);
      race->Update(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num + kv_correct);
      // race->Search(yield, key_array[locate], KEY_LENGTH, r_value);
      // if (strcmp(r_value, value_array[locate]) != 0) // pos,neg
      // {
      //   search_failed++;
        // cout << "search failed " << locate << "\t correct:" << value_array[locate] << "\t get value:" << r_value << "\tt_id:" << thread_gid << "\tstart num:" << start_num << endl;
      // }
      break;
    case 'r':
      if(!race->Search(yield, key_array[locate], KEY_LENGTH, r_value))
      {
        search_failed++;
      }
      // if (strcmp(r_value, value_array[locate]) != 0) // pos,neg
      // {
      //   search_failed++;
      //   // cout << "search failed " << locate << "\t correct:" << value_array[locate] << "\t get value:" << r_value << "\tt_id:" << thread_gid << "\tstart num:" << start_num << endl;
      // }
      break;
    case 'd':
      // cout << locate << endl;
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


#ifdef UPDATE_SIZE
  update_size[coro_id] = race->update_size;
  update_cnt[coro_id] = race->update_cnt;
#endif
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
  t_finish_num = 0;
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

  // cout << thread_gid <<  " insert thread_kv_offset: " << thread_kv_offset << endl;

  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(thread_local_id);
  rdma_buffer_allocator = new RDMABufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);

  // alloc client dir and lock
  Directory *cache_dir = (Directory *)malloc(sizeof(Directory));
  memset(cache_dir, 0, sizeof(Directory));
  size_t local_dir_lock = 0;

  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;

#ifdef UPDATE_SIZE
  update_size = (uint64_t *)malloc(sizeof(uint64_t) * coro_num);
  update_cnt = (uint64_t *)malloc(sizeof(uint64_t) * coro_num);
  memset(update_size, 0, sizeof(uint64_t) * coro_num);
  memset(update_cnt, 0, sizeof(uint64_t) * coro_num);
  uint64_t total_size = 0;
  uint64_t total_cnt = 0;
#endif

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
      if (bench_name == "race")
      {
#ifdef GLOBAL_CACHE
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(GlobalLoadRACE, _1, coro_i, params->global_cache_dir,&local_dir_lock, params));
#else
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(LoadRACE, _1, coro_i, cahce_dir, &local_dir_lock, params));
#endif
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
#ifdef GLOBAL_CACHE
  InitDirShare(params->global_cache_dir, thread_local_id, params->symbol);
#else
  InitDir(cache_dir);
#endif

  cout << thread_local_id << " init dir successfully!" << endl;

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
    SetAndWaitOhterClient(params->total_thread_num / params->thread_num_per_machine * (params->run_cnt ++)); // client_num
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

#ifdef UPDATE_SIZE
  for(int i = 1; i < coro_num; i++)
  {
    total_size += update_size[i];
    total_cnt += update_cnt[i];
  }

  params->update_size = total_size;
  params->update_cnt = total_cnt;
  // cout << "thread " << thread_local_id << " update " << (double)total_size / 1048576 << " MB" << endl;
  // cout << "thread " << thread_local_id << " update " << total_cnt << " times" << endl;
  // cout << "thread " << thread_local_id << "search success num: " << search_failed << endl;
#endif

  clock_gettime(CLOCK_REALTIME, &msr_end);
  double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
  // std::cout << "Thread " << thread_gid << " Insert Speed: " << (num * params->total_thread_num) / msr_sec << "\t" << std::endl;

  // Clean
  if (random_generator)
    delete[] random_generator;
  delete coro_sched;
}

void delete_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency)
{
  auto bench_name = params->bench_name;
  key_array = _key_array;
  value_array = _value_array;
  delete_num_per_thread = num;
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
  size_t local_dir_lock = 0;

  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;

  search_failed = 0;
  delete_stat_attempted_total = 0; // reset count
  stop_run = false;         // reset stop_run

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
      if (bench_name == "race")
      {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(DeleteRACE, _1, coro_i, cache_dir, &local_dir_lock));
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
  InitDir(cache_dir);


  signal->operator++();
  if (thread_local_id == 0)
  {
    while (*signal != SET_REMOTE)
    {
      asm("nop");
    }
    SetAndWaitOhterClient(params->total_thread_num / params->thread_num_per_machine * (params->run_cnt ++)); // client_num
    *signal = FINISH_SET;
  }
  while (*signal != START_RUN)
  {
    asm("nop");
  }

  struct timespec msr_start, msr_end;

  // std::cout << "thread: " << thread_gid << "start delete " << std::endl;
  clock_gettime(CLOCK_REALTIME, &msr_start);


  // Start the first coroutine
  coro_sched->coro_array[0].func();


  clock_gettime(CLOCK_REALTIME, &msr_end);
  double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;

  // std::cout << "Thread " << thread_gid << " Delete Speed: " << (num * params->total_thread_num) / msr_sec << "\t" << std::endl;

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
  size_t local_dir_lock = 0;

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
      if (bench_name == "race")
      {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunRACE, _1, coro_i, cache_dir, &local_dir_lock, params));
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
  InitDir(cache_dir);

  if(cache_dir->global_depth == 0)
  {
    cout << "global depth: 0!" << endl;
    InitDir(cache_dir);
  }


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
    SetAndWaitOhterClient(params->total_thread_num / params->thread_num_per_machine * (params->run_cnt ++)); // client_num * 2 next client
    *signal = FINISH_SET;
  }
  while (*signal != START_RUN)
  {
    asm("nop");
  }

#ifdef UPDATE_SIZE
  update_size = (uint64_t *)malloc(sizeof(uint64_t) * coro_num);
  update_cnt = (uint64_t *)malloc(sizeof(uint64_t) * coro_num);
  memset(update_size, 0, sizeof(uint64_t) * coro_num);
  memset(update_cnt, 0, sizeof(uint64_t) * coro_num);
  uint64_t total_size = 0;
  uint64_t total_cnt = 0;
#endif

  coro_sched->coro_array[0].func();

#ifdef UPDATE_SIZE
  for(int i = 1; i < coro_num; i++)
  {
    total_size += update_size[i];
    total_cnt += update_cnt[i];
  }

  params->update_size = total_size;
  params->update_cnt = total_cnt;
  // cout << "thread " << thread_local_id << " update " << (double)total_size / 1048576 << " MB" << endl;
  // cout << "thread " << thread_local_id << " update " << total_cnt << " times" << endl;
#endif



  if (search_failed != 0)
    cout << "Search Failed:" << search_failed << endl;

  // RDMA_LOG(DBG) << "Thread: " << thread_gid << ". Loop RDMA alloc times: " << rdma_buffer_allocator->loop_times;

  // Clean
  if (random_generator)
    delete[] random_generator;
  delete coro_sched;
  return search_failed;
}


void update_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array,char *_method_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency)
{
   auto bench_name = params->bench_name;
  key_array = _key_array;
  value_array = _value_array;
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

  // cout << thread_gid  <<  " update thread_kv_offset: " << thread_kv_offset << endl;



  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(thread_local_id);
  rdma_buffer_allocator = new RDMABufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);

  // alloc client dir and lock
  Directory *cache_dir = (Directory *)malloc(sizeof(Directory));
  memset(cache_dir, 0, sizeof(Directory));
  size_t local_dir_lock = 0;

  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;

  search_failed = 0;
  stat_attempted_total = 0; // reset count
  stop_run = false;         // reset stop_run

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
      if (bench_name == "race")
      {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(UpdateRACE, _1, coro_i, cache_dir, &local_dir_lock, params));
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
  InitDir(cache_dir);


  signal->operator++();
  if (thread_local_id == 0)
  {
    while (*signal != SET_REMOTE)
    {
      asm("nop");
    }
    SetAndWaitOhterClient(params->total_thread_num / params->thread_num_per_machine * (params->run_cnt ++)); // client_num
    *signal = FINISH_SET;
  }

  while (*signal != START_RUN)
  {
    asm("nop");
  }

  struct timespec msr_start, msr_end;

  // std::cout << "thread: " << thread_gid << "start delete " << std::endl;
  clock_gettime(CLOCK_REALTIME, &msr_start);


  // Start the first coroutine
  coro_sched->coro_array[0].func();


  clock_gettime(CLOCK_REALTIME, &msr_end);
  double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;

  cout << "thread " << thread_gid << " search failed: " << search_failed << endl;
  // std::cout << "Thread " << thread_gid << " Delete Speed: " << (num * params->total_thread_num) / msr_sec << "\t" << std::endl;

  // Clean
  if (random_generator)
    delete[] random_generator;
  delete coro_sched;
}

void WarmRACE(coro_yield_t &yield, coro_id_t coro_id, Directory *cache_dir, size_t *local_dir_lock)
{
// Each coroutine has a race: Each coroutine is a coordinator
  RACE *race = new RACE(meta_man,
                        qp_man,
                        thread_gid,
                        coro_id,
                        coro_sched,
                        rdma_buffer_allocator,
                        thread_kv_offset,
                        cache_dir,
                        local_dir_lock);
  // if(coro_id == 1)
    // cout << "thread " << thread_gid << " " << race->kv_ptr << endl;
  char r_value[32];

  while (true)
  {
    int locate = start_num + stat_attempted_total++;
    race->Insert(yield, key_array[locate], value_array[locate], KEY_LENGTH, VALUE_LENGTH, locate - start_num);
    if (stat_attempted_total >= num_per_thread)
    {
      break;
    }
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

  // cout << thread_gid <<  " insert thread_kv_offset: " << thread_kv_offset << endl;

  auto alloc_rdma_region_range = params->global_rdma_region->GetThreadLocalRegion(thread_local_id);
  rdma_buffer_allocator = new RDMABufferAllocator(alloc_rdma_region_range.first, alloc_rdma_region_range.second);

  // alloc client dir and lock
  Directory *cache_dir = (Directory *)malloc(sizeof(Directory));
  memset(cache_dir, 0, sizeof(Directory));
  size_t local_dir_lock = 0;

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
      if (bench_name == "race")
      {
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(WarmRACE, _1, coro_i, cache_dir, &local_dir_lock));
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
  InitDir(cache_dir);

  signal->operator++();
  if (thread_local_id == 0)
  {
    while (*signal != SET_REMOTE)
    {
      asm("nop");
    }
    SetAndWaitOhterClient(params->total_thread_num / params->thread_num_per_machine * (params->run_cnt ++)); // client_num
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