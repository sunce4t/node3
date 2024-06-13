// Author: Ming Zhang
// Copyright (c) 2022

#include "worker/handler.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>

#include "util/json_config.h"
#include "worker/worker.h"

std::atomic<uint64_t> tx_id_generator;
std::atomic<uint64_t> connected_t_num;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> medianlat_vec;
std::vector<double> taillat_vec;
std::vector<double> lock_durations;
std::vector<uint64_t> total_try_times;
std::vector<uint64_t> total_commit_times;
struct timespec msr_start, msr_end;

double aver(long *var, int len)
{
  double aver = 0;
  for (int i = 0; i < len; i++)
  {
    aver += (double)(var[i]) / len;
  }
  return aver;
}

void Handler::ConfigureComputeNode(int argc, char *argv[])
{
  std::string config_file = "../../../config/compute_node_config.json";
  if (argc == 4)
  {
    std::string s1 = "sed -i '5c \"thread_num_per_machine\": " + std::string(argv[2]) + ",' " + config_file;
    std::string s2 = "sed -i '6c \"coroutine_num\": " + std::string(argv[3]) + ",' " + config_file;
    system(s1.c_str());
    system(s2.c_str());
  }
  return;
}

void Handler::GenThreads(std::string bench_name)
{
  std::string config_filepath = "../../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  node_id_t machine_num = (node_id_t)client_conf.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)client_conf.get("machine_id").get_int64();
  t_id_t thread_num_per_machine = (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
  const int coro_num = (int)client_conf.get("coroutine_num").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);

  size_t WARM_NUM, LOAD_NUM, RUN_NUM, UPDATE_NUM, DELETE_NUM;

  config_filepath = "../../../config/" + bench_name + "_config.json";

  auto r_json_config = JsonConfig::load_file(config_filepath);
  auto conf = r_json_config.get(bench_name);
  uint64_t key;
  LOAD_NUM = conf.get("load_num").get_uint64();
  string load = conf.get("load_data_path").get_str();

  auto total_insert_blked = new std::unordered_map<uint64_t, uint64_t>[30];
  auto total_blk_lock = new std::mutex[32];
  auto total_blk_cnt = new uint64_t[32];
  // std::atomic<uint64_t> global_dir_lock;
  // global_dir_lock.store(0);
  std::uint64_t global_dir_lock = UNLOCK;
  uint64_t global_symbol = 0;
  uint64_t global_split_lock = UNLOCK;
  Directory *cache_dir = (Directory *)malloc(sizeof(Directory));
  memset(cache_dir, 0, sizeof(Directory));
  memset(total_blk_cnt, 0, sizeof(uint64_t) * 32);

#ifdef WARMUP
  WARM_NUM = conf.get("warm_num").get_uint64();
  string warm = conf.get("warm_data_path").get_str();
  RDMA_LOG(INFO) << "Warm Data Path:" << warm;
  char *w_method = (char *)malloc(sizeof(char) * WARM_NUM);
  Key_t *w_key = (Key_t *)malloc(sizeof(Key_t *) * WARM_NUM);
  Value_t *w_value = (Value_t *)malloc(sizeof(Value_t *) * WARM_NUM);
  for (int i = 0; i < WARM_NUM; i++)
  {
    w_key[i] = (Key_t)calloc(KEY_LENGTH, sizeof(char));
    w_value[i] = (Value_t)calloc(VALUE_LENGTH, sizeof(char));
  }
  ifstream w_ifs;
  w_ifs.open(warm);
  if (!w_ifs)
  {
    cerr << "No file." << endl;
    exit(1);
  }
  else
  {
    for (int i = 0; i < WARM_NUM; i++)
    {
      w_ifs >> w_method[i];
      w_ifs >> key;
      // memcpy(l_keys[i], &key, sizeof(uint64_t));
      sprintf(w_key[i], "%ld", key);
      w_ifs.getline(w_value[i], VALUE_LENGTH);
    }
    w_ifs.close();
    RDMA_LOG(INFO) << warm << " is used.";
  }
#endif

  RDMA_LOG(INFO) << "Load Data Path:" << load;
  
  char *l_method = (char *)malloc(sizeof(char) * LOAD_NUM);
  Key_t *l_key = (Key_t *)malloc(sizeof(Key_t *) * LOAD_NUM);
  Value_t *l_value = (Value_t *)malloc(sizeof(Value_t *) * LOAD_NUM);
  for (int i = 0; i < LOAD_NUM; i++)
  {
    l_key[i] = (Key_t)calloc(KEY_LENGTH, sizeof(char));
    l_value[i] = (Value_t)calloc(VALUE_LENGTH, sizeof(char));
  }
  ifstream l_ifs;
  l_ifs.open(load);
  if (!l_ifs)
  {
    cerr << "No file." << endl;
    exit(1);
  }
  else
  {
    for (int i = 0; i < LOAD_NUM; i++)
    {
      l_ifs >> l_method[i];
      l_ifs >> key;
      // memcpy(l_keys[i], &key, sizeof(uint64_t));
      sprintf(l_key[i], "%ld", key);
      l_ifs.getline(l_value[i], VALUE_LENGTH);
    }
    l_ifs.close();
    RDMA_LOG(INFO) << load << " is used.";
  }


#ifndef ONLY_INSERT
  RUN_NUM = conf.get("run_num").get_uint64();
  string run = conf.get("run_data_path").get_str();
  RDMA_LOG(INFO) << "Run Data Path:" << run;
  char *r_method = (char *)malloc(sizeof(char) * RUN_NUM);
  Key_t *r_key = (Key_t *)malloc(sizeof(Key_t *) * RUN_NUM);
  Value_t *r_value = (Value_t *)malloc(sizeof(Value_t *) * RUN_NUM);
  for (int i = 0; i < RUN_NUM; i++)
  {
    r_key[i] = (Key_t)malloc(sizeof(char) * KEY_LENGTH);
    r_value[i] = (Value_t)malloc(sizeof(char) * VALUE_LENGTH);
  }
  ifstream r_ifs;
  r_ifs.open(run);
  if (!r_ifs)
  {
    cerr << "No file." << endl;
    exit(1);
  }
  else
  {
    for (int i = 0; i < RUN_NUM; i++)
    {
      r_ifs >> r_method[i];
      r_ifs >> key;
      // memcpy(r_keys[i], &key, sizeof(uint64_t));
      sprintf(r_key[i], "%ld", key);
      r_ifs.getline(r_value[i], VALUE_LENGTH);
    }
    r_ifs.close();
    RDMA_LOG(INFO) << run << " is used.";
  }
#endif

#ifdef UPDATE
  UPDATE_NUM = conf.get("update_num").get_uint64();
  string update = conf.get("update_data_path").get_str();
  RDMA_LOG(INFO) << "Update Data Path:" << update;
  char *u_method = (char *)malloc(sizeof(char) * UPDATE_NUM);
  Key_t *u_key = (Key_t *)malloc(sizeof(Key_t *) * UPDATE_NUM);
  Value_t *u_value = (Value_t *)malloc(sizeof(Value_t *) * UPDATE_NUM);
  for (int i = 0; i < UPDATE_NUM; i++)
  {
    u_key[i] = (Key_t)malloc(sizeof(char) * KEY_LENGTH);
    u_value[i] = (Value_t)malloc(sizeof(char) * VALUE_LENGTH);
  }
  ifstream u_ifs;
  u_ifs.open(update);
  if (!u_ifs)
  {
    cerr << "No file." << endl;
    exit(1);
  }
  else
  {
    for (int i = 0; i < UPDATE_NUM; i++)
    {
      u_ifs >> u_method[i];
      u_ifs >> key;
      // memcpy(r_keys[i], &key, sizeof(uint64_t));
      sprintf(u_key[i], "%ld", key);
      u_ifs.getline(u_value[i], VALUE_LENGTH);
    }
    u_ifs.close();
    RDMA_LOG(INFO) << update << " is used.";
  }
#endif

#ifdef DELETE
  DELETE_NUM = conf.get("delete_num").get_uint64();
  string delete_file = conf.get("delete_data_path").get_str();
  RDMA_LOG(INFO) << "Delete Data Path:" << delete_file;

  char *d_method = (char *)malloc(sizeof(char) * DELETE_NUM);
  Key_t *d_key = (Key_t *)malloc(sizeof(Key_t *) * DELETE_NUM);
  Value_t *d_value = (Value_t *)malloc(sizeof(Value_t *) * DELETE_NUM);
  // use same file
  if(delete_file.compare(load) == 0)
  {
    RDMA_LOG(INFO) << "Same file as load.";
    for (int i = 0; i < DELETE_NUM; i++)
    {
      d_key[i] = l_key[i];
      d_value[i] = l_value[i];
      d_method[i] = 'd';
    }
  }
  else
  {
  for (int i = 0; i < DELETE_NUM; i++)
  {
    d_key[i] = (Key_t)calloc(KEY_LENGTH, sizeof(char));
    d_value[i] = (Value_t)calloc(VALUE_LENGTH, sizeof(char));
  }
  l_ifs.open(delete_file);
  if (!l_ifs)
  {
    cerr << "No file." << endl;
    exit(1);
  }
  else
  {
    for (int i = 0; i < DELETE_NUM; i++)
    {
      l_ifs >> d_method[i];
      l_ifs >> key;
      // memcpy(l_keys[i], &key, sizeof(uint64_t));
      sprintf(d_key[i], "%ld", key);
      l_ifs.getline(d_value[i], VALUE_LENGTH);
    }
    l_ifs.close();
    RDMA_LOG(INFO) << delete_file << " is used.";
  } 
  }
#endif

  /* Start working */
#ifdef TEST_LATENCY
  long *load_latency, *run_latency;
  load_latency = (long *)malloc(sizeof(long) * LOAD_NUM);
  run_latency = (long *)malloc(sizeof(long) * RUN_NUM);
#endif
  atomic<int> signal; // For multi thread start
  signal = 0;
  tx_id_generator = 0; // Initial transaction id == 0
  connected_t_num = 0; // Sync all threads' RDMA QP connections
  auto thread_arr = new std::thread[thread_num_per_machine];

  auto *global_meta_man = new MetaManager();
  RDMA_LOG(INFO) << "Alloc local memory: " << (size_t)(thread_num_per_machine * PER_THREAD_ALLOC_SIZE) / (1024 * 1024) << " MB. Waiting...";
  auto *global_rdma_region = new RDMARegionAllocator(global_meta_man, thread_num_per_machine);

  auto *param_arr = new struct thread_params[thread_num_per_machine];

  RACE *race_client = nullptr;

  if (bench_name == "race")
  {
    ;
  }

  RDMA_LOG(INFO) << "Spawn threads to execute...";

  size_t chunk_size = LOAD_NUM / thread_num_per_machine;

  for (t_id_t i = 0; i < thread_num_per_machine; i++)
  {
    param_arr[i].thread_local_id = i;
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].coro_num = coro_num;
    param_arr[i].bench_name = bench_name;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].global_rdma_region = global_rdma_region;
    param_arr[i].thread_num_per_machine = thread_num_per_machine;
    param_arr[i].total_thread_num = (machine_num * thread_num_per_machine);
    param_arr[i].finish_num = 0;
    param_arr[i].load_num = LOAD_NUM / thread_num_per_machine;
    param_arr[i].run_cnt = 1;
    param_arr[i].insert_blked = total_insert_blked;
    param_arr[i].blk_cnt = total_blk_cnt;
    param_arr[i].blk_lock = total_blk_lock;
    param_arr[i].global_dir_lock = &global_dir_lock;
    param_arr[i].global_cache_dir = cache_dir;
    param_arr[i].global_split_lock = &global_split_lock;
    param_arr[i].symbol = &global_symbol;
#ifdef WARMUP
    param_arr[i].warm_up = 1;
    param_arr[i].warm_num = WARM_NUM / thread_num_per_machine;
#else
    param_arr[i].warm_up = 0;
    param_arr[i].warm_num = 0;
#endif
  }


  cout << param_arr[0].warm_num << " " << param_arr[0].load_num << endl;
#ifdef WARMUP
  size_t w_chunk_size = WARM_NUM / thread_num_per_machine;
  for (t_id_t i = 0; i < thread_num_per_machine; i++)
  {
    if (i != thread_num_per_machine - 1)
    {
      thread_arr[i] = std::thread(warm_thread,
                                  &param_arr[i],
                                  w_key,
                                  w_value,
                                  i * w_chunk_size,
                                  w_chunk_size,
                                  machine_id,
                                  &signal,
                                  nullptr);
    }
    else
    {
      thread_arr[i] = std::thread(warm_thread,
                                  &param_arr[i],
                                  w_key,
                                  w_value,
                                  i * w_chunk_size,
                                  WARM_NUM - i * w_chunk_size,
                                  machine_id,
                                  &signal,
                                  nullptr);
    }
  /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i * 2+NUMA_NODE, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }

  while (signal.load() != thread_num_per_machine)
  {
    asm("nop");
  }

  // thread 0 sets remote ready singal
  signal = SET_REMOTE;
  while (signal.load() != FINISH_SET)
  {
    asm("nop");
  }
  RDMA_LOG(INFO) << "Init Dir Success! Ready Go!";
  signal = START_RUN;
  for (t_id_t i = 0; i < thread_num_per_machine; i++)
  {
    if (thread_arr[i].joinable())
    {
      thread_arr[i].join();
    }
  }
  std::cout << "Warm up! " << WARM_NUM << " used: " << std::endl;
  signal = 0;
  connected_t_num = 0; // reset
#endif

  for (t_id_t i = 0; i < thread_num_per_machine; i++)
  {
    if (i != thread_num_per_machine - 1)
    {
#ifdef TEST_LATENCY
      thread_arr[i] = std::thread(load_thread,
                                  &param_arr[i],
                                  l_key,
                                  l_value,
                                  i * chunk_size,
                                  chunk_size,
                                  machine_id,
                                  &signal,
                                  load_latency);
#else
      thread_arr[i] = std::thread(load_thread,
                                  &param_arr[i],
                                  l_key,
                                  l_value,
                                  i * chunk_size,
                                  chunk_size,
                                  machine_id,
                                  &signal,
                                  nullptr);
#endif
    }
    else
    {
#ifdef TEST_LATENCY
      thread_arr[i] = std::thread(load_thread,
                                  &param_arr[i],
                                  l_key,
                                  l_value,
                                  i * chunk_size,
                                  LOAD_NUM - i * chunk_size,
                                  machine_id,
                                  &signal,
                                  load_latency);
#else
      thread_arr[i] = std::thread(load_thread,
                                  &param_arr[i],
                                  l_key,
                                  l_value,
                                  i * chunk_size,
                                  LOAD_NUM - i * chunk_size,
                                  machine_id,
                                  &signal,
                                  nullptr);
#endif
    }


    /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i * 2+NUMA_NODE, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }

  while (signal.load() != thread_num_per_machine)
  {
    asm("nop");
  }

  // thread 0 sets remote ready singal
  signal = SET_REMOTE;
  while (signal.load() != FINISH_SET)
  {
    asm("nop");
  }
  RDMA_LOG(INFO) << "Init Dir Success! Ready Go!";
  clock_gettime(CLOCK_REALTIME, &msr_start);
  signal = START_RUN;

  bool monitor = true;
#ifdef MONITOR
  std::thread t = std::thread(&get_real_time_info,
                              param_arr,
                              &monitor);
#endif
  for (t_id_t i = 0; i < thread_num_per_machine; i++)
  {
    if (thread_arr[i].joinable())
    {
      thread_arr[i].join();
    }
  }
  monitor = false;
  clock_gettime(CLOCK_REALTIME, &msr_end);
  double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
  std::cout << "Insert Speed: " << LOAD_NUM / msr_sec << "\t" << " used: " << msr_sec << std::endl;
  signal = 0;
  connected_t_num = 0; // reset

#ifdef UPDATE_SIZE
  uint64_t total_size = 0;
  uint64_t total_cnt = 0;
  for(int i = 0; i < thread_num_per_machine; i++)
  {
    total_size += param_arr[i].update_size;
    total_cnt += param_arr[i].update_cnt;
  }
  std::cout << "total update size: " << total_size << " B" << std::endl;
  std::cout << "total update :" << total_cnt << " times" << std::endl;
#endif

#ifdef TEST_LATENCY
  sort(load_latency, load_latency + LOAD_NUM);
  for(int i = 1; i <= 10000; i++)
  {
    if(i == 10000)
    {
      cout << i << "%:\t" << load_latency[(int)(LOAD_NUM) - 1] << endl;
      break;
    }
    cout << i << "%:\t" << load_latency[(int)(LOAD_NUM * 0.0001 * i)] << endl;
  }
  // cout << "min: " << load_latency[0] << "\t50%: " << load_latency[(int)(LOAD_NUM * 0.5)] << "\t1%: " << load_latency[(int)(LOAD_NUM * 0.01)] << "\tmax: " << load_latency[LOAD_NUM - 1] << "\tmax-1%: " << load_latency[(int)(LOAD_NUM - 2)] << endl;
  // cout << "aver: " << aver(load_latency, LOAD_NUM) << "\t50%: " << load_latency[(int)(LOAD_NUM * 0.5)] << "\t90%: " << load_latency[(int)(LOAD_NUM * 0.9)] << "\t95%: " << load_latency[(int)(LOAD_NUM * 0.95)] << "\t99%: " << load_latency[(int)(LOAD_NUM * 0.99)] << "\t99.9%: " << load_latency[(int)(LOAD_NUM * 0.999)] << "\tmax: " << load_latency[LOAD_NUM - 1] << "\tmax-1%: " << load_latency[(int)(LOAD_NUM - 2)] << endl;
#endif

#ifdef TWO_STEP
char ctrl;
std::cout << "input enter to continue..." << std::endl;
std::cin >> ctrl; 
#endif

#ifndef ONLY_INSERT
  chunk_size = RUN_NUM / thread_num_per_machine;
  // n_chunk_size = LOAD_NUM / thread_num_per_machine;
  for (t_id_t i = 0; i < thread_num_per_machine; i++)
  {
    if (i != thread_num_per_machine - 1)
    {
#ifdef TEST_LATENCY
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  r_key,
                                  r_value,
                                  r_method,
                                  i * chunk_size,
                                  chunk_size,
                                  machine_id,
                                  &signal,
                                  run_latency);
#else
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  r_key,
                                  r_value,
                                  r_method,
                                  i * chunk_size,
                                  chunk_size,
                                  machine_id,
                                  &signal,
                                  nullptr);
#endif
    }
    else
    {
#ifdef TEST_LATENCY
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  r_key,
                                  r_value,
                                  r_method,
                                  i * chunk_size,
                                  RUN_NUM - i * chunk_size,
                                  machine_id,
                                  &signal,
                                  run_latency);
#else
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  r_key,
                                  r_value,
                                  r_method,
                                  i * chunk_size,
                                  RUN_NUM - i * chunk_size,
                                  machine_id,
                                  &signal,
                                  nullptr);
#endif
    }

    /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i * 2 + NUMA_NODE, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }
  while (signal.load() != thread_num_per_machine)
  {
    asm("nop");
  }

  signal = SET_REMOTE;
  while (signal.load() != FINISH_SET)
  {
    asm("nop");
  }
  RDMA_LOG(INFO) << "Init Dir Success! Ready Go!";
  clock_gettime(CLOCK_REALTIME, &msr_start);
  signal = START_RUN;
  for (t_id_t i = 0; i < thread_num_per_machine; i++)
  {
    if (thread_arr[i].joinable())
    {
      thread_arr[i].join();
    }
  }
  clock_gettime(CLOCK_REALTIME, &msr_end);
  msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
  std::cout << "Search Speed: " << RUN_NUM / msr_sec << "\t" << "used: " << msr_sec << std::endl;
  signal = 0;
   connected_t_num = 0; // reset
#ifdef TEST_LATENCY
  sort(run_latency, run_latency + RUN_NUM);
  cout << "min: " << run_latency[0] << "\t50%: " << run_latency[(int)(RUN_NUM * 0.5)] << "\t1%: " << run_latency[(int)(RUN_NUM * 0.01)] << "\tmax: " << run_latency[RUN_NUM - 1] << "\tmax-1%: " << run_latency[(int)(RUN_NUM - 2)] << endl;
  cout << "aver: " << aver(run_latency, RUN_NUM) << "\t50%: " << run_latency[(int)(RUN_NUM * 0.5)] << "\t90%: " << run_latency[(int)(RUN_NUM * 0.9)] << "\t95%: " << run_latency[(int)(RUN_NUM * 0.95)] << "\t99%: " << run_latency[(int)(RUN_NUM * 0.99)] << "\t99.9%: " << run_latency[(int)(RUN_NUM * 0.999)] << "\tmax: " << run_latency[RUN_NUM - 1] << "\tmax-1%: " << run_latency[(int)(RUN_NUM - 2)] << endl;

#endif

#ifdef UPDATE_SIZE
  total_size = 0;
  total_cnt = 0;
  for(int i = 0; i < thread_num_per_machine; i++)
  {
    total_size += param_arr[i].update_size;
    total_cnt += param_arr[i].update_cnt;
  }
  std::cout << "total update size: " << total_size << " B" << std::endl;
  std::cout << "total update :" << total_cnt << " times" << std::endl;
#endif

#endif


#ifdef TWO_STEP
ctrl;
std::cout << "input enter to continue..." << std::endl;
std::cin >> ctrl; 
#endif

cout << "Update" << endl;

#ifdef UPDATE
chunk_size = UPDATE_NUM /thread_num_per_machine;
  for (t_id_t i = 0; i < thread_num_per_machine; i++)
  {
    if (i != thread_num_per_machine - 1)
    {
#ifdef TEST_LATENCY
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  u_key,
                                  u_value,
                                  u_method,
                                  i * chunk_size,
                                  chunk_size,
                                  machine_id,
                                  &signal,
                                  run_latency);
#else
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  u_key,
                                  u_value,
                                  u_method,
                                  i * chunk_size,
                                  chunk_size,
                                  machine_id,
                                  &signal,
                                  nullptr);
#endif
    }
    else
    {
#ifdef TEST_LATENCY
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  u_key,
                                  u_value,
                                  u_method,
                                  i * chunk_size,
                                  UPDATE_NUM - i * chunk_size,
                                  machine_id,
                                  &signal,
                                  run_latency);
#else
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  u_key,
                                  u_value,
                                  u_method,
                                  i * chunk_size,
                                  UPDATE_NUM - i * chunk_size,
                                  machine_id,
                                  &signal,
                                  nullptr);
#endif
    }

    /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i * 2+NUMA_NODE, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }
  while (signal.load() != thread_num_per_machine)
  {
    asm("nop");
  }

  signal = SET_REMOTE;
  while (signal.load() != FINISH_SET)
  {
    asm("nop");
  }
  RDMA_LOG(INFO) << "Init Dir Success! Ready Go!";
  clock_gettime(CLOCK_REALTIME, &msr_start);
  signal = START_RUN;
  for (t_id_t i = 0; i < thread_num_per_machine; i++)
  {
    if (thread_arr[i].joinable())
    {
      thread_arr[i].join();
    }
  }
  clock_gettime(CLOCK_REALTIME, &msr_end);
  signal = 0;
  connected_t_num = 0; // reset

  msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
  std::cout << "Update Speed: " << UPDATE_NUM / msr_sec << "\t" << "used: " << msr_sec << std::endl;

#ifdef TEST_LATENCY
  sort(run_latency, run_latency + UPDATE_NUM);
  cout << "min: " << run_latency[0] << "\t50%: " << run_latency[(int)(UPDATE_NUM * 0.5)] << "\t1%: " << run_latency[(int)(UPDATE_NUM * 0.01)] << "\tmax: " << run_latency[UPDATE_NUM - 1] << "\tmax-1%: " << run_latency[(int)(UPDATE_NUM - 2)] << endl;
  cout << "aver: " << aver(run_latency, UPDATE_NUM) << "\t50%: " << run_latency[(int)(UPDATE_NUM * 0.5)] << "\t90%: " << run_latency[(int)(UPDATE_NUM * 0.9)] << "\t95%: " << run_latency[(int)(UPDATE_NUM * 0.95)] << "\t99%: " << run_latency[(int)(UPDATE_NUM * 0.99)] << "\t99.9%: " << run_latency[(int)(UPDATE_NUM * 0.999)] << "\tmax: " << run_latency[UPDATE_NUM - 1] << "\tmax-1%: " << run_latency[(int)(UPDATE_NUM - 2)] << endl;
#endif
#endif


#ifdef DELETE
  chunk_size = DELETE_NUM /thread_num_per_machine;
  for (t_id_t i = 0; i < thread_num_per_machine; i++)
  {
    if (i != thread_num_per_machine - 1)
    {
#ifdef TEST_LATENCY
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  l_key,
                                  l_value,
                                  d_method,
                                  i * chunk_size,
                                  chunk_size,
                                  machine_id,
                                  &signal,
                                  run_latency);
#else
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  d_key,
                                  d_value,
                                  d_method,
                                  i * chunk_size,
                                  chunk_size,
                                  machine_id,
                                  &signal,
                                  nullptr);
#endif
    }
    else
    {
#ifdef TEST_LATENCY
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  l_key,
                                  l_value,
                                  d_method,
                                  i * chunk_size,
                                  DELETE_NUM - i * chunk_size,
                                  machine_id,
                                  &signal,
                                  run_latency);
#else
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  d_key,
                                  d_value,
                                  d_method,
                                  i * chunk_size,
                                  DELETE_NUM - i * chunk_size,
                                  machine_id,
                                  &signal,
                                  nullptr);
#endif
    }

    /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i * 2+NUMA_NODE, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }
  while (signal.load() != thread_num_per_machine)
  {
    asm("nop");
  }

  RDMA_LOG(INFO) << "Delete will start soon!";


  signal = SET_REMOTE;
  while (signal.load() != FINISH_SET)
  {
    asm("nop");
  }
  RDMA_LOG(INFO) << "Init Dir Success! Ready Go!";
  clock_gettime(CLOCK_REALTIME, &msr_start);
  signal = START_RUN;
  for (t_id_t i = 0; i < thread_num_per_machine; i++)
  {
    if (thread_arr[i].joinable())
    {
      thread_arr[i].join();
    }
  }
  clock_gettime(CLOCK_REALTIME, &msr_end);
  signal = 0;
  connected_t_num = 0; // reset

  msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
  std::cout << "Delete Speed: " << DELETE_NUM / msr_sec << "\t" << "used: " << msr_sec << std::endl;
#ifdef TEST_LATENCY
  sort(run_latency, run_latency + DELETE_NUM);
  cout << "min: " << run_latency[0] << "\t50%: " << run_latency[(int)(DELETE_NUM * 0.5)] << "\t1%: " << run_latency[(int)(DELETE_NUM * 0.01)] << "\tmax: " << run_latency[DELETE_NUM - 1] << "\tmax-1%: " << run_latency[(int)(DELETE_NUM - 2)] << endl;
  cout << "aver: " << aver(run_latency, DELETE_NUM) << "\t50%: " << run_latency[(int)(DELETE_NUM * 0.5)] << "\t90%: " << run_latency[(int)(DELETE_NUM * 0.9)] << "\t95%: " << run_latency[(int)(DELETE_NUM * 0.95)] << "\t99%: " << run_latency[(int)(DELETE_NUM * 0.99)] << "\t99.9%: " << run_latency[(int)(DELETE_NUM * 0.999)] << "\tmax: " << run_latency[DELETE_NUM - 1] << "\tmax-1%: " << run_latency[(int)(DELETE_NUM - 2)] << endl;
#endif

#endif

  uint64_t group_cnt[11] = {0};
  for(int i = 0; i < 30; i++)
  {
    for(auto it = total_insert_blked[i].begin(); it != total_insert_blked[i].end(); it ++)
    {
      if(it->second <= 10)
        group_cnt[it->second] ++;
      else
      {
        group_cnt[10] ++;
        cout << "over : " << it->second << endl;
      }
      // cout << i << " " << it->first << " " << it->second << endl;
    }
  }
  for(int i = 1; i <= 10; i ++)
  {
    cout << i << " " << group_cnt[i] << endl;
  }

  uint64_t res = 0;
  for(int i = 0; i < thread_num_per_machine; i++)
  {
    res += total_blk_cnt[i];
  }
  std::cout << "total blk cnt is: " << res << std::endl;


  RDMA_LOG(INFO) << "DONE";

  delete[] param_arr;
  delete global_rdma_region;
  delete global_meta_man;
  if (race_client)
    delete race_client;
}

void Handler::OutputResult(std::string bench_name, std::string system_name)
{
  std::string results_cmd = "mkdir -p ../../../bench_results/" + bench_name;
  system(results_cmd.c_str());
  std::ofstream of, of_detail, of_abort_rate;
  std::string res_file = "../../../bench_results/" + bench_name + "/result.txt";
  std::string detail_res_file = "../../../bench_results/" + bench_name + "/detail_result.txt";
  std::string abort_rate_file = "../../../bench_results/" + bench_name + "/abort_rate.txt";

  of.open(res_file.c_str(), std::ios::app);
  of_detail.open(detail_res_file.c_str(), std::ios::app);
  of_abort_rate.open(abort_rate_file.c_str(), std::ios::app);

  of_detail << system_name << std::endl;
  of_detail << "tid attemp_tp tp 50lat 99lat" << std::endl;

  of_abort_rate << system_name << " tx_type try_num commit_num abort_rate" << std::endl;

  double total_attemp_tp = 0;
  double total_tp = 0;
  double total_median = 0;
  double total_tail = 0;

  for (int i = 0; i < tid_vec.size(); i++)
  {
    of_detail << tid_vec[i] << " " << attemp_tp_vec[i] << " " << tp_vec[i] << " " << medianlat_vec[i] << " " << taillat_vec[i] << std::endl;
    total_attemp_tp += attemp_tp_vec[i];
    total_tp += tp_vec[i];
    total_median += medianlat_vec[i];
    total_tail += taillat_vec[i];
  }

  size_t thread_num = tid_vec.size();

  double avg_median = total_median / thread_num;
  double avg_tail = total_tail / thread_num;

  std::sort(medianlat_vec.begin(), medianlat_vec.end());
  std::sort(taillat_vec.begin(), taillat_vec.end());

  // of_detail << total_attemp_tp << " " << total_tp << " " << medianlat_vec[0] << " " << medianlat_vec[thread_num - 1]
  //           << " " << avg_median << " " << taillat_vec[0] << " " << taillat_vec[thread_num - 1] << " " << avg_tail << std::endl;

  // of << system_name << " " << total_attemp_tp / 1000 << " " << total_tp / 1000 << " " << avg_median << " " << avg_tail << std::endl;

  of_detail << std::endl;
  of_abort_rate << std::endl;

  of.close();
  of_detail.close();
  of_abort_rate.close();

  std::cerr << system_name << " " << total_attemp_tp / 1000 << " " << total_tp / 1000 << " " << avg_median << " " << avg_tail << std::endl;

  // Open it when testing the duration
#if LOCK_WAIT
  if (bench_name == "MICRO")
  {
    // print avg lock duration
    std::string file = "../../../bench_results/" + bench_name + "/avg_lock_duration.txt";
    of.open(file.c_str(), std::ios::app);

    double total_lock_dur = 0;
    for (int i = 0; i < lock_durations.size(); i++)
    {
      total_lock_dur += lock_durations[i];
    }

    of << system_name << " " << total_lock_dur / lock_durations.size() << std::endl;
    std::cerr << system_name << " avg_lock_dur: " << total_lock_dur / lock_durations.size() << std::endl;
  }
#endif
}

void Handler::ConfigureComputeNodeForMICRO(int argc, char *argv[])
{
  std::string workload_filepath = "../../../config/micro_config.json";
  std::string arg = std::string(argv[1]);
  char access_type = arg[0];
  std::string s;
  if (access_type == 's')
  {
    // skewed
    s = "sed -i '4c \"is_skewed\": true,' " + workload_filepath;
  }
  else if (access_type == 'u')
  {
    // uniform
    s = "sed -i '4c \"is_skewed\": false,' " + workload_filepath;
  }
  system(s.c_str());
  // write ratio
  std::string write_ratio = arg.substr(2); // e.g: 75
  s = "sed -i '7c \"write_ratio\": " + write_ratio + ",' " + workload_filepath;
  system(s.c_str());
}
