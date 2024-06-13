// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include "allocator/region_allocator.h"
#include "base/common.h"
// #include "connection/meta_manager.h"
#include "race/race.h" //define meta_manager

struct thread_params
{
  t_id_t thread_local_id;
  t_id_t thread_global_id;
  t_id_t thread_num_per_machine;
  t_id_t total_thread_num;
  size_t warm_num;
  size_t load_num;
  uint64_t warm_up;
  uint64_t run_cnt;
  MetaManager *global_meta_man;
  RDMARegionAllocator *global_rdma_region;
  int coro_num;
  uint64_t machine_id;
  std::string bench_name;

  uint64_t update_size;
  uint64_t update_cnt;
  uint64_t insert_cnt;
  uint64_t split_cnt;
  uint64_t finish_num;

  // std::atomic<uint64_t> *global_dir_lock;
  // std::mutex *global_dir_lock;
  std::uint64_t *global_dir_lock;
  Directory * global_cache_dir;
  uint64_t *global_split_lock;
  uint64_t *symbol;

  uint64_t *blk_cnt;
  std::mutex *blk_lock;
  std::unordered_map<uint64_t, uint64_t>* insert_blked;
};

void warm_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency);
void load_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency);
void delete_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency);
void update_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, char *_method_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency);
int run_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, char *_method_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency);

void get_real_time_info(thread_params *params, bool *run);