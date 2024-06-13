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
  uint64_t run_cnt;
  uint64_t warm_up;

  MetaManager *global_meta_man;
  RDMARegionAllocator *global_rdma_region;
  int coro_num;
  std::string bench_name;

  uint64_t update_size;
  uint64_t cas_size;
  uint64_t read_size;
  uint64_t write_size;

  uint64_t finish_num;
  Directory * cache_dir;

  // used for test predictor/partial key
  uint64_t partial_size;
  uint64_t full_size;

  uint64_t hit_cnt;
  uint64_t total_cnt;
};

void warm_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency);
void load_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency);
int run_thread(thread_params *params, Key_t *_key_array, Value_t *_value_array, char *_method_array, size_t start, size_t num, node_id_t machine_id, atomic<int> *signal, long *latency);

void get_real_time_info(thread_params *params, bool *run);

void get_real_space_info(thread_params *params, bool *run);

void get_predictor_info(thread_params *params, bool *run);