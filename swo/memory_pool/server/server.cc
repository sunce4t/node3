// Author: Ming Zhang
// Copyright (c) 2022

#include "server.h"

#include <stdlib.h>
#include <unistd.h>

#include <thread>
#include <math.h>
#include <city.h>

#include "util/json_config.h"

uint64_t partial_num, hash_num, i_hash_num;

void Server::AllocMem()
{
  RDMA_LOG(INFO) << "Start allocating memory...";
  hash_buffer = (char *)malloc(hash_buf_size);
  assert(hash_buffer);
  RDMA_LOG(INFO) << "Alloc DRAM data region success!";
  offset_t reserve_start = hash_buf_size * 0.9; // reserve 10% for hash conflict in case of full bucket
  hash_reserve_buffer = hash_buffer + reserve_start;
}

void Server::InitMem()
{
  RDMA_LOG(INFO) << "Start initializing memory...";
  memset(hash_buffer, 0, hash_buf_size);
  RDMA_LOG(INFO) << "Initialize memory success!";
}

void Server::InitRDMA()
{
  /************************************* RDMA+PM Initialization ***************************************/
  RDMA_LOG(INFO) << "Start initializing RDMA...";
  rdma_ctrl = std::make_shared<RdmaCtrl>(server_node_id, local_port);
  RdmaCtrl::DevIdx idx{.dev_id = 0, .port_id = 1}; // using the first RNIC's first port
  rdma_ctrl->open_thread_local_device(idx);
  RDMA_ASSERT(
      rdma_ctrl->register_memory(SERVER_HASH_BUFF_ID, hash_buffer, hash_buf_size, rdma_ctrl->get_device()) == true);
  RDMA_LOG(INFO) << "Register memory success!";
}

// All servers need to load data
void Server::LoadData(node_id_t machine_id,
                      node_id_t machine_num, // number of memory nodes
                      std::string &workload)
{
  /************************************* Load Data ***************************************/
  RDMA_LOG(INFO) << "Start loading database data...";
  // Init tables
  MemStoreAllocParam mem_store_alloc_param(hash_buffer, hash_buffer, 0, hash_reserve_buffer); // region start,store start,offset,end
  MemStoreReserveParam mem_store_reserve_param(hash_reserve_buffer, 0, hash_buffer + hash_buf_size);
  if (workload == "SWO")
  {
    race_server = new RACE_DB();
    race_server->LoadIndex(machine_id, machine_num, &mem_store_alloc_param, &mem_store_reserve_param);
  }
  RDMA_LOG(INFO) << "Loading table successfully!";
}

void Server::CleanTable()
{
  if (race_server)
  {
    delete race_server;
    RDMA_LOG(INFO) << "delete race tables";
  }
}

void Server::CleanQP()
{
  rdma_ctrl->destroy_rc_qp();
}

void Server::SendMeta(node_id_t machine_id, std::string &workload, size_t compute_node_num)
{
  // Prepare hash meta
  char *index_meta_buffer = nullptr;
  size_t total_meta_size = 0;
  PrepareIndexMeta(machine_id, workload, &index_meta_buffer, total_meta_size); // 在index_meta_buffer中写入机器id,分配的哈希表的长度等
  assert(index_meta_buffer != nullptr);
  assert(total_meta_size != 0);

  // Send memory store meta to all the compute nodes via TCP
  for (size_t index = 0; index < compute_node_num; index++)
  {
    SendIndexMeta(index_meta_buffer, total_meta_size);
  }
  free(index_meta_buffer);
}

void Server::PrepareIndexMeta(node_id_t machine_id, std::string &workload, char **index_meta_buffer, size_t &total_meta_size)
{
  // Get all hash meta
  std::vector<IndexMeta *> primary_index_meta_vec;
  std::vector<HashIndex *> all_priamry_indexes;
  if (workload == "SWO")
  {
    all_priamry_indexes = race_server->GetPrimaryHashStore();
  }
  for (auto &hash_index : all_priamry_indexes)
  {
    auto *index_meta = new IndexMeta(hash_index->GetIndexID(),
                                     hash_index->GetDirOffset(),
                                     hash_index->GetSegOffset(),
                                     hash_index->GetKVOffset(),
                                     (uint64_t)hash_index->GetDirPtr(),
                                     (uint64_t)hash_index->GetSegPtr(),
                                     (uint64_t)hash_index->GetKVPtr(),
                                     (uint64_t)hash_index->count_ptr,
                                     (uint64_t)hash_index->split_ptr,
                                     (uint64_t)hash_index->partial_ptr,
                                     hash_index->GetBucketNum(),
                                     DEFAULT_DEPTH,
                                     hash_index->GetBaseOff());
    primary_index_meta_vec.emplace_back(index_meta);
  }

  int index_meta_len = sizeof(IndexMeta);
  size_t primary_index_meta_num = primary_index_meta_vec.size();
  RDMA_LOG(INFO) << "primary hash meta num: " << primary_index_meta_num;
  total_meta_size = sizeof(primary_index_meta_num) + sizeof(machine_id) + primary_index_meta_num * index_meta_len + sizeof(MEM_STORE_META_END);
  *index_meta_buffer = (char *)malloc(total_meta_size);

  char *local_buf = *index_meta_buffer;

  // Fill primary hash meta
  *((size_t *)local_buf) = primary_index_meta_num;
  local_buf += sizeof(primary_index_meta_num);
  *((node_id_t *)local_buf) = machine_id;
  local_buf += sizeof(machine_id);
  for (size_t i = 0; i < primary_index_meta_num; i++)
  {
    memcpy(local_buf + i * index_meta_len, (char *)primary_index_meta_vec[i], index_meta_len);
  }
  local_buf += primary_index_meta_num * index_meta_len;
  // EOF
  *((uint64_t *)local_buf) = MEM_STORE_META_END;
}

void Server::SendIndexMeta(char *index_meta_buffer, size_t &total_meta_size)
{
  //> Using TCP to send hash meta
  /* --------------- Initialize socket ---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(local_meta_port);   // change host little endian to big endian
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY); // change host "0.0.0.0" to big endian
  int listen_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (listen_socket < 0)
  {
    RDMA_LOG(ERROR) << "Server creates socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server creates socket success";
  if (bind(listen_socket, (const struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
  {
    RDMA_LOG(ERROR) << "Server binds socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server binds socket success";
  int max_listen_num = 10;
  if (listen(listen_socket, max_listen_num) < 0)
  {
    RDMA_LOG(ERROR) << "Server listens error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server listens success";
  int from_client_socket = accept(listen_socket, NULL, NULL);
  // int from_client_socket = accept(listen_socket, (struct sockaddr*) &client_addr, &client_socket_length);
  if (from_client_socket < 0)
  {
    RDMA_LOG(ERROR) << "Server accepts error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server accepts success";

  /* --------------- Sending hash metadata ----------------- */
  auto retlen = send(from_client_socket, index_meta_buffer, total_meta_size, 0);
  if (retlen < 0)
  {
    RDMA_LOG(ERROR) << "Server sends hash meta error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  RDMA_LOG(INFO) << "Server sends hash meta success";
  size_t recv_ack_size = 100;
  char *recv_buf = (char *)malloc(recv_ack_size);
  recv(from_client_socket, recv_buf, recv_ack_size, 0);
  if (strcmp(recv_buf, "[ACK]index_meta_received_from_client") != 0)
  {
    std::string ack(recv_buf);
    RDMA_LOG(ERROR) << "Client receives hash meta error. Received ack is: " << ack;
  }

  free(recv_buf);
  close(from_client_socket);
  close(listen_socket);
}

void Server::TotalSearch(uint64_t locate)
{
  Pair *kv = (Pair *)race_server->index->GetKVPtr();
  Segment *seg = (Segment *)race_server->index->GetSegPtr();
  Pair *find = &kv[locate];
  for (int i = 0; i < MAX_SEG; i++)
  {
    for (int j = 0; j < BUCKET_NUM; j++)
    {
      for (int k = 0; k < SLOT_NUM; k++)
      {
        if (GetPair(seg[i].bucket[j].slot[k]) == find)
        {
          cout << "Find! Seg:" << i << " Bucket: " << j << " Slot: " << k << endl;
          return;
        }
      }
    }
  }
  cout << "Not Find" << endl;
  return;
}

bool Server::Run(size_t compute_node_num)
{
  // Now server just waits for user typing quit to finish
  // Server's CPU is not used during one-sided RDMA requests from clients
  // printf("====================================================================================================\n");
  // printf(
  //     "Server now starts counting times\n");
  // while (((TimeCount *)race_server->index->count_ptr)->finish_client_num != compute_node_num)
  // {
  //   usleep(2000);
  // }
  // double max = 0;
  // for (int i = 0; i < compute_node_num; i++)
  // {
  //   if (max < ((TimeCount *)race_server->index->count_ptr)->client_time[i])
  //     max = ((TimeCount *)race_server->index->count_ptr)->client_time[i];
  // }
  // std::string config_filepath = "../../../config/race_config.json";

  // auto r_json_config = JsonConfig::load_file(config_filepath);
  // auto conf = r_json_config.get("swo");
  // uint64_t key;
  // size_t load_num = conf.get("load_num").get_uint64();
  // std::cout << "Load Speed: " << load_num / max << "\t" << std::endl;
#ifndef ONLY_INSERT
#endif
  printf("====================================================================================================\n");
  printf(
      "Server now runs as a disaggregated mode. No CPU involvement during RDMA-based transaction processing\n"
      "Type c to run another round, type q if you want to exit :)\n");
  while (true)
  {
    char ch;
    scanf("%c", &ch);
    if (ch == 'q')
    {
      return false;
    }
    else if (ch == 'c')
    {
      return true;
    }
    else if (ch == 't')
    {
      GetActInsert();
    }
    else if (ch == 'l')
    {
      GetLoadFactor();
    }
    else if (ch == 'f')
    {
      printf("Type locate to find:");
      uint64_t locate;
      scanf("%ld", &locate);
      TotalSearch(locate);
    }
    else
    {
      printf("Type c to run another round, type q if you want to exit :)\n");
    }
    cout << "partial num: " << partial_num << "\thash_num: " << hash_num << "\ti_hash_num: " << i_hash_num << endl;
    usleep(2000);
  }
}

void Server::ServerSplit(uint64_t point)
{
  ;
}

// without paritial bit

// void Server::ServerSplit(uint64_t point)
// {
//   Segment *seg = (Segment *)race_server->index->GetSegPtr();
//   Directory *dir = (Directory *)race_server->index->GetDirPtr();
//   size_t move_bit = 0, level = 0, seg_idx;
//   auto l_depth = GetDepth(point);
//   auto mod_depth = l_depth % SEG_DEPTH;
//   auto act_depth = GetAddr(point)->bucket[0].suffix; // 实际旧的对应的段的id
//   do
//   {
//     seg_idx = (act_depth >> move_bit) & (SEG_NUM - 1);
//     if ((dir->level[level].ls_point[seg_idx] & ls_mask) != 0) //
//     {
//       level = dir->level[level].ls_point[seg_idx] & (ls_mask - 1);
//       move_bit += SEG_DEPTH; // move to next seg
//     }
//     else
//     {
//       break;
//     }
//   } while (true);
//   if (GetDepth(point) % SEG_DEPTH == 0) // this time cache_dir old
//   {
//     uint32_t used = dir->pool_used++;
//     if (dir->pool_used >= MAX_LEVEL)
//     {
//       RDMA_LOG(ERROR) << "Exceed Max LEVEL";
//       exit(-1);
//     }
//     for (int i = 0; i < SEG_NUM; i++)
//     {
//       dir->level[used].ls_point[i] = point;
//     }
//     // CAS point
//     CAS(&dir->level[level].ls_point[seg_idx], &dir->level[level].ls_point[seg_idx], ((uint64_t)(used) | ls_mask));
//     level = used;
//     seg_idx = 0;

//     //   for (int i = 0; i < pow(2, dir->global_depth); i++) // dir split
//     //   {

//     //     dir->seg[i + (int)pow(2, dir->global_depth)] = dir->seg[i]; // 将新分裂出来的目录的指针指向之前的段
//     //   }
//     //   cout << "dir split:" << seg_num << "\t" << dir->global_depth << endl;
//     //   CAS(&dir->global_depth, &dir->global_depth, dir->global_depth + 1);
//   }

//   auto new_locate = act_depth + (int)pow(2, l_depth); // 新段的id

//   for (int j = 0; j < BUCKET_NUM; j++)
//   {
//     CAS(&seg[act_depth].bucket[j].local_depth, &seg[act_depth].bucket[j].local_depth, seg[act_depth].bucket[j].local_depth + 1);
//   }
//   for (int j = 0; j < BUCKET_NUM; j++)
//   {
//     auto p_bucket = p_key[act_depth][j]; // partial key bucket
//     auto bucket = seg[act_depth].bucket[j];
//     seg[new_locate].bucket[j].local_depth = bucket.local_depth;
//     seg[new_locate].bucket[j].suffix = new_locate;
//     for (int k = 0; k < SLOT_NUM; k++)
//     {
//       if (bucket.slot[k] == NULL)
//         flag_seg[j][k] = 0;
//       else
//       {
// #ifdef PARTIAL_KEY
//         if (p_bucket[k] == NULL) // true
// #else
//         if (true)
// #endif
//         {
//           hash_num++;
//           uint64_t hash = CityHash64WithSeed(GetPair(bucket.slot[k])->key, KEY_LENGTH, f_seed); // 这个gdb这里有问题
//           auto seg_idx = hash & ((int)pow(2, bucket.local_depth) - 1);
//           auto buc_idx = hash >> (64 - BUCKET_SUFFIX);
//           auto b_offset = buc_idx / 2 * 3 + (buc_idx % 2);
//           // 如果第一种hash不对
//           if ((seg_idx != new_locate && seg_idx != act_depth) || (b_offset != j && b_offset + 1 != j))
//           {
//             // use cuckoo
//             hash = CityHash64WithSeed(GetPair(bucket.slot[k])->key, KEY_LENGTH, s_seed);
//             seg_idx = hash & ((int)pow(2, l_depth + 1) - 1);
//           }
//           // auto seg_idx = rand() % 2 == 0 ? new_locate : act_depth;

//           if (seg_idx != new_locate)
//           {
//             flag_seg[j][k] = 1;
// #ifdef PARTIAL_KEY
//             p_bucket[k] = hash >> (DEFAULT_DEPTH * SEG_DEPTH);
// #endif
//           }
//           else
//           {
//             seg[new_locate].bucket[j].slot[k] = bucket.slot[k];
//             flag_seg[j][k] = 2;
// #ifdef PARTIAL_KEY
//             p_key[new_locate][j][k] = hash >> (DEFAULT_DEPTH * SEG_DEPTH);
// #endif
//           }
//           // #ifdef PARTIAL_KEY
//           //           if (p_key[new_locate][j][0] != 0 && p_key[new_locate][j][k] == p_key[new_locate][j][0] && k != 0)   //same?
//           //             cout << "error!!!" << before << "now: " << p_key[new_locate][j][0] << endl;
//           // #endif
//         }
//         else
//         {
//           partial_num++;
//           if ((p_bucket[k] & (1 << (bucket.local_depth - (DEFAULT_DEPTH * SEG_DEPTH + 1)))) == 0) // act_depth;
//           {
//             flag_seg[j][k] = 1;
//           }
//           else
//           {
//             seg[new_locate].bucket[j].slot[k] = bucket.slot[k];
//             flag_seg[j][k] = 2;
// #ifdef PARTIAL_KEY
//             p_key[new_locate][j][k] = p_bucket[k];
//             p_bucket[k] = 0;
// #endif
//           }
//         }
//       }
//     }
//   }
//   int check = 0;
//   // // 多个seg指针指向同一个段，对它们进行分离
//   for (int i = 0; seg_idx + i < SEG_NUM; i += pow(2, mod_depth))
//   {
//     if (check % 2 == 0)
//     {
//       // only modified local_depth
//       CAS(&dir->level[level].ls_point[seg_idx + i], &dir->level[level].ls_point[seg_idx + i], dir->level[level].ls_point[seg_idx + i] + (1UL << 48));
//     }
//     else
//     {
//       // both change
//       uint64_t s = CreateSegUnit((Segment *)((uint64_t)&seg[new_locate]), l_depth + 1);
//       CAS(&dir->level[level].ls_point[seg_idx + i], &dir->level[level].ls_point[seg_idx + i], s);
//     }
//     check++;
//   }

//   // // delete

//   for (int i = 0; i < BUCKET_NUM; i++)
//   {
//     for (int j = 0; j < SLOT_NUM; j++)
//     {
//       if (seg[act_depth].bucket[i].slot[j] != NULL)
//       {
//         if (flag_seg[i][j] == 2)
//         {
//           CAS(&seg[act_depth].bucket[i].slot[j], &seg[act_depth].bucket[i].slot[j], nullptr); // delete
//           // seg[act_depth].bucket[i].slot[j] = 0;
//         }
//         else if (flag_seg[i][j] == 0)
//         {
//           i_hash_num++;
//           // cout << "empty" << seg_num << "\t" << act_depth << endl;
//           Pair *kv_buf = GetPair(seg[act_depth].bucket[i].slot[j]);
//           auto hash = CityHash64WithSeed(((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, f_seed);
//           auto seg_idx = hash & ((int)pow(2, l_depth + 1) - 1);
//           auto buc_idx = hash >> (64 - BUCKET_SUFFIX);
//           auto b_offset = buc_idx / 2 * 3 + (buc_idx % 2);
//           if ((seg_idx != new_locate && seg_idx != act_depth) || (b_offset != i && b_offset + 1 != i)) //
//           {
//             // use cuckoo
//             hash = CityHash64WithSeed(((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, s_seed);
//             seg_idx = hash & ((int)pow(2, l_depth + 1) - 1);
//           }
//           if (seg_idx == new_locate)
//           {
//             CAS(&seg[act_depth].bucket[i].slot[j], &seg[act_depth].bucket[i].slot[j], nullptr); // delete   //todo?re insert?
// #ifdef PARTIAL_KEY
//             p_key[new_locate][i][j] = hash >> (DEFAULT_DEPTH * SEG_DEPTH);
// #endif
//           }
//           else
//           {
// #ifdef PARTIAL_KEY
//             p_key[act_depth][i][j] = hash >> (DEFAULT_DEPTH * SEG_DEPTH);
// #endif
//           }
//         }
//       }
//     }
//   }
// }

void Server::GetSplit(size_t compute_node_num, bool *isrun)
{
  partial_num = 0;
  hash_num = 0;
  i_hash_num = 0;
  memset(p_key, 0, sizeof(uint16_t) * MAX_SEG * BUCKET_NUM * SLOT_NUM);
  uint64_t *_ptr = (uint64_t *)race_server->index->split_ptr;
  Segment *seg = (Segment *)race_server->index->GetSegPtr();
  Directory *dir = (Directory *)race_server->index->GetDirPtr();
  while (*isrun)
  {
    for (int i = 0; i <= compute_node_num * MAX_THREAD * MAX_COR; i++)
    {
      if (_ptr[i] != 0)
      {
        if (GetDepth(_ptr[i]) == GetAddr(_ptr[i])->bucket[0].local_depth)
          ServerSplit(_ptr[i]);
        else
        {
          // cout << "not match" << endl;
        }
        CAS((uint64_t *)&_ptr[i], &_ptr[i], NULL);
      }
    }
  }
}

void Server::ScanPartial(bool *isrun)
{
  p_key = (partial_t *)race_server->index->partial_ptr;
  char get_key[16];
  Segment *seg = (Segment *)race_server->index->GetSegPtr();
  Directory *dir = (Directory *)race_server->index->GetDirPtr();
  uint64_t max_depth = DEFAULT_DEPTH * SEG_DEPTH;
  uint64_t max = (uint64_t)pow(2, max_depth);
  uint64_t hash, buc_idx, read, scan_num = 0;
  Pair *bind_addr;
  unsigned long offset, seg_idx;
  partial_t partial;
  while (*isrun)
  {
    uint64_t p_offset = 0;
    for (int i = 0; i < max; i++)
    {
      if (seg[i].seg_lock == UNLOCK)
      {
        if (max_depth < seg[i].bucket[0].local_depth && seg[i].bucket[0].local_depth < 50) // for not error TODO may
        {
          max_depth = seg[i].bucket[0].local_depth;
          max = (uint64_t)pow(2, max_depth);
        }
        for (int j = 0; j < BUCKET_NUM; j++)
        {
          for (int k = 0; k < SLOT_NUM; k++)
          {
            p_offset = i * 48 * 7 + j * 7 + k;
            bind_addr = seg[i].bucket[j].slot[k];
            if ((bind_addr != 0) && (p_key[p_offset] == 0))
            {
              memcpy(get_key, GetPair(bind_addr)->key, KEY_LENGTH);
              hash = CityHash64WithSeed(get_key, KEY_LENGTH, f_seed);
              seg_idx = hash & ((int)pow(2, seg[i].bucket[j].local_depth) - 1);
              buc_idx = hash >> (64 - BUCKET_SUFFIX);
              offset = buc_idx / 2 * 3 + (buc_idx % 2);
              if ((seg_idx != i) || (offset != j && offset + 1 != j))
              {
                // cout << get_key << "i j k:" << i << j << k << endl;
                hash = CityHash64WithSeed(get_key, KEY_LENGTH, s_seed);
                seg_idx = hash & ((int)pow(2, seg[i].bucket[j].local_depth) - 1);
                buc_idx = hash >> (64 - BUCKET_SUFFIX);
                offset = buc_idx / 2 * 3 + (buc_idx % 2);
                if ((seg_idx != i) || (offset != j && offset + 1 != j))
                {
                  if (seg[i].seg_lock != UNLOCK)
                  { // seg is locking
                    goto SKIP;
                  }
                  else
                  {
                    cout << "error" << endl;
                  }
                }
              }
              if (bind_addr != seg[i].bucket[j].slot[k])
              {
                continue; // the item has been remove or delete or split
              }
              // align
              // if (k < 4)
              // {
              //   read = *((uint64_t *)&p_key[i][j][0]);
              //   partial = (hash >> (DEFAULT_DEPTH * SEG_DEPTH));
              //   read = read + (partial << (k * 16));
              //   CAS(((uint64_t *)&p_key[i][j][0]), ((uint64_t *)&p_key[i][j][0]), read); // add cas partial
              // }
              // else
              // {
              //   read = *((uint64_t *)&p_key[i][j][4]);
              //   partial = (hash >> (DEFAULT_DEPTH * SEG_DEPTH));
              //   read = read + (partial << ((k - 4) * 16));
              //   CAS((uint64_t *)&p_key[i][j][4], ((uint64_t *)&p_key[i][j][4]), read);
              // }
              read = *((uint64_t *)&p_key[p_offset]);
              partial = (hash >> (DEFAULT_DEPTH * SEG_DEPTH));
              read = read + partial;
              CAS((&p_key[p_offset]), (&p_key[p_offset]), partial);
              if ((p_key[p_offset] == 0) && (partial != 0))
                cout << p_key[p_offset] << partial << endl;
              scan_num++;
              if (scan_num % 1000000 == 0)
                cout << "scan_num: " << scan_num << "offset: " << p_offset << endl;
            }
          }
        }
      }
    SKIP:;
    }
  }
}
void Server::GetLoadFactor(bool *isrun)
{
  while (*isrun)
  {
    size_t used = 0, total = 0;
    Directory *dir_ptr = (Directory *)race_server->index->GetDirPtr();
    // for (int i = 0; i < pow(2, dir_ptr->global_depth); i++)
    // {
    //   auto l_depth = GetDepth(dir_ptr->seg[i]);
    //   auto act_depth = i & ((int)pow(2, l_depth) - 1); // 实际旧的对应的段的id
    //   if (act_depth == i)
    //   {
    //     for (int j = 0; j < BUCKET_NUM; j++)
    //     {
    //       for (int k = 0; k < SLOT_NUM; k++)
    //       {
    //         if (GetAddr(dir_ptr->seg[i])->bucket[j].slot[k] != NULL)
    //           used++;
    //         total++;
    //       }
    //     }
    //   }
    // }
    // if (used / ((double)total) >= 0.5)
    //   cout << "Load Factor: " << used / ((double)total) << endl;
    // usleep(2000);
  }
}

void Server::GetLoadFactor() // presss
{
  uint64_t total_used = 0, total = 0;
  Segment *seg = (Segment *)race_server->index->GetSegPtr();
  for (int i = 0; i < MAX_SEG; i++)
  {
    if (seg[i].bucket[0].local_depth != 0)
      total += SLOT_NUM * BUCKET_NUM;
    for (int j = 0; j < BUCKET_NUM; j++)
    {
      for (int k = 0; k < SLOT_NUM; k++)
      {
        if (seg[i].bucket[j].slot[k] != 0)
          total_used++;
      }
    }
  }
  cout << "Load Factor: " << ((double)total_used) / total << endl;
}

void Server::GetActInsert()
{
  uint64_t total = 0;
  Segment *seg = (Segment *)race_server->index->GetSegPtr();
  for (int i = 0; i < MAX_SEG; i++)
  {
    for (int j = 0; j < BUCKET_NUM; j++)
    {
      for (int k = 0; k < SLOT_NUM; k++)
      {
        if (seg[i].bucket[j].slot[k] != 0)
          total++;
      }
    }
  }
  cout << "Total Insert: " << total << endl;
}

int main(int argc, char *argv[])
{
  // Configure of this server
  std::string config_filepath = "../../../config/memory_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);

  auto local_node = json_config.get("local_memory_node");
  node_id_t machine_num = (node_id_t)local_node.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)local_node.get("machine_id").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);
  int local_port = (int)local_node.get("local_port").get_int64();
  int local_meta_port = (int)local_node.get("local_meta_port").get_int64();
  int use_pm = (int)local_node.get("use_pm").get_int64();
  std::string pm_root = local_node.get("pm_root").get_str();
  std::string workload = local_node.get("workload").get_str();
  auto mem_size_GB = local_node.get("mem_size_GB").get_uint64();
  auto log_buf_size_GB = local_node.get("log_buf_size_GB").get_uint64();

  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto compute_node_ips = compute_nodes.get("compute_node_ips"); // Array
  size_t compute_node_num = compute_node_ips.size();

  // std::string pm_file = pm_root + "pm_node" + std::to_string(machine_id); // Use fsdax
  std::string pm_file = pm_root; // Use devdax
  size_t mem_size = (size_t)1024 * 1024 * 1024 * mem_size_GB;
  size_t hash_buf_size = mem_size; // Currently, we support the hash structure
  size_t log_buf_size = (size_t)1024 * 1024 * 1024 * log_buf_size_GB;

  auto server = std::make_shared<Server>(machine_id, local_port, local_meta_port, hash_buf_size, log_buf_size, use_pm, pm_file, mem_size);
  server->AllocMem();
  server->InitMem();
  server->LoadData(machine_id, machine_num, workload);
  server->SendMeta(machine_id, workload, compute_node_num);
  server->InitRDMA();
  bool run = true;

#ifdef SERVER_SPLIT
  std::thread t = std::thread(&Server::GetSplit,
                              server,
                              compute_node_num,
                              &run);
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
#ifdef NUMA_NODE
  CPU_SET(1 ^ NUMA_NODE, &cpuset);
#else
  CPU_SET(MAX_THREAD + 1, &cpuset);
#endif
  int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0)
  {
    RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
  }
#endif
#ifdef SERVER_SCAN
  std::thread t = std::thread(&Server::ScanPartial,
                              server,
                              &run);
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
#ifdef NUMA_NODE
  CPU_SET(1 ^ NUMA_NODE, &cpuset);
#else
  CPU_SET(MAX_THREAD + 1, &cpuset);
#endif
  int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0)
  {
    RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
  }
#endif

  bool run_next_round = server->Run(compute_node_num);
  run = false;
#ifdef SERVER_SPLIT
  if (t.joinable())
    t.join();
#endif

  // Continue to run the next round. RDMA does not need to be inited twice
  while (run_next_round)
  {
    server->InitMem();
    server->CleanTable();
    server->CleanQP();
    server->LoadData(machine_id, machine_num, workload);
    server->SendMeta(machine_id, workload, compute_node_num);
    run = true;
#ifdef SERVER_SPLIT
    std::thread t = std::thread(&Server::GetSplit,
                                server,
                                compute_node_num,
                                &run);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
#ifdef NUMA_NODE
    CPU_SET(1 ^ NUMA_NODE, &cpuset);
#else
    CPU_SET(MAX_THREAD + 1, &cpuset);
#endif
    int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
#endif
#ifdef SERVER_SCAN
    std::thread t = std::thread(&Server::ScanPartial,
                                server,
                                &run);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
#ifdef NUMA_NODE
    CPU_SET(1 ^ NUMA_NODE, &cpuset);
#else
    CPU_SET(MAX_THREAD + 1, &cpuset);
#endif
    int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
#endif

    run_next_round = server->Run(compute_node_num);
    run = false;
#ifdef SERVER_SPLIT
    if (t.joinable())
      t.join();
#endif
  }

  // Stat the cpu utilization
  auto pid = getpid();
  std::string copy_cmd = "cp /proc/" + std::to_string(pid) + "/stat ./";
  system(copy_cmd.c_str());

  copy_cmd = "cp /proc/uptime ./";
  system(copy_cmd.c_str());
  return 0;
}