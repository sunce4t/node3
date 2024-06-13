#pragma once

#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "memstore/mem_store.h"
#include "memstore/structs.h"
#include "util/hash.h"
#include "util/debug.h"

#define OFFSET_NOT_FOUND -1
#define OFFSET_FOUND 0
#define VERSION_TOO_OLD -2 // The new version < old version

#define SLOT_NOT_FOUND -1
#define SLOT_INV -2
#define SLOT_LOCKED -3
#define SLOT_FOUND 0

const int SLOT_NUM_PER_SEG = 32;
const size_t SlotSize = sizeof(size_t);

struct IndexMeta
{
  // To which table this hash store belongs
  index_id_t index_id;

  // Virtual address of the table, used to calculate the distance
  // between some HashNodes with the table for traversing
  // the linked list
  uint64_t dir_offset;

  // The point to seg in alloc memory
  uint64_t seg_offset;

  // The point to kv_pool in alloc memory
  uint64_t kv_offset;

  // ptr
  uint64_t dir_ptr;

  // The point to seg in alloc memory
  uint64_t seg_ptr;

  // The point to kv_pool in alloc memory
  uint64_t kv_ptr;

  uint64_t count_ptr;

  // Offset of the table, relative to the RDMA local_mr
  offset_t base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // Size of hash node
  uint64_t depth;

  IndexMeta(index_id_t index_id,
            uint64_t dir_offset,
            uint64_t seg_offset,
            uint64_t kv_offset,
            uint64_t dir_ptr,
            uint64_t seg_ptr,
            uint64_t kv_ptr,
            uint64_t count_ptr,
            uint64_t bucket_num,
            uint64_t depth,
            offset_t base_off) : index_id(index_id),
                                 dir_offset(dir_offset),
                                 seg_offset(seg_offset),
                                 kv_offset(kv_offset),
                                 dir_ptr(dir_ptr),
                                 seg_ptr(seg_ptr),
                                 kv_ptr(kv_ptr),
                                 count_ptr(count_ptr),
                                 base_off(base_off),
                                 bucket_num(bucket_num),
                                 depth(depth) {}
  IndexMeta() {}
} Aligned8;

// A hashnode is a bucket
struct HashSeg
{
  // A dataitem is a slot
  size_t slots[SLOT_NUM_PER_SEG];
  HashSeg *next;
} Aligned8;

class HashIndex
{
public:
  HashIndex(index_id_t index_id, uint64_t bucket_num, MemStoreAllocParam *param)
      : index_id(index_id), base_off(0), bucket_num(bucket_num), node_num(0)
  {
    assert(bucket_num > 0);
    index_size = sizeof(Directory) + MAX_SEG * sizeof(Segment) + sizeof(Pair) * MAX_KV + sizeof(TimeCount); // size_t use signal
    region_start_ptr = param->mem_region_start;
    std::cout << (uint64_t)param->mem_store_start << " " << param->mem_store_alloc_offset + index_size << " " << (uint64_t)param->mem_store_reserve << endl;
    assert((uint64_t)param->mem_store_start + param->mem_store_alloc_offset + index_size <= (uint64_t)param->mem_store_reserve);
    dir_ptr = param->mem_store_start + param->mem_store_alloc_offset;
    seg_ptr = dir_ptr + sizeof(Directory);
    kv_ptr = seg_ptr + MAX_SEG * sizeof(Segment); // TODO write num need alloc
    count_ptr = kv_ptr + sizeof(Pair) * MAX_KV;
    param->mem_store_alloc_offset += index_size; // signal use 8 byte

    base_off = (uint64_t)dir_ptr - (uint64_t)region_start_ptr;
    assert(base_off >= 0);

    RDMA_LOG(INFO) << "Table " << index_id << " size: " << index_size / 1024 / 1024
                   << " MB. Start address: " << std::hex << "0x" << (uint64_t)dir_ptr
                   << ", base_off: 0x" << base_off << ", bucket_size: " << std::dec << SLOT_NUM_PER_SEG * SlotSize << " B";
    assert(dir_ptr != nullptr);
    assert(seg_ptr != nullptr);
    assert(kv_ptr != nullptr);
    memset(dir_ptr, 0, sizeof(Directory));
    memset(seg_ptr, 0, sizeof(Segment) * MAX_SEG);
    memset(kv_ptr, 0, sizeof(Pair) * MAX_KV);
    memset(count_ptr,0,sizeof(TimeCount));
  }

  index_id_t GetIndexID() const
  {
    return index_id;
  }

  offset_t GetBaseOff() const
  {
    return base_off;
  }

  uint64_t GetHashSegSize() const
  {
    return sizeof(HashSeg);
  }

  uint64_t GetBucketNum() const
  {
    return bucket_num;
  }

  char *GetDirPtr() const
  {
    return dir_ptr;
  }

  char *GetSegPtr() const
  {
    return seg_ptr;
  }

  char *GetKVPtr() const
  {
    return kv_ptr;
  }

  size_t GetDirOffset() const
  {
    return (uint64_t)dir_ptr - (uint64_t)region_start_ptr;
  }

  size_t GetSegOffset() const
  {
    return (uint64_t)seg_ptr - (uint64_t)region_start_ptr;
  }

  size_t GetKVOffset() const
  {
    return (uint64_t)kv_ptr - (uint64_t)region_start_ptr;
  }

  depth_t GetDepth() const
  {
    return ((Directory *)dir_ptr)->global_depth;
  }

  offset_t GetItemRemoteOffset(const void *item_ptr) const
  {
    return (uint64_t)item_ptr - (uint64_t)region_start_ptr;
  }

  uint64_t IndexSize() const
  {
    return index_size;
  }

  char *count_ptr;

private:
  // To which table this hash store belongs
  index_id_t index_id;

  // The offset in the RDMA region
  offset_t base_off;

  // Total hash buckets
  uint64_t bucket_num;

  // The point to dir in alloc memory
  char *dir_ptr;

  // The point to seg in alloc memory
  char *seg_ptr;

  // The point to kv_pool in alloc memory
  char *kv_ptr;

  // Total hash node nums
  uint64_t node_num;

  // The size of the entire hash table
  size_t index_size;

  // Start of the memory region address, for installing remote offset for data item
  char *region_start_ptr;
};
