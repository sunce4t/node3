#include <city.h>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <list>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>
#include <city.h>

#include "allocator/buffer_allocator.h"
#include "base/common.h"

#include "connection/meta_manager.h" //define hash_index.h
#include "connection/qp_manager.h"
// #include "memstore/hash_index.h"
#include "race/doorbell.h"

#include "util/debug.h"
#include "util/hash.h"
#include "util/json_config.h"

#define FINGER(x, y) ((uint8_t)(CityHash32((x), (y)) >> 24))

class RACE
{
public:
    RACE(MetaManager *meta_man,
         QPManager *qp_man,
         t_id_t tid,
         coro_id_t coroid,
         CoroutineScheduler *sched,
         RDMABufferAllocator *rdma_buffer_allocator,
         uint64_t thread_kv_offset,
         uint64_t thread_num_per_machine,
         Directory *dir,
         size_t *local_lock);

    size_t Insert(coro_yield_t &yield, char *key, char *value, uint32_t key_len, uint32_t value_len, size_t used);
    bool Search(coro_yield_t &yield, char *key, uint32_t key_len, char *re_value);
    bool Update(coro_yield_t &yield, char *key, char *value, uint32_t key_len, uint32_t value_len, size_t used);
    bool Delete(coro_yield_t &yield, char *key, uint32_t key_len);
    // 分裂更新suffix和local_depth
    SplitStatus Split(coro_yield_t &yield, uint64_t suffix, uint64_t point, uint64_t level, uint64_t seg_idx);
    bool Re_read(coro_yield_t &yield, Bucket *buc, size_t hash, size_t *point, size_t *level, size_t *seg_idx, uint64_t *offset, RCQP *qp);
    bool UpdateDir(coro_yield_t &yield, size_t level_num, bool sync_flag); // update dir whether sync
    bool UpdateRestDir(coro_yield_t &yield, depth_t now_depth);
    void SendSplit(coro_yield_t &yield, uint64_t point, uint64_t level);
    bool RDMACAS(coro_id_t coro_id, RCQP *qp, char *local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap);
    bool RDMARead(coro_id_t coro_id, RCQP *qp,char *local_buf, uint64_t remote_offset, size_t size);
    bool RDMAWrite(coro_id_t coro_id, RCQP *qp,char *local_buf, uint64_t remote_offset, size_t size);

public:
    CoroutineScheduler *coro_sched;                // Thread local coroutine scheduler
    RDMABufferAllocator *thread_rdma_buffer_alloc; /// alloc local memory region
    QPManager *thread_qp_man;                      // Thread local qp connection manager. Each transaction thread has one
    MetaManager *global_meta_man;                  // Global metadata manager
    Directory *cache_dir;                          // cache check
    size_t *local_level_lock;                      // use to lock update directory
    version_t dir_version;
    RCQP *qp;

    t_id_t t_id;
    coro_id_t coro_id;
    uint64_t dir_offset;
    uint64_t seg_offset;
    uint64_t kv_offset; // per thread
    uint64_t partial_offset;

    uint64_t dir_ptr;
    uint64_t seg_ptr;
    uint64_t kv_ptr; // per thread
    uint64_t count_ptr;
    uint64_t split_ptr;
    uint64_t partial_ptr;

    uint64_t *update_size;
    uint64_t *cas_size;
    uint64_t *read_size;
    uint64_t *write_size;

    uint64_t *full_size;
    uint64_t *partial_size;
    uint64_t *hit_cnt;
    uint64_t *total_cnt;
};
