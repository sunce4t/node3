#include "race/race.h"
#include "race/hash.h"

bool FALSE = false;
bool TRUE = true;

RACE::RACE(MetaManager *meta_man,
           QPManager *qp_man,
           t_id_t tid,
           coro_id_t coroid,
           CoroutineScheduler *sched,
           RDMABufferAllocator *rdma_buffer_allocator,
           uint64_t thread_kv_offset,
           uint64_t thread_num,
           Directory *dir,
           size_t *local_lock)
{
    t_id = tid;
    coro_id = coroid;
    coro_sched = sched;
    global_meta_man = meta_man;
    thread_qp_man = qp_man;
    thread_rdma_buffer_alloc = rdma_buffer_allocator;
    cache_dir = dir;
    local_level_lock = local_lock;
    dir_offset = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).dir_offset;
    seg_offset = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).seg_offset;
    kv_offset = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).kv_offset + thread_kv_offset;

    dir_ptr = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).dir_ptr;
    seg_ptr = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).seg_ptr;
    kv_ptr = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).kv_ptr + thread_kv_offset;
    count_ptr = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).count_ptr;
    split_ptr = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).split_ptr + ((tid / thread_num * MAX_THREAD * MAX_COR) + (tid % thread_num * MAX_COR) + coro_id) * sizeof(uint64_t);
    partial_ptr = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).partial_ptr;
    partial_offset = partial_ptr - dir_ptr;
    dir_version = 0;
    qp = thread_qp_man->GetRemoteDataQPWithNodeID(global_meta_man->GetPrimaryNodeID(DEFAULT_ID));
}

ALWAYS_INLINE
bool RACE::RDMACAS(coro_id_t coro_id, RCQP *qp, char *local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap)
{
    (*cas_size) = (*cas_size) + 8;
    return coro_sched->RDMACAS(coro_id, qp, local_buf, remote_offset, compare, swap);
}

ALWAYS_INLINE
bool RACE::RDMARead(coro_id_t coro_id, RCQP *qp, char *local_buf, uint64_t remote_offset, size_t size)
{
    (*read_size) = (*read_size) + size;
    return coro_sched->RDMARead(coro_id, qp, local_buf, remote_offset, size);
}

ALWAYS_INLINE
bool RACE::RDMAWrite(coro_id_t coro_id, RCQP *qp, char *local_buf, uint64_t remote_offset, size_t size)
{
    (*write_size) = (*write_size) + size;
    return coro_sched->RDMAWrite(coro_id, qp, local_buf, remote_offset, size);
}

bool RACE::UpdateDir(coro_yield_t &yield, size_t level_num, bool sync_flag = false)
{
    bool other_split = false;
    while (local_level_lock[level_num] != 0) // is locking used for split/udpate
    {
        coro_sched->YieldForUpdateDir(yield, coro_id); // yield for other thread
        other_split = true;
        // return false; // other cor is update/spliting
    }
    if (other_split)
        return true;
    local_level_lock[level_num] = coro_id; // lock it for cor
RE_UPDATE:
    // node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    // RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    size_t NowSize = SEG_NUM * LSPointSize; // get depth_t
    (*update_size) = (*update_size) + NowSize;
    uint64_t offset = global_meta_man->dir_offset + sizeof(size_t) + level_num * sizeof(Level);
    char *data_buf = thread_rdma_buffer_alloc->Alloc(NowSize);
    if (!sync_flag)
    {
        RDMARead(coro_id, qp, data_buf, offset, NowSize);
        coro_sched->Yield(yield, coro_id);
    }
    else
    {
        coro_sched->RDMAReadSync(coro_id, qp, data_buf, offset, NowSize);
    }
    memcpy(&cache_dir->level[level_num], data_buf, NowSize);
    // if (cache_dir->global_depth != before_gd)
    // {
    //     sync_flag = true; // this time dir is error so need sync
    //     goto RE_UPDATE;
    // }
    local_level_lock[level_num] = 0;
    return true;
}

bool RACE::UpdateRestDir(coro_yield_t &yield, depth_t now_depth)
{
    // while (*local_dir_lock != 0) // is locking used for split/udpate
    // {
    //     coro_sched->YieldForUpdateDir(yield, coro_id); // yield for other thread
    //     // return false; // other cor is update/spliting
    // }
    // *local_dir_lock = coro_id; // lock it for cor
    // node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    // RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    // depth_t before_gd = cache_dir->global_depth;
    // if (likely(before_gd != now_depth)) // other cor update
    // {
    //     size_t NowSize = (pow(2, now_depth) - pow(2, cache_dir->global_depth)) * sizeof(uint64_t); // get depth_t
    //     char *data_buf = thread_rdma_buffer_alloc->Alloc(NowSize);
    //     // cout << "init success" << t_id << " coro_id: " << coro_id << endl;
    //     RDMARead(coro_id, qp, data_buf, global_meta_man->dir_offset + sizeof(uint64_t) + pow(2, cache_dir->global_depth) * sizeof(uint64_t), NowSize);
    //     // cout << "send success" << t_id << " coro_id: " << coro_id << endl;
    //     coro_sched->Yield(yield, coro_id);
    //     // cout << "update success" << t_id << " coro_id: " << coro_id << endl;
    //     memcpy(&(cache_dir->seg[(int)pow(2, cache_dir->global_depth)]), data_buf, NowSize);
    //     cache_dir->global_depth = now_depth;
    // }
    // *local_dir_lock = 0;
    return true;
}

ALWAYS_INLINE
bool RACE::Re_read(coro_yield_t &yield, Bucket *buc, size_t hash, size_t *point, size_t *level, size_t *seg_idx, uint64_t *offset, RCQP *qp) // async  re read bucket
{
    // cout << "re_read:" << *seg_idx << endl;
    *level = 0;
    int move_bit = 0;
    do
    {
        *seg_idx = (hash >> move_bit) & (SEG_NUM - 1);
        if (cache_dir->level[*level].ls_point[*seg_idx] == 0)
        {
            UpdateDir(yield, *level); // the level is not load   means not equal?
        }
        if ((cache_dir->level[*level].ls_point[*seg_idx] & ls_mask) != 0) //
        {
            *level = cache_dir->level[*level].ls_point[*seg_idx] & (ls_mask - 1);
            move_bit += SEG_DEPTH; // move to next seg
        }
        else
        {
            *point = cache_dir->level[*level].ls_point[*seg_idx];
            break;
        }
    } while (true);
    uint32_t buc_idx = hash >> (64 - BUCKET_SUFFIX);
    do
    {
        *offset = (buc_idx / 2 * 3 + (buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(*point)) - seg_ptr + seg_offset;
        char *buf = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
        if (!RDMARead(coro_id, qp, buf, *offset, BucketSize * 2))
            return false;
        coro_sched->Yield(yield, coro_id);
        memcpy(buc, (void *)buf, BucketSize * 2);
    } while (*offset != (buc_idx / 2 * 3 + (buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(*point)) - seg_ptr + seg_offset);
    return true;
}

size_t RACE::Insert(coro_yield_t &yield, char *key, char *value, uint32_t key_len, uint32_t value_len, size_t used)
{
    // get qp
    // node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    // RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    bool write_flag = true;
    uint64_t f_hash = CityHash64WithSeed(key, key_len, f_seed);
    uint64_t s_hash = CityHash64WithSeed(key, key_len, s_seed);
    // uint64_t f_hash = hash_funcs[0](key, key_len, f_seed);
    // uint64_t s_hash = hash_funcs[1](key, key_len, s_seed);

    Pair *p; // only one
    uint64_t f_point, s_point, move_bit, f_level, s_level, f_seg_idx, s_seg_idx;
RE_START:
    move_bit = 0;
    f_level = 0;
    s_level = 0;
    do
    {
        f_seg_idx = (f_hash >> move_bit) & (SEG_NUM - 1);
        if (cache_dir->level[f_level].ls_point[f_seg_idx] == 0)
        {
            UpdateDir(yield, f_level); // the level is not load
        }
        if ((cache_dir->level[f_level].ls_point[f_seg_idx] & ls_mask) != 0) //
        {
            f_level = cache_dir->level[f_level].ls_point[f_seg_idx] & (ls_mask - 1);
            move_bit += SEG_DEPTH; // move to next seg
        }
        else
        {
            f_point = cache_dir->level[f_level].ls_point[f_seg_idx];
            break;
        }
    } while (true);
    move_bit = 0; // re_set
    do
    {
        s_seg_idx = (s_hash >> move_bit) & (SEG_NUM - 1);
        if (cache_dir->level[s_level].ls_point[s_seg_idx] == 0)
        {
            UpdateDir(yield, s_level); // the level is not load
        }
        if ((cache_dir->level[s_level].ls_point[s_seg_idx] & ls_mask) != 0) //
        {
            s_level = cache_dir->level[s_level].ls_point[s_seg_idx] & (ls_mask - 1);
            move_bit += SEG_DEPTH; // move to next seg
        }
        else
        {
            s_point = cache_dir->level[s_level].ls_point[s_seg_idx];
            break;
        }
    } while (true);
    uint32_t f_buc_idx = f_hash >> (64 - BUCKET_SUFFIX); // 需要额外减1？
    uint32_t s_buc_idx = s_hash >> (64 - BUCKET_SUFFIX);
    // cout << (uint64_t)(f_seg_idx) << "\t" << (uint64_t)(f_buc_idx) << endl;
    auto f_offset = (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)GetAddr(f_point) - seg_ptr + seg_offset;
    auto s_offset = (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)GetAddr(s_point) - seg_ptr + seg_offset;
    char *buf1 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    char *buf2 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    (*read_size) = (*read_size) + BucketSize * 4;
    if (write_flag)
    {
        (*write_size) = (*write_size) + sizeof(Pair);
        write_flag = false; // only insert one kv TODO这里偶尔会出现search fail
#ifdef PASSIVE_ACK
        std::shared_ptr<ReadWriteKVBatch> doorbell = std::make_shared<ReadWriteKVBatch>(true);
#else
        std::shared_ptr<ReadWriteKVBatch> doorbell = std::make_shared<ReadWriteKVBatch>();
#endif
        doorbell->SetReadCombineReq(buf1, f_offset, BucketSize * 2, 0);
        doorbell->SetReadCombineReq(buf2, s_offset, BucketSize * 2, 1);

        // 这里被修改指向新的了
        Pair kv;
        memcpy(kv.key, key, key_len);
        memcpy(kv.value, value, value_len);
        // kv.key[key_len] = '\0';
        // kv.value[value_len] = '\0';
        kv.key_len = key_len;
        kv.value_len = value_len;
        kv.crc = CityHash64(value, value_len); // value_CRC
        char *buf3 = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
        memcpy(buf3, &kv, sizeof(Pair));
        doorbell->SetWriteKVReq(buf3, used * sizeof(Pair) + kv_offset, sizeof(Pair));
        doorbell->SendReqs(coro_sched, qp, coro_id);
        coro_sched->Yield(yield, coro_id);

        p = (Pair *)((uint64_t)kv_ptr + used * sizeof(Pair));
        uint8_t finger = FINGER(key, key_len);
        uint8_t kv_len = key_len + value_len;
        p = CreateSlot(p, kv_len, finger); // create a available slot
        if (((uint64_t)p & addr_mask) < kv_ptr)
        {
            cout << "error " << endl;
        }

        // recheck whether cache is error
        if (f_offset != (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(f_point)) - seg_ptr + seg_offset ||
            s_offset != (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(s_point)) - seg_ptr + seg_offset)
        {
            goto RE_START;
        }
    }
    else
    {
        std::shared_ptr<ReadCombineBatch> doorbell = std::make_shared<ReadCombineBatch>();
        doorbell->SetReadCombineReq(buf1, f_offset, BucketSize * 2, 0);
        doorbell->SetReadCombineReq(buf2, s_offset, BucketSize * 2, 1);

        // 这里被修改指向新的了
        doorbell->SendReqs(coro_sched, qp, coro_id);
        coro_sched->Yield(yield, coro_id);

        // recheck whether cache is error
        if (f_offset != (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)GetAddr(f_point) - seg_ptr + seg_offset ||
            s_offset != (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)GetAddr(s_point) - seg_ptr + seg_offset)
        {
            goto RE_START;
        }
    }

    Bucket f_buc[2], s_buc[2];
    memcpy(f_buc, (void *)buf1, BucketSize * 2);
    memcpy(s_buc, (void *)buf2, BucketSize * 2);
RE_INSERT:

    if (f_buc[0].local_depth < s_buc[0].local_depth)
    {
    FRE_INSERT:
        // find and write
        for (int i = 0; i < SLOT_NUM; i++) // 比较suffix开始
        {
            for (int j = 0; j < 2; j++)
            {
                if (f_buc[j].slot[i] == nullptr) // 能不能Batch?
                {
                    if (f_buc[0].local_depth != GetDepth(f_point)) // 若正在分裂
                    {
                        UpdateDir(yield, f_level);
                        Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp); // 有没有必要？要不要？
                        goto RE_INSERT;                                                               // 再去搜一遍新桶
                    }
                    char *cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair *));
                    RDMACAS(coro_id, qp, cas_buf, f_offset + j * BucketSize + i * sizeof(Pair *), 0x0, (uint64_t)p); // 偏移量要加！！
                    coro_sched->Yield(yield, coro_id);
                    if (*(uint64_t *)cas_buf == 0x0)
                    {
                        size_t before_offset = f_offset, before_depth = GetDepth(f_point);
                        size_t suffix = f_buc[j].suffix;
                        Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp); // 有可能读到其他段
                        // suffix = (f_hash & ((int)pow(2, f_buc[j].local_depth) - 1));
                        if ((f_buc[j].local_depth != before_depth && f_buc[j].suffix != suffix) || f_offset != before_offset) // 其他的更新成功后会不会影响这里判断
                        // 分裂12阶段不匹配或者其他协程更新了Dir
                        {
                            cout << "Offset Error! Before Suffix: " << suffix << "Bucket Suffix: " << f_buc[j].suffix << endl;
                            RDMACAS(coro_id, qp, cas_buf, f_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)p, 0x0);
                            coro_sched->Yield(yield, coro_id);
                            if (*(uint64_t *)cas_buf == (uint64_t)f_buc[j].slot[i]) // swap成功
                            {
                                do
                                {
                                    UpdateDir(yield, f_level);
                                    Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp);
                                    suffix = (f_hash & ((int)pow(2, f_buc[j].local_depth) - 1));
                                } while (f_buc[j].suffix != suffix);
                                goto FRE_INSERT;
                            }
                        }
                        for (int m = 0; m < SLOT_NUM; m++)
                        {
                            for (int n = 0; n < 2; n++)
                            {
                                if (f_buc[n].slot[m] != 0x0 && FINGER(key, key_len) == GetFinger(f_buc[n].slot[m]))
                                {
                                    if (m != i && n != j)
                                    {
                                        char *read_kv_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
                                        uint64_t r_offset = (uint64_t)GetPair(f_buc[n].slot[m]) - dir_ptr;
                                        RDMARead(coro_id, qp, read_kv_buf, r_offset, sizeof(Pair));
                                        coro_sched->Yield(yield, coro_id);
                                        Pair kv_pair;
                                        memcpy(&kv_pair, (void *)read_kv_buf, sizeof(Pair));
                                        if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len))
                                        {
                                            // delete duplicate key
                                            RDMACAS(coro_id, qp, cas_buf, f_offset + n * BucketSize + m * sizeof(Pair *), (uint64_t)f_buc[n].slot[m], 0x0);
                                            coro_sched->Yield(yield, coro_id);
                                        }
                                    }
                                }
                            }
                        }
                        return 2;
                    }
                    else
                    {
                        Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp);
                        goto RE_INSERT;
                    }
                }
            }
        }
    SRE_INSERT:
        // find and write
        for (int i = 0; i < SLOT_NUM; i++) // 比较suffix开始
        {
            for (int j = 0; j < 2; j++)
            {
                if (s_buc[j].slot[i] == nullptr) // 能不能Batch?
                {
                    if (s_buc[0].local_depth != GetDepth(s_point)) // 若正在分裂
                    {
                        UpdateDir(yield, s_level);
                        Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp); // 有没有必要？要不要？
                        goto RE_INSERT;                                                               // 再去搜一遍新桶
                    }
                    char *cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair *));
                    RDMACAS(coro_id, qp, cas_buf, s_offset + j * BucketSize + i * sizeof(Pair *), 0x0, (uint64_t)p);
                    coro_sched->Yield(yield, coro_id);
                    if (*(uint64_t *)cas_buf == 0x0)
                    {
                        size_t before_offset = s_offset, before_depth = GetDepth(s_point);
                        size_t suffix = s_buc[j].suffix;
                        Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp);
                        // suffix = (s_hash & ((int)pow(2, s_buc[j].local_depth) - 1));
                        if ((s_buc[j].local_depth != before_depth && s_buc[j].suffix != suffix) || before_offset != s_offset) // 其他的更新成功后会不会影响这里判断
                        // 分裂12阶段不匹配或者其他协程更新了Dir
                        {
                            cout << "Offset Error! Before Suffix: " << suffix << "Bucket Suffix: " << s_buc[j].suffix << endl;
                            RDMACAS(coro_id, qp, cas_buf, s_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)p, 0x0);
                            coro_sched->Yield(yield, coro_id);
                            if (*(uint64_t *)cas_buf == (uint64_t)s_buc[j].slot[i]) // swap成功
                            {
                                do
                                {
                                    UpdateDir(yield, s_level);
                                    Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp);
                                    suffix = (s_hash & ((int)pow(2, s_buc[j].local_depth) - 1));
                                } while (s_buc[j].suffix != suffix);
                                goto SRE_INSERT;
                            }
                        }
                        for (int m = 0; m < SLOT_NUM; m++)
                        {
                            for (int n = 0; n < 2; n++)
                            {
                                if (s_buc[n].slot[m] != 0x0 && FINGER(key, key_len) == GetFinger(s_buc[n].slot[m]))
                                {
                                    if (m != i && n != j)
                                    {
                                        char *read_kv_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
                                        uint64_t r_offset = (uint64_t)GetPair(s_buc[n].slot[m]) - dir_ptr;
                                        RDMARead(coro_id, qp, read_kv_buf, r_offset, sizeof(Pair));
                                        coro_sched->Yield(yield, coro_id);
                                        Pair kv_pair;
                                        memcpy(&kv_pair, (void *)read_kv_buf, sizeof(Pair));
                                        if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len))
                                        {
                                            // delete duplicate key
                                            RDMACAS(coro_id, qp, cas_buf, s_offset + n * BucketSize + m * sizeof(Pair *), (uint64_t)s_buc[n].slot[m], 0x0);
                                            coro_sched->Yield(yield, coro_id);
                                        }
                                    }
                                }
                            }
                        }
                        return 3;
                    }
                    else
                    {
                        Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp);
                        goto RE_INSERT;
                    }
                }
            }
        }
    }
    else
    {
    SRE_INSERT2:
        // find and write
        for (int i = 0; i < SLOT_NUM; i++) // 比较suffix开始
        {
            for (int j = 0; j < 2; j++)
            {
                if (s_buc[j].slot[i] == nullptr) // 能不能Batch?
                {
                    if (s_buc[0].local_depth != GetDepth(s_point)) // 若正在分裂
                    {
                        UpdateDir(yield, s_level);
                        Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp); // 有没有必要？要不要？
                        goto RE_INSERT;                                                               // 再去搜一遍新桶
                    }
                    char *cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair *));
                    RDMACAS(coro_id, qp, cas_buf, s_offset + j * BucketSize + i * sizeof(Pair *), 0x0, (uint64_t)p);
                    coro_sched->Yield(yield, coro_id);
                    if (*(uint64_t *)cas_buf == 0x0)
                    {
                        size_t before_offset = s_offset, before_depth = GetDepth(s_point);
                        size_t suffix = s_buc[j].suffix;
                        Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp);
                        // suffix = (s_hash & ((int)pow(2, s_buc[j].local_depth) - 1));
                        if ((s_buc[j].local_depth != before_depth && s_buc[j].suffix != suffix) || before_offset != s_offset) // 其他的更新成功后会不会影响这里判断
                        // 分裂12阶段不匹配或者其他协程更新了Dir
                        {
                            cout << "Offset Error! Before Suffix: " << suffix << "Bucket Suffix: " << s_buc[j].suffix << endl;
                            RDMACAS(coro_id, qp, cas_buf, s_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)p, 0x0);
                            coro_sched->Yield(yield, coro_id);
                            if (*(uint64_t *)cas_buf == (uint64_t)s_buc[j].slot[i]) // swap成功
                            {
                                do
                                {
                                    UpdateDir(yield, s_level);
                                    Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp);
                                    suffix = (s_hash & ((int)pow(2, s_buc[j].local_depth) - 1));
                                } while (s_buc[j].suffix != suffix);
                                goto SRE_INSERT2;
                            }
                        }
                        for (int m = 0; m < SLOT_NUM; m++)
                        {
                            for (int n = 0; n < 2; n++)
                            {
                                if (s_buc[n].slot[m] != 0x0 && FINGER(key, key_len) == GetFinger(s_buc[n].slot[m]))
                                {
                                    if (m != i && n != j)
                                    {
                                        char *read_kv_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
                                        uint64_t r_offset = (uint64_t)GetPair(s_buc[n].slot[m]) - dir_ptr;
                                        RDMARead(coro_id, qp, read_kv_buf, r_offset, sizeof(Pair));
                                        coro_sched->Yield(yield, coro_id);
                                        Pair kv_pair;
                                        memcpy(&kv_pair, (void *)read_kv_buf, sizeof(Pair));
                                        if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len))
                                        {
                                            // delete duplicate key
                                            RDMACAS(coro_id, qp, cas_buf, s_offset + n * BucketSize + m * sizeof(Pair *), (uint64_t)s_buc[n].slot[m], 0x0);
                                            coro_sched->Yield(yield, coro_id);
                                        }
                                    }
                                }
                            }
                        }
                        return 3;
                    }
                    else
                    {
                        Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp);
                        goto RE_INSERT;
                    }
                }
            }
        }
    FRE_INSERT2:
        // find and write
        for (int i = 0; i < SLOT_NUM; i++) // 比较suffix开始
        {
            for (int j = 0; j < 2; j++)
            {
                if (f_buc[j].slot[i] == nullptr) // 能不能Batch?
                {
                    if (f_buc[0].local_depth != GetDepth(f_point)) // 若正在分裂
                    {
                        UpdateDir(yield, f_level);
                        Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp); // 有没有必要？要不要？
                        goto RE_INSERT;                                                               // 再去搜一遍新桶
                    }
                    char *cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair *));
                    RDMACAS(coro_id, qp, cas_buf, f_offset + j * BucketSize + i * sizeof(Pair *), 0x0, (uint64_t)p); // 偏移量要加！！
                    coro_sched->Yield(yield, coro_id);
                    if (*(uint64_t *)cas_buf == 0x0)
                    {
                        size_t before_offset = f_offset, before_depth = GetDepth(f_point);
                        size_t suffix = f_buc[j].suffix;
                        Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp); // 有可能读到其他段
                        // suffix = (f_hash & ((int)pow(2, f_buc[j].local_depth) - 1));
                        if ((f_buc[j].local_depth != before_depth && f_buc[j].suffix != suffix) || f_offset != before_offset) // 其他的更新成功后会不会影响这里判断
                        // 分裂12阶段不匹配或者其他协程更新了Dir
                        {
                            cout << "Offset Error! Before Suffix: " << suffix << "Bucket Suffix: " << f_buc[j].suffix << endl;
                            RDMACAS(coro_id, qp, cas_buf, f_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)p, 0x0);
                            coro_sched->Yield(yield, coro_id);
                            if (*(uint64_t *)cas_buf == (uint64_t)f_buc[j].slot[i]) // swap成功
                            {
                                do
                                {
                                    UpdateDir(yield, f_level);
                                    Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp);
                                    suffix = (f_hash & ((int)pow(2, f_buc[j].local_depth) - 1));
                                } while (f_buc[j].suffix != suffix);
                                goto FRE_INSERT2;
                            }
                        }
                        for (int m = 0; m < SLOT_NUM; m++)
                        {
                            for (int n = 0; n < 2; n++)
                            {
                                if (f_buc[n].slot[m] != 0x0 && FINGER(key, key_len) == GetFinger(f_buc[n].slot[m]))
                                {
                                    if (m != i && n != j)
                                    {
                                        char *read_kv_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
                                        uint64_t r_offset = (uint64_t)GetPair(f_buc[n].slot[m]) - dir_ptr;
                                        RDMARead(coro_id, qp, read_kv_buf, r_offset, sizeof(Pair));
                                        coro_sched->Yield(yield, coro_id);
                                        Pair kv_pair;
                                        memcpy(&kv_pair, (void *)read_kv_buf, sizeof(Pair));
                                        if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len))
                                        {
                                            // delete duplicate key
                                            RDMACAS(coro_id, qp, cas_buf, f_offset + n * BucketSize + m * sizeof(Pair *), (uint64_t)f_buc[n].slot[m], 0x0);
                                            coro_sched->Yield(yield, coro_id);
                                        }
                                    }
                                }
                            }
                        }
                        return 2;
                    }
                    else
                    {
                        Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp);
                        goto RE_INSERT;
                    }
                }
            }
        }
    }

#ifndef SERVER_SPLIT
    if (f_buc[0].local_depth < s_buc[0].local_depth)
    {
        if (f_buc[0].local_depth != GetDepth(f_point)) // 若正在分裂
        {
            // if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
            // {
            UpdateDir(yield, f_level);
            Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp); // TODO if the suffix is equal don't need to read???
            goto RE_INSERT;                                                               // 再去搜一遍新桶
            // }
        }
        switch (Split(yield, f_buc[0].suffix, f_point, f_level, f_seg_idx))
        {
        case OFFSETUPDATED:
            break;
        case OTHERCORUSED:
            // coro_sched->YieldForSplit(yield, coro_id);
            break;
        case SUCCSPLIT:
            break;
        case ERRORDIR: // re read and re try insert
            break;
        default:
            break;
        }
        Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp);
        goto RE_INSERT; // 还需要Check吗
    }

    if (s_buc[0].local_depth != GetDepth(s_point)) // 若正在分裂
    {
        // if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
        // {
        UpdateDir(yield, s_level);
        Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp);
        goto RE_INSERT; // 再去搜一遍新桶
        // }
    }
    switch (Split(yield, s_buc[0].suffix, s_point, s_level, s_seg_idx))
    {
    case OFFSETUPDATED:
        break;
    case OTHERCORUSED:
        // coro_sched->YieldForSplit(yield, coro_id);
        break;
    case SUCCSPLIT:
        break;
    case ERRORDIR: // re read and re try insert
        break;
    default:
        break;
    }
    Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp);
    goto RE_INSERT; // 还需要Check吗
#else

    if (f_buc[0].local_depth < s_buc[0].local_depth)
    {
        if (f_buc[0].local_depth != GetDepth(f_point)) // 若正在分裂
        {
            if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取   //for send split
            {
                UpdateDir(yield, f_level);
                Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp); // TODO if the suffix is equal don't need to read???
                goto RE_INSERT;                                                               // 再去搜一遍新桶
            }
        }
        SendSplit(yield, f_point, f_level);
        Re_read(yield, f_buc, f_hash, &f_point, &f_level, &f_seg_idx, &f_offset, qp);
        goto RE_INSERT; // 还需要Check吗
    }
    if (s_buc[0].local_depth != GetDepth(s_point)) // 若正在分裂
    {
        if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取   //for send split
        {
            UpdateDir(yield, s_level);
            Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp); // TODO if the suffix is equal don't need to read???
            goto RE_INSERT;                                                               // 再去搜一遍新桶
        }
    }
    SendSplit(yield, s_point, s_level);
    Re_read(yield, s_buc, s_hash, &s_point, &s_level, &s_seg_idx, &s_offset, qp);
    goto RE_INSERT; // 还需要Check吗

#endif
    return 0; // not available
}

bool RACE::Search(coro_yield_t &yield, char *key, uint32_t key_len, char *re_value)
{
    // get qp
    // node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    // RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    bool write_flag = true;
    uint64_t f_hash = CityHash64WithSeed(key, key_len, f_seed);
    uint64_t s_hash = CityHash64WithSeed(key, key_len, s_seed);
    // uint64_t f_hash = hash_funcs[0](key, key_len, f_seed);
    // uint64_t s_hash = hash_funcs[1](key, key_len, s_seed);
    Pair *p; // only one
    uint64_t f_point, s_point, move_bit, f_level, s_level, f_seg_idx, s_seg_idx;
RE_START:
    move_bit = 0;
    f_level = 0;
    s_level = 0;
    do
    {
        f_seg_idx = (f_hash >> move_bit) & (SEG_NUM - 1);
        if (cache_dir->level[f_level].ls_point[f_seg_idx] == 0)
        {
            UpdateDir(yield, f_level); // the level is not load
        }
        if ((cache_dir->level[f_level].ls_point[f_seg_idx] & ls_mask) != 0) //
        {
            f_level = cache_dir->level[f_level].ls_point[f_seg_idx] & (ls_mask - 1);
            move_bit += SEG_DEPTH; // move to next seg
        }
        else
        {
            f_point = cache_dir->level[f_level].ls_point[f_seg_idx];
            break;
        }
    } while (true);
    move_bit = 0; // re_set
    do
    {
        s_seg_idx = (s_hash >> move_bit) & (SEG_NUM - 1);
        if (cache_dir->level[s_level].ls_point[s_seg_idx] == 0)
        {
            UpdateDir(yield, s_level); // the level is not load
        }
        if ((cache_dir->level[s_level].ls_point[s_seg_idx] & ls_mask) != 0) //
        {
            s_level = cache_dir->level[s_level].ls_point[s_seg_idx] & (ls_mask - 1);
            move_bit += SEG_DEPTH; // move to next seg
        }
        else
        {
            s_point = cache_dir->level[s_level].ls_point[s_seg_idx];
            break;
        }
    } while (true);
    uint32_t f_buc_idx = f_hash >> (64 - BUCKET_SUFFIX); // 需要额外减1？
    uint32_t s_buc_idx = s_hash >> (64 - BUCKET_SUFFIX);
    // cout << (uint64_t)(f_seg_idx) << "\t" << (uint64_t)(f_buc_idx) << endl;
    auto f_offset = (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)GetAddr(f_point) - seg_ptr + seg_offset;
    auto s_offset = (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)GetAddr(s_point) - seg_ptr + seg_offset;
    char *buf1 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    char *buf2 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    std::shared_ptr<ReadCombineBatch> doorbell = std::make_shared<ReadCombineBatch>();
    doorbell->SetReadCombineReq(buf1, f_offset, BucketSize * 2, 0);
    doorbell->SetReadCombineReq(buf2, s_offset, BucketSize * 2, 1);
    doorbell->SendReqs(coro_sched, qp, coro_id);
    coro_sched->Yield(yield, coro_id);
    (*read_size) = (*read_size) + BucketSize * 4;

    Bucket f_buc[2], s_buc[2];
    memcpy(f_buc, (void *)buf1, sizeof(Bucket) * 2);
    memcpy(s_buc, (void *)buf2, sizeof(Bucket) * 2);

FRE_READ:
    for (int i = 0; i < SLOT_NUM; i++) // 先搜索f_bucket
    {
        for (int j = 0; j < 2; j++)
        {
            if (FINGER(key, key_len) == GetFinger(f_buc[j].slot[i]) && f_buc[j].slot[i] != NULL) // 这里比较失败！
            {
                uint64_t pair_offset = (uint64_t)GetPair(f_buc[j].slot[i]) - dir_ptr;
                char *buf3 = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
                if (!RDMARead(coro_id, qp, buf1, pair_offset, sizeof(Pair)))
                    return false;
                coro_sched->Yield(yield, coro_id);
                struct Pair kv_pair;
                memcpy(&kv_pair, (void *)buf1, sizeof(Pair));
                if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len)) // 检查value是否符合crc，以免当时在读取时别释放/重新分配
                {
                    strcpy(re_value, kv_pair.value);
                    return true;
                }
            }
        }
    }
    if (f_buc[0].local_depth != GetDepth(f_point)) // 若local depth 对不上，代表 directory 更新了
    {
        if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
        {
            UpdateDir(yield, f_level);
            Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_level, &f_seg_idx, &f_offset, qp);
            goto FRE_READ; // 再去搜一遍新桶
        }
    }

SRE_READ:
    for (int i = 0; i < SLOT_NUM; i++) // 先搜索s_bucket
    {
        for (int j = 0; j < 2; j++)
        {
            if (FINGER(key, key_len) == GetFinger(s_buc[j].slot[i]) && s_buc[j].slot[i] != NULL) // 这里比较失败！
            {
                uint64_t pair_offset = (uint64_t)GetPair(s_buc[j].slot[i]) - dir_ptr;
                char *buf3 = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
                if (!RDMARead(coro_id, qp, buf1, pair_offset, sizeof(Pair)))
                    return false;
                coro_sched->Yield(yield, coro_id);
                struct Pair kv_pair;
                memcpy(&kv_pair, (void *)buf1, sizeof(Pair));
                if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len)) // 检查value是否符合crc，以免当时在读取时别释放/重新分配
                {
                    strcpy(re_value, kv_pair.value);
                    return true;
                }
            }
        }
    }
    if (s_buc[0].local_depth != GetDepth(s_point)) // 若local depth 对不上，代表 directory 更新了
    {
        if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
        {
            UpdateDir(yield, s_level);
            Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_level, &s_seg_idx, &s_offset, qp);
            goto SRE_READ; // 再去搜一遍新桶
        }
    }
    return false;
}

SplitStatus RACE::Split(coro_yield_t &yield, uint64_t suffix, uint64_t point, uint64_t level, uint64_t seg_idx) // only when other cor split dir return fasle
{                                                                                                               // need to promise the dir is new
    // node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    // RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    // lock seg
    char *lock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    uint64_t lock_offset = (uint64_t)GetAddr(point) - seg_ptr + seg_offset + sizeof(Bucket) * BUCKET_NUM; // this time may be stale
    RDMACAS(coro_id, qp, lock_buf, lock_offset, UNLOCK, t_id);                                            // 可以和查看globaldepth合并
    coro_sched->Yield(yield, coro_id);
    if (*((lock_t *)lock_buf) != UNLOCK) // 若获取锁失败，目标值为true，代表别的线程在更新该seg
    {

        do
        {
            RDMARead(coro_id, qp, lock_buf, lock_offset, sizeof(lock_t));
            coro_sched->Yield(yield, coro_id);
        } while (*((lock_t *)lock_buf) != UNLOCK);
        UpdateDir(yield, level); //?here?update?

        return OFFSETUPDATED; // TODO可优化，可以尝试直接split
    }

RE_SPLIT:
    if (GetDepth(point) % SEG_DEPTH == 0) // this time cache_dir old
    {
        // if (GetDepth(point) == 18)
        //     cout << "point" << endl;
        // cout << "Try split: " << seg_num << " t_id: " << t_id << " coro_id: " << coro_id << endl;
        // if (*local_dir_lock != 0) // other cor may used update
        // {
        //     coro_sched->YieldForUpdateDir(yield, coro_id); // yield for other thread
        //     goto RE_SPLIT;
        // }
        // *local_dir_lock = coro_id;
        char *used_buf = thread_rdma_buffer_alloc->Alloc(sizeof(uint64_t));
        uint32_t used = cache_dir->pool_used;
        while (true)
        {
            RDMACAS(coro_id, qp, used_buf, dir_offset, used, used + 1); // get used pool
            coro_sched->Yield(yield, coro_id);
            if (used != *(uint32_t *)used_buf)
            {
                used = *(uint32_t *)used_buf;
            }
            else
            {
                cache_dir->pool_used = used + 1;
                if (cache_dir->pool_used >= MAX_LEVEL)
                {
                    RDMA_LOG(ERROR) << "Exceed Max LEVEL";
                    exit(-1);
                }
                break;
            }
        }
        for (int i = 0; i < SEG_NUM; i++)
        {
            cache_dir->level[used].ls_point[i] = point;
        }
        char *write_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Level));
        memcpy(write_buf, &cache_dir->level[used], sizeof(Level));
        RDMAWrite(coro_id, qp, write_buf, (dir_offset + sizeof(size_t) + sizeof(Level) * used), sizeof(Level)); // write level
        coro_sched->Yield(yield, coro_id);
        char *cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(uint64_t));
        cache_dir->level[level].ls_point[seg_idx] = ((uint64_t)(used) | ls_mask);
        RDMACAS(coro_id, qp, used_buf, dir_offset + sizeof(size_t) + sizeof(Level) * level + seg_idx * LSPointSize, point, cache_dir->level[level].ls_point[seg_idx]); // cas point
        coro_sched->Yield(yield, coro_id);
        // cout << "dir split:" << suffix << "\tcoro_id:" << coro_id << "\tthread_id:" << t_id << endl;
        level = used;
        seg_idx = 0; //?
    }

    // TODO need check??

    // char *check_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    // RDMARead(coro_id, qp, check_buf, dir_offset + sizeof(uint64_t) + MAX_SEG * sizeof(SegUnit), sizeof(lock_t));
    // coro_sched->Yield(yield, coro_id);
    // bool flag = false;
    // while (*(lock_t *)check_buf != UNLOCK) // check dir
    // {
    //     flag = true;
    //     RDMARead(coro_id, qp, check_buf, dir_offset + sizeof(uint64_t) + MAX_SEG * sizeof(SegUnit), sizeof(lock_t));
    //     coro_sched->Yield(yield, coro_id);
    // }
    // if (flag)
    // {
    //     UpdateDir(yield);
    // }

    auto l_depth = GetDepth(point);
    int mod_depth = l_depth % SEG_DEPTH;
    auto act_depth = suffix;                            // 实际旧的对应的段的id
    auto new_locate = act_depth + (int)pow(2, l_depth); // 新段的id
    if (new_locate >= MAX_SEG)
    {
        RDMA_LOG(ERROR) << "Exceed Max Seg Num. L_Depth :" << l_depth << " Act_Depth: " << act_depth;
        exit(-1);
    }

    //  修改旧段的每个桶的local depth //this time not match
    for (int i = 0; i < BUCKET_NUM; i++) // other cor/thread may finish split this time cache dir become stale
    {
        char *lockseg_buf = thread_rdma_buffer_alloc->Alloc(sizeof(uint64_t));
        uint64_t offset = (uint64_t)GetAddr(point) - seg_ptr + seg_offset + i * sizeof(Bucket) + SLOT_NUM * sizeof(Pair *);
        uint64_t _suffix = act_depth;
        uint64_t cmp = l_depth + (_suffix << 32);
#ifdef PASSIVE_ACK
        if (i != BUCKET_NUM - 1)
        {
            coro_sched->RDMACASUnsignal(coro_id, qp, lockseg_buf, offset, cmp, cmp + 1); // depth+1
        }
        else
        {
            RDMACAS(coro_id, qp, lockseg_buf, offset, cmp, cmp + 1); // depth+1
            coro_sched->Yield(yield, coro_id);
        }
#else
        RDMACAS(coro_id, qp, lockseg_buf, offset, cmp, cmp + 1); // depth+1
        coro_sched->Yield(yield, coro_id);
        if (*(uint64_t *)lockseg_buf != cmp) // not occur??
        {
            // if (seg_offset + seg_num * SegSize + sizeof(Bucket) * BUCKET_NUM != lock_offset)
            RDMA_LOG(ERROR) << "Split Error! Now:" << _suffix << " T_id: " << t_id;
            RDMACAS(coro_id, qp, lock_buf, lock_offset, t_id, UNLOCK); // release seg lock
            coro_sched->Yield(yield, coro_id);
            UpdateDir(yield, level, true); // update dir sync

            return ERRORDIR;
        }
#endif
    }

    size_t hash;
    int flag_seg[BUCKET_NUM][SLOT_NUM];
    partial_t p1[BUCKET_NUM][SLOT_NUM] = {0}, p2[BUCKET_NUM][SLOT_NUM] = {0};

    // read segment
    Segment *new_segment = (Segment *)malloc(SegSize);
    char *new_seg_buf = thread_rdma_buffer_alloc->Alloc(SegSize);
#ifdef PARTIAL_SPLIT
    char *partial_buf = thread_rdma_buffer_alloc->Alloc(sizeof(partial_t) * BUCKET_NUM * SLOT_NUM);
    (*partial_size) = (*partial_size) + sizeof(partial_t) * BUCKET_NUM * SLOT_NUM + SegSize;
    std::shared_ptr<ReadCombineBatch> doorbell = std::make_shared<ReadCombineBatch>();
    doorbell->SetReadCombineReq(partial_buf, partial_offset + (act_depth * BUCKET_NUM * SLOT_NUM) * sizeof(partial_t), sizeof(partial_t) * BUCKET_NUM * SLOT_NUM, 0);
    doorbell->SetReadCombineReq(new_seg_buf, (uint64_t)GetAddr(point) - seg_ptr + seg_offset, SegSize, 1);
    doorbell->SendReqs(coro_sched, qp, coro_id);
    coro_sched->Yield(yield, coro_id);
    (*read_size) = (*read_size) + sizeof(partial_t) * BUCKET_NUM * SLOT_NUM + SegSize * 4;
    memcpy(new_segment, new_seg_buf, SegSize);
    memcpy(p1, partial_buf, sizeof(partial_t) * BUCKET_NUM * SLOT_NUM);
#else
    (*full_size) = (*full_size) + SegSize;
    RDMARead(coro_id, qp, new_seg_buf, (uint64_t)GetAddr(point) - seg_ptr + seg_offset, SegSize);
    coro_sched->Yield(yield, coro_id);
    memcpy(new_segment, new_seg_buf, SegSize);
#endif
    // new_segment = (Segment *)new_seg_buf; // 这里可能会被reset，还是重新分配一块

    for (int i = 0; i < BUCKET_NUM; i++)
    {

        new_segment->bucket[i].suffix = new_locate;
        new_segment->bucket[i].local_depth = l_depth + 1; // why? don't need?? TODO
        for (int j = 0; j < SLOT_NUM; j++)
        {
            if (new_segment->bucket[i].slot[j] != NULL)
            {
                (*total_cnt) = (*total_cnt) + 1;
#ifdef PARTIAL_SPLIT
                if (p1[i][j] == NULL) //
                {
                    (*partial_size) = (*partial_size) + sizeof(Pair);
#else
                if (true)
                {
                    (*full_size) = (*full_size) + sizeof(Pair);
#endif
                    char *kv_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
                    RDMARead(coro_id, qp, kv_buf, (uint64_t)GetPair(new_segment->bucket[i].slot[j]) - dir_ptr, sizeof(Pair));
                    coro_sched->Yield(yield, coro_id);
                    hash = CityHash64WithSeed(((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, f_seed);
                    // hash = hash_funcs[0](((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, f_seed);
                    // 第一种hash情况下的seg_idx
                    auto _seg_idx = hash & ((int)pow(2, l_depth + 1) - 1);
                    auto buc_idx = hash >> (64 - BUCKET_SUFFIX);
                    auto b_offset = buc_idx / 2 * 3 + (buc_idx % 2);
                    // 如果第一种hash不对
                    if ((_seg_idx != new_locate && _seg_idx != act_depth) || (b_offset != i && b_offset + 1 != i))
                    {
                        // use cuckoo
                        hash = CityHash64WithSeed(((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, s_seed);
                        // hash = hash_funcs[1](((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, s_seed);
                        _seg_idx = hash & ((int)pow(2, l_depth + 1) - 1);
                    }
                    // 如果该kv应该存在旧段，那么新段对应的slot置空
                    if (_seg_idx != new_locate)
                    {
                        new_segment->bucket[i].slot[j] = NULL;
                        flag_seg[i][j] = 1; // old don't need delete
#ifdef PARTIAL_SPLIT
                        // item insert
                        // char *par_buf = thread_rdma_buffer_alloc->Alloc(sizeof(partial_t)); // TODO Delete
                        // *(uint16_t *)par_buf = (uint16_t)(hash >> (DEFAULT_DEPTH * SEG_DEPTH));
                        // coro_sched->RDMAWriteUnsignal(coro_id, qp, kv_buf, partial_offset + (act_depth * BUCKET_NUM * SLOT_NUM + i * SLOT_NUM + j) * sizeof(partial_t), sizeof(partial_t));
                        // seg insert
                        p1[i][j] = (hash >> (DEFAULT_DEPTH * SEG_DEPTH));
#endif
                    }
                    else
                    {
                        flag_seg[i][j] = 2; // need delete old seg
#ifdef PARTIAL_SPLIT
                        // item insert
                        // char *par_buf = thread_rdma_buffer_alloc->Alloc(sizeof(partial_t)); // TODO Delete
                        // *(uint16_t *)par_buf = (uint16_t)(hash >> (DEFAULT_DEPTH * SEG_DEPTH));
                        // coro_sched->RDMAWriteUnsignal(coro_id, qp, kv_buf, partial_offset + (new_locate * BUCKET_NUM * SLOT_NUM + i * SLOT_NUM + j) * sizeof(partial_t), sizeof(partial_t));
                        // seg insert
                        p2[i][j] = (hash >> (DEFAULT_DEPTH * SEG_DEPTH));
#endif
                    }
                }
                else
                {
                    (*hit_cnt) = (*hit_cnt) + 1;
                    if ((p1[i][j] & (1 << (l_depth - (DEFAULT_DEPTH * SEG_DEPTH)))) == 0) //+1 +1 抵消 in old segment  优先运算==所以要加括号
                    {
                        new_segment->bucket[i].slot[j] = NULL;
                        flag_seg[i][j] = 1; // old don't need delete
                    }
                    else
                    {
                        flag_seg[i][j] = 2;  // need delete old seg
                        p2[i][j] = p1[i][j]; // move delete
                        p1[i][j] = 0;
                    }
                }
            }
            else
            {
                flag_seg[i][j] = 0; // mean NULL
            }
        }
    }

    new_segment->seg_lock = t_id;
    new_seg_buf = thread_rdma_buffer_alloc->Alloc(SegSize); // re alloc re-write
    memcpy(new_seg_buf, new_segment, SegSize);
#ifdef PARTIAL_SPLIT
    char *p1_buf = thread_rdma_buffer_alloc->Alloc(sizeof(partial_t) * BUCKET_NUM * SLOT_NUM);
    char *p2_buf = thread_rdma_buffer_alloc->Alloc(sizeof(partial_t) * BUCKET_NUM * SLOT_NUM);
    memcpy(p1_buf, p1, sizeof(partial_t) * BUCKET_NUM * SLOT_NUM);
    memcpy(p2_buf, p2, sizeof(partial_t) * BUCKET_NUM * SLOT_NUM);
    std::shared_ptr<WriteBatch> writedoor = std::make_shared<WriteBatch>();
    writedoor->SetWriteReq(p1_buf, partial_offset + (act_depth * BUCKET_NUM * SLOT_NUM) * sizeof(partial_t), sizeof(partial_t) * BUCKET_NUM * SLOT_NUM, 0);
    writedoor->SetWriteReq(p2_buf, partial_offset + (new_locate * BUCKET_NUM * SLOT_NUM) * sizeof(partial_t), sizeof(partial_t) * BUCKET_NUM * SLOT_NUM, 1);
    writedoor->SetWriteReq(new_seg_buf, seg_offset + sizeof(Segment) * (new_locate), SegSize, 2);
    writedoor->SendReqs(coro_sched, qp, coro_id);
    coro_sched->Yield(yield, coro_id);
    (*write_size) = (*write_size) + sizeof(partial_t) * BUCKET_NUM * SLOT_NUM + sizeof(partial_t) * BUCKET_NUM * SLOT_NUM + SegSize;
#else
    RDMAWrite(coro_id, qp, new_seg_buf, seg_offset + sizeof(Segment) * (new_locate), SegSize);
    coro_sched->Yield(yield, coro_id);
#endif
    free(new_segment);

    int seg_loc = seg_idx & ((int)pow(2, mod_depth) - 1), check = 0;
    uint64_t stale_point = cache_dir->level[level].ls_point[seg_loc];
// 多个seg指针指向同一个段，对它们进行分离
RE_POINT:
    for (int i = 0; seg_loc + i < SEG_NUM; i += pow(2, mod_depth))
    {
        uint64_t offset = dir_offset + sizeof(uint64_t) + level * sizeof(Level) + (seg_loc + i) * LSPointSize;
        // don't need to check depth as the level may be stale
#ifdef PASSIVE_ACK
        if (check % 2 == 0) // use base is new , level may be stale
        {
            // only modified local_depth
            if (seg_loc + i < SEG_NUM - pow(2, mod_depth))
            {
                coro_sched->RDMACASUnsignal(coro_id, qp, lock_buf, offset, stale_point, stale_point + (1UL << 48)); // depth+1
            }
            else
            {
                RDMACAS(coro_id, qp, lock_buf, offset, stale_point, stale_point + (1UL << 48)); // depth+1
                coro_sched->Yield(yield, coro_id);
            }
            cache_dir->level[level].ls_point[seg_loc + i] = stale_point + (1UL << 48);
        }
        else
        {
            // both change
            uint64_t s = CreateSegUnit((Segment *)((uint64_t)seg_ptr + sizeof(Segment) * (new_locate)), l_depth + 1);
            if (seg_loc + i < SEG_NUM - pow(2, mod_depth))
            {
                coro_sched->RDMACASUnsignal(coro_id, qp, lock_buf, offset, stale_point, s);
            }
            else
            {
                RDMACAS(coro_id, qp, lock_buf, offset, stale_point, s);
                coro_sched->Yield(yield, coro_id);
            }
            cache_dir->level[level].ls_point[seg_loc + i] = s;
        }
#else
        if (check % 2 == 0) // use base is new , level may be stale
        {
            // only modified local_depth
            RDMACAS(coro_id, qp, lock_buf, offset, stale_point, stale_point + (1UL << 48)); // depth+1
            coro_sched->Yield(yield, coro_id);
            cache_dir->level[level].ls_point[seg_loc + i] = stale_point + (1UL << 48);
        }
        else
        {
            // both change
            uint64_t s = CreateSegUnit((Segment *)((uint64_t)seg_ptr + sizeof(Segment) * (new_locate)), l_depth + 1);
            RDMACAS(coro_id, qp, lock_buf, offset, stale_point, s);
            coro_sched->Yield(yield, coro_id);
            cache_dir->level[level].ls_point[seg_loc + i] = s;
        }
#endif
        check++;
    }

    // finally delete
    Segment *old_segment = (Segment *)malloc(SegSize);
    char *slot_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair *));
    char *old_seg_buf = thread_rdma_buffer_alloc->Alloc(SegSize);
    RDMARead(coro_id, qp, old_seg_buf, (uint64_t)GetAddr(point) - seg_ptr + seg_offset, SegSize);
    coro_sched->Yield(yield, coro_id);
    memcpy(old_segment, old_seg_buf, SegSize);
    // old_segment = (Segment *)old_seg_buf;

    for (int i = 0; i < BUCKET_NUM; i++)
    {
        old_segment->bucket[i].local_depth = l_depth + 1;
        for (int j = 0; j < SLOT_NUM; j++)
        {
            if (old_segment->bucket[i].slot[j] != NULL)
            {
                if (flag_seg[i][j] == 2)
                {
                    uint64_t offset = (uint64_t)GetAddr(point) + i * sizeof(Bucket) + j * sizeof(Pair *) - seg_ptr + seg_offset; // BUCKET
                    RDMACAS(coro_id, qp, slot_buf, offset, (uint64_t)old_segment->bucket[i].slot[j], 0x0);
                    coro_sched->Yield(yield, coro_id);
                }
                else if (flag_seg[i][j] == 0)
                {
                    // TODO need add partial key?
                    //  cout << "empty" << seg_num << "\t" << act_depth << endl;
                    uint64_t offset = (uint64_t)GetPair(old_segment->bucket[i].slot[j]) - dir_ptr;
                    char *kv_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
                    RDMARead(coro_id, qp, kv_buf, offset, sizeof(Pair));
                    coro_sched->Yield(yield, coro_id);
                    hash = CityHash64WithSeed(((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, f_seed);
                    // hash = hash_funcs[0](((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, f_seed);
                    auto _seg_idx = hash & ((int)pow(2, l_depth + 1) - 1);
                    auto buc_idx = hash >> (64 - BUCKET_SUFFIX);
                    auto b_offset = buc_idx / 2 * 3 + (buc_idx % 2);
                    if ((_seg_idx != new_locate && _seg_idx != act_depth) || (b_offset != i && b_offset + 1 != i)) //
                    {
                        // use cuckoo
                        hash = CityHash64WithSeed(((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, s_seed);
                        // hash = hash_funcs[1](((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, s_seed);
                        _seg_idx = hash & ((int)pow(2, l_depth + 1) - 1);
                    }
                    if (_seg_idx == new_locate) // todo re_insert?
                    {
                        offset = (uint64_t)GetAddr(point) + i * sizeof(Bucket) + j * sizeof(Pair *) - seg_ptr + seg_offset; // BUCKET
                        RDMACAS(coro_id, qp, slot_buf, offset, (uint64_t)old_segment->bucket[i].slot[j], 0x0);
                        coro_sched->Yield(yield, coro_id);
                    }
                }
            }
        }
    }
    free(old_segment);

    lock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    RDMACAS(coro_id, qp, lock_buf, seg_offset + act_depth * SegSize + sizeof(Bucket) * BUCKET_NUM, t_id, UNLOCK); // 可以和查看globaldepth合并
    coro_sched->Yield(yield, coro_id);
    RDMACAS(coro_id, qp, lock_buf, seg_offset + new_locate * SegSize + sizeof(Bucket) * BUCKET_NUM, t_id, UNLOCK); // 可以和查看globaldepth合并
    coro_sched->Yield(yield, coro_id);

    // if ((seg_num == 7390 || seg_num == 3294) && cache_dir->seg[seg_num] >> 48 == 13)
    //     cout << "test success" << endl;

    return SUCCSPLIT;
}

bool RACE::Update(coro_yield_t &yield, char *key, char *value, uint32_t key_len, uint32_t value_len, size_t used)
{
    bool write_flag = true;
    uint64_t f_hash = CityHash64WithSeed(key, key_len, f_seed);
    uint64_t s_hash = CityHash64WithSeed(key, key_len, s_seed);
    Pair *p; // only one
    Pair kv;
    memcpy(kv.key, key, key_len);
    memcpy(kv.value, value, value_len);
    kv.key_len = key_len;
    kv.value_len = value_len;
    kv.crc = CityHash64(value, value_len); // value_CRC
    uint64_t f_point, s_point, move_bit, f_level, s_level, f_seg_idx, s_seg_idx;
    char *buf1 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    char *buf2 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    char *buf3 = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
    move_bit = 0;
    f_level = 0;
    s_level = 0;
    do
    {
        f_seg_idx = (f_hash >> move_bit) & (SEG_NUM - 1);
        if (cache_dir->level[f_level].ls_point[f_seg_idx] == 0)
        {
            UpdateDir(yield, f_level); // the level is not load
        }
        if ((cache_dir->level[f_level].ls_point[f_seg_idx] & ls_mask) != 0) //
        {
            f_level = cache_dir->level[f_level].ls_point[f_seg_idx] & (ls_mask - 1);
            move_bit += SEG_DEPTH; // move to next seg
        }
        else
        {
            f_point = cache_dir->level[f_level].ls_point[f_seg_idx];
            break;
        }
    } while (true);
    move_bit = 0; // re_set
    do
    {
        s_seg_idx = (s_hash >> move_bit) & (SEG_NUM - 1);
        if (cache_dir->level[s_level].ls_point[s_seg_idx] == 0)
        {
            UpdateDir(yield, s_level); // the level is not load
        }
        if ((cache_dir->level[s_level].ls_point[s_seg_idx] & ls_mask) != 0) //
        {
            s_level = cache_dir->level[s_level].ls_point[s_seg_idx] & (ls_mask - 1);
            move_bit += SEG_DEPTH; // move to next seg
        }
        else
        {
            s_point = cache_dir->level[s_level].ls_point[s_seg_idx];
            break;
        }
    } while (true);

    uint32_t f_buc_idx = f_hash >> (64 - BUCKET_SUFFIX); // 需要额外减1？
    uint32_t s_buc_idx = s_hash >> (64 - BUCKET_SUFFIX);
    // cout << (uint64_t)(f_seg_idx) << "\t" << (uint64_t)(f_buc_idx) << endl;
    auto f_offset = (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)GetAddr(f_point) - seg_ptr + seg_offset;
    auto s_offset = (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)GetAddr(s_point) - seg_ptr + seg_offset;
    memcpy(buf3, &kv, sizeof(Pair));
#ifdef PASSIVE_ACK
    std::shared_ptr<ReadWriteKVBatch> doorbell = std::make_shared<ReadWriteKVBatch>(true);
#else
    std::shared_ptr<ReadWriteKVBatch> doorbell = std::make_shared<ReadWriteKVBatch>();
#endif
    doorbell->SetReadCombineReq(buf1, f_offset, BucketSize * 2, 0);
    doorbell->SetReadCombineReq(buf2, s_offset, BucketSize * 2, 1);
    doorbell->SetWriteKVReq(buf3, used * sizeof(Pair) + kv_offset, sizeof(Pair));
    doorbell->SendReqs(coro_sched, qp, coro_id);
    coro_sched->Yield(yield, coro_id);
    (*read_size) = (*read_size) + BucketSize * 4;

    Bucket f_buc[2], s_buc[2];
    memcpy(f_buc, (void *)buf1, sizeof(Bucket) * 2);
    memcpy(s_buc, (void *)buf2, sizeof(Bucket) * 2);

    p = (Pair *)((uint64_t)kv_ptr + used * sizeof(Pair));
    uint8_t finger = FINGER(key, key_len);
    uint8_t kv_len = key_len + value_len;
    p = CreateSlot(p, kv_len, finger); // create a available slot
    char *cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));

FRE_UPDATE:
    for (int i = 0; i < SLOT_NUM; i++) // 先搜索f_bucket
    {
        for (int j = 0; j < 2; j++)
        {
            if (FINGER(key, key_len) == GetFinger(f_buc[j].slot[i]) && f_buc[j].slot[i] != NULL) // 这里比较失败！
            {
                uint64_t pair_offset = (uint64_t)GetPair(f_buc[j].slot[i]) - dir_ptr;
                if (!RDMARead(coro_id, qp, buf1, pair_offset, sizeof(Pair)))
                    return false;
                coro_sched->Yield(yield, coro_id);
                struct Pair kv_pair;
                memcpy(&kv_pair, (void *)buf1, sizeof(Pair));
                if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len)) // 检查value是否符合crc，以免当时在读取时别释放/重新分配
                {
                     if (f_buc[0].local_depth != GetDepth(f_point))
                    {
                        if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                        {
                            // restart，需要更新directory
                            UpdateDir(yield, false);
                            Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_level, &f_seg_idx, &f_offset, qp);
                            goto FRE_UPDATE;
                        }
                    }

                    RDMACAS(coro_id, qp, cas_buf, f_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)f_buc[j].slot[i], (uint64_t)p);
                    coro_sched->Yield(yield, coro_id);
                    if (*(uint64_t *)cas_buf != (uint64_t)f_buc[j].slot[i]) // cas失败
                    {
                        // 若cas失败原因为split，则更新directory
                        if (f_buc[0].local_depth != GetDepth(f_point))
                        {
                            if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                            {
                                // restart，需要更新directory
                                UpdateDir(yield, false);
                                Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_level, &f_seg_idx, &f_offset, qp);
                                goto FRE_UPDATE;
                            }
                        }
                        // 否则失败原因是竞争
                        Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_level, &f_seg_idx, &f_offset, qp);
                        goto FRE_UPDATE;
                    }
                    return true;
                }
            }
        }
    }

SRE_UPDATE:
    for (int i = 0; i < SLOT_NUM; i++) // 先搜索s_bucket
    {
        for (int j = 0; j < 2; j++)
        {
            if (FINGER(key, key_len) == GetFinger(s_buc[j].slot[i]) && s_buc[j].slot[i] != NULL) // 这里比较失败！
            {
                uint64_t pair_offset = (uint64_t)GetPair(s_buc[j].slot[i]) - dir_ptr;
                if (!RDMARead(coro_id, qp, buf1, pair_offset, sizeof(Pair)))
                    return false;
                coro_sched->Yield(yield, coro_id);
                struct Pair kv_pair;
                memcpy(&kv_pair, (void *)buf1, sizeof(Pair));
                if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len)) // 检查value是否符合crc，以免当时在读取时别释放/重新分配
                {
                    if (s_buc[0].local_depth != GetDepth(s_point))
                    {
                        if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                        {
                            // 重新update
                            UpdateDir(yield, false);
                            Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_level, &s_seg_idx, &s_offset, qp);
                            goto SRE_UPDATE;
                        }
                    }
                    RDMACAS(coro_id, qp, cas_buf, s_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)s_buc[j].slot[i], (uint64_t)p);
                    coro_sched->Yield(yield, coro_id);
                    if (*(uint64_t *)cas_buf != (uint64_t)s_buc[j].slot[i]) // cas失败
                    {
                        // 若cas失败原因为split，则更新directory
                        if (s_buc[0].local_depth != GetDepth(s_point))
                        {
                            if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                            {
                                // restart，需要更新directory
                                UpdateDir(yield, false);
                                Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_level, &s_seg_idx, &s_offset, qp);
                                goto SRE_UPDATE;
                            }
                        }
                        // 否则失败原因是竞争
                        Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_level, &s_seg_idx, &s_offset, qp);
                        goto SRE_UPDATE;
                    }
                    return true;
                }
            }
        }
    }
    return false;
}

bool RACE::Delete(coro_yield_t &yield, char *key, uint32_t key_len)
{
    bool write_flag = true;
    uint64_t f_hash = CityHash64WithSeed(key, key_len, f_seed);
    uint64_t s_hash = CityHash64WithSeed(key, key_len, s_seed);
    uint64_t f_point, s_point, move_bit, f_level, s_level, f_seg_idx, s_seg_idx;
    char *buf1 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    char *buf2 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    char *cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair *));
RE_START:
    move_bit = 0;
    f_level = 0;
    s_level = 0;
    do
    {
        f_seg_idx = (f_hash >> move_bit) & (SEG_NUM - 1);
        if (cache_dir->level[f_level].ls_point[f_seg_idx] == 0)
        {
            UpdateDir(yield, f_level); // the level is not load
        }
        if ((cache_dir->level[f_level].ls_point[f_seg_idx] & ls_mask) != 0) //
        {
            f_level = cache_dir->level[f_level].ls_point[f_seg_idx] & (ls_mask - 1);
            move_bit += SEG_DEPTH; // move to next seg
        }
        else
        {
            f_point = cache_dir->level[f_level].ls_point[f_seg_idx];
            break;
        }
    } while (true);
    move_bit = 0; // re_set
    do
    {
        s_seg_idx = (s_hash >> move_bit) & (SEG_NUM - 1);
        if (cache_dir->level[s_level].ls_point[s_seg_idx] == 0)
        {
            UpdateDir(yield, s_level); // the level is not load
        }
        if ((cache_dir->level[s_level].ls_point[s_seg_idx] & ls_mask) != 0) //
        {
            s_level = cache_dir->level[s_level].ls_point[s_seg_idx] & (ls_mask - 1);
            move_bit += SEG_DEPTH; // move to next seg
        }
        else
        {
            s_point = cache_dir->level[s_level].ls_point[s_seg_idx];
            break;
        }
    } while (true);

    uint32_t f_buc_idx = f_hash >> (64 - BUCKET_SUFFIX); // 需要额外减1？
    uint32_t s_buc_idx = s_hash >> (64 - BUCKET_SUFFIX);
    // cout << (uint64_t)(f_seg_idx) << "\t" << (uint64_t)(f_buc_idx) << endl;
    auto f_offset = (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)GetAddr(f_point) - seg_ptr + seg_offset;
    auto s_offset = (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)GetAddr(s_point) - seg_ptr + seg_offset;
    std::shared_ptr<ReadCombineBatch> doorbell = std::make_shared<ReadCombineBatch>();
    doorbell->SetReadCombineReq(buf1, f_offset, BucketSize * 2, 0);
    doorbell->SetReadCombineReq(buf2, s_offset, BucketSize * 2, 1);
    doorbell->SendReqs(coro_sched, qp, coro_id);
    coro_sched->Yield(yield, coro_id);
    (*read_size) = (*read_size) + BucketSize * 4;

    Bucket f_buc[2], s_buc[2];
    memcpy(f_buc, (void *)buf1, sizeof(Bucket) * 2);
    memcpy(s_buc, (void *)buf2, sizeof(Bucket) * 2);

FRE_DELETE:
    memcpy(f_buc, (void *)buf1, sizeof(Bucket) * 2);

    for (int j = 0; j < 2; j++)
    {
        for (int i = 0; i < SLOT_NUM; i++)
        {
            if (FINGER(key, key_len) == GetFinger(f_buc[j].slot[i]) && f_buc[j].slot[i] != NULL)
            {
                uint64_t pair_offset = (uint64_t)GetPair(f_buc[j].slot[i]) - dir_ptr;
                // 第二个RTT，读取slot的信息
                if (!coro_sched->RDMARead(coro_id, qp, buf1, pair_offset, sizeof(Pair)))
                {
                    return false;
                }
                coro_sched->Yield(yield, coro_id);
                struct Pair kv_pair;
                memcpy(&kv_pair, (void *)buf1, sizeof(Pair));
                if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len)) // 检查value是否符合crc
                {
                    if (f_buc[0].local_depth != GetDepth(f_point))
                    {
                        if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                        {
                            // restart，需要更新directory
                            UpdateDir(yield, false);
                            Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_level, &f_seg_idx, &f_offset, qp);
                            goto FRE_DELETE;
                        }
                    }
                    // cache还未过期，直接cas slot
                    // 第三个RTT，修改slot的指针
                    coro_sched->RDMACAS(coro_id, qp, cas_buf, f_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)f_buc[j].slot[i], NULL);
                    coro_sched->Yield(yield, coro_id);

                    if (*(uint64_t *)cas_buf != (uint64_t)f_buc[j].slot[i]) // cas失败
                    {
                        // 若cas失败原因为split，则更新directory
                        if (f_buc[0].local_depth != GetDepth(f_point))
                        {
                            if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                            {
                                // restart，需要更新directory
                                UpdateDir(yield, false);
                                Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_level, &f_seg_idx, &f_offset, qp);
                                goto FRE_DELETE;
                            }
                        }
                        // 补充：混合负载下还有一种情况就是被更新了
                        // 若不是因为分裂，则要么是其他client已经删除，此时slot为null；
                        // 或是其他client删除完，又插入新的slot，此时slot不为null
                        // 这两种情况都返回false
                        return false;
                    }
                    auto p_offset = ((partial_offset + ((f_buc[0].suffix * BUCKET_NUM) + (f_buc_idx / 2 * 3 + (f_buc_idx % 2) + j) * SLOT_NUM + i) * sizeof(partial_t)) / 8) * 8;
                    partial_t p = (f_hash >> (DEFAULT_DEPTH * SEG_DEPTH));
                    coro_sched->RDMACASUnsignal(coro_id, qp, cas_buf, p_offset, (uint64_t)p, NULL);
                    return true;
                }
            }
        }
    }

SRE_DELETE:
    memcpy(s_buc, (void *)buf2, sizeof(Bucket) * 2);

    for (int j = 0; j < 2; j++)
    {
        for (int i = 0; i < SLOT_NUM; i++)
        {
            if (FINGER(key, key_len) == GetFinger(s_buc[j].slot[i]) && s_buc[j].slot[i] != NULL)
            {
                uint64_t pair_offset = (uint64_t)GetPair(s_buc[j].slot[i]) - dir_ptr;
                // 第二个RTT，读取slot的信息
                if (!coro_sched->RDMARead(coro_id, qp, buf2, pair_offset, sizeof(Pair)))
                    return false;
                coro_sched->Yield(yield, coro_id);
                struct Pair kv_pair;
                memcpy(&kv_pair, (void *)buf2, sizeof(Pair));
                if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len)) // 检查value是否符合crc
                {
                    if (s_buc[0].local_depth != GetDepth(s_point))
                    {
                        if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                        {
                            // restart，需要更新directory
                            UpdateDir(yield, false);
                            Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_level, &s_seg_idx, &s_offset, qp);
                            goto SRE_DELETE;
                        }
                    }
                    // cache还未过期，直接cas slot
                    // 第三个RTT，修改slot的指针
                    coro_sched->RDMACAS(coro_id, qp, cas_buf, s_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)s_buc[j].slot[i], NULL);
                    coro_sched->Yield(yield, coro_id);

                    if (*(uint64_t *)cas_buf != (uint64_t)s_buc[j].slot[i]) // cas失败
                    {
                        // 若cas失败原因为split，则更新directory
                        if (s_buc[0].local_depth != GetDepth(s_point))
                        {
                            if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                            {
                                // restart，需要更新directory
                                UpdateDir(yield, false);
                                Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_level, &s_seg_idx, &s_offset, qp);
                                goto SRE_DELETE;
                            }
                        }
                        // 若不是因为分裂，则要么是其他client已经删除，此时slot为null；
                        // 或是其他client删除完，又插入新的slot，此时slot不为null
                        // 这两种情况都返回false
                        return false;
                    }
                    auto p_offset = (partial_offset + ((s_buc[0].suffix * BUCKET_NUM) + (s_buc_idx / 2 * 3 + (s_buc_idx % 2) + j) * SLOT_NUM + i) * sizeof(partial_t) / 8) * 8;
                    partial_t p = (s_hash >> (DEFAULT_DEPTH * SEG_DEPTH));
                    coro_sched->RDMACASUnsignal(coro_id, qp, cas_buf, p_offset, (uint64_t)p, NULL);
                    return true;
                }
            }
        }
    }
    return false;
}

void RACE::SendSplit(coro_yield_t &yield, uint64_t point, uint64_t level)
{
    // // if (seg_num == 807)
    // //     cout << "seg_num " << (size_t)seg_num << "\tdepth: " << (size_t)depth << endl;
    // node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    // RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    // SegInfo s;
    // s.seg = seg_num;
    // s.depth = depth;
    // uint64_t _;
    // memcpy(&_, &s, sizeof(uint64_t));
    char *buf = thread_rdma_buffer_alloc->Alloc(sizeof(uint64_t));
    do
    {
        RDMACAS(coro_id, qp, buf, split_ptr - dir_ptr, NULL, point); // 可以和查看globaldepth合并
        coro_sched->Yield(yield, coro_id);
    } while (*(size_t *)buf != NULL);
    // split finish
    do
    {
        RDMARead(coro_id, qp, buf, split_ptr - dir_ptr, sizeof(uint64_t)); // 可以和查看globaldepth合并
        coro_sched->Yield(yield, coro_id);
    } while (*(size_t *)buf != NULL);
    UpdateDir(yield, level);
}
