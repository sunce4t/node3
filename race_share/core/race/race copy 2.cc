#include "race/race.h"

#define CAS(_p, _u, _v) (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

bool FALSE = false;
bool TRUE = true;

RACE::RACE(MetaManager *meta_man,
           QPManager *qp_man,
           t_id_t tid,
           coro_id_t coroid,
           CoroutineScheduler *sched,
           RDMABufferAllocator *rdma_buffer_allocator,
           uint64_t thread_kv_offset,
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
    local_dir_lock = local_lock;
    dir_offset = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).dir_offset;
    seg_offset = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).seg_offset;
    kv_offset = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).kv_offset + thread_kv_offset;

    dir_ptr = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).dir_ptr;
    seg_ptr = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).seg_ptr;
    kv_ptr = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).kv_ptr + thread_kv_offset;
    count_ptr = meta_man->GetPrimaryIndexMetaWithIndexID(DEFAULT_ID).count_ptr;
    dir_version = 0;
}


bool RACE::UpdateDir(coro_yield_t &yield, bool sync_flag = false) /// TODO return false then yield
{
    uint64_t cmp = UNLOCK;
    // uint64_t swap = t_id;
    if(split_flag==false){
        split_flag=true;
    }
    bool other_split = false;
#ifdef GLOBAL_CACHE
    // cout << "update dir!" << endl;
    // while(global_dir_lock->load() != 0)
    // 每个线程派一个代表去竞争
    while(*local_dir_lock != 0 && *local_dir_lock != coro_id)
    {
        coro_sched->YieldForUpdateDir(yield, coro_id); // yield for other thread
        other_split = true;
    }
    if(other_split)
        return true;
    *local_dir_lock = coro_id;
    // 每个代表都尝试去获取全局更新锁
    if(CAS(global_dir_lock, &cmp, t_id) == false)
    {
        //获得锁失败的代表，需要等待完成
        while(*global_dir_lock != UNLOCK)
        {
            coro_sched->YieldForUpdateDir(yield, coro_id); // yield for other thread
        }
        return true;
    }
#else
    while (*local_dir_lock != 0) // is locking used for split/udpate
    {
        // cout << *local_dir_lock << endl;
        coro_sched->YieldForUpdateDir(yield, coro_id); // yield for other thread
        other_split = true;
        // return false; // other cor is update/spliting
    }
#endif
#ifdef GLOBAL_CACHE
    ;
#else
    if (other_split)
        return true;
    *local_dir_lock = coro_id;
#endif
    depth_t cur_gd = 0;
RE_UPDATE:
    node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    depth_t before_gd = cache_dir->global_depth;
    if(sync_flag)
        before_gd = cur_gd;
    size_t NowSize = sizeof(uint64_t) + pow(2, before_gd) * SegUnitSize; // get depth_t
    char *data_buf = thread_rdma_buffer_alloc->Alloc(NowSize);

    update_size = update_size + NowSize;
    // cout << update_size << endl;
    update_cnt ++;
    if (!sync_flag)
    {
        coro_sched->RDMARead(coro_id, qp, data_buf, global_meta_man->dir_offset, NowSize);
        coro_sched->Yield(yield, coro_id);
    }
    else
    {
        coro_sched->RDMAReadSync(coro_id, qp, data_buf, global_meta_man->dir_offset, NowSize);
    }
    // split逻辑是先修改本地gd，然后再修改远端gd
    // 因此存在，本地gd已经修改，但远端还未修改，那么读过来的可能为旧值
    if(cache_dir->global_depth <= *(depth_t *)data_buf)
    {
        uint64_t new_gd = *(depth_t *)data_buf;
        // uint64_t cmp;
        // memcpy(cache_dir, data_buf, NowSize);
        uint64_t entry;
        for(int i = 0; i < (int)pow(2, before_gd); i ++)
        {
            entry = *(uint64_t *)(data_buf + (i + 1) * SegUnitSize);
            if(entry != cache_dir->seg[i])
                CAS(&cache_dir->seg[i], &cache_dir->seg[i], entry);
        }
        // if(cache_dir->global_depth != *(depth_t *)data_buf)
        // before_gd = *(depth_t *)data_buf;
        // cur_gd 不等于 0 说明不是第一次进入这里
        if(cache_dir->global_depth != cur_gd && cur_gd != 0)
            CAS(&cache_dir->global_depth, &cache_dir->global_depth, cur_gd);
        cur_gd = new_gd;
        // CAS(&cache_dir->global_depth, &cache_dir->global_depth, *(uint64_t *)data_buf);
        // for debug
        if(cache_dir->global_depth>50)
            cout << "endl" << endl;
        if (cache_dir->global_depth != cur_gd)
        {
            sync_flag = true; // this time dir is error so need sync
            goto RE_UPDATE;
        }
    }
#ifdef GLOBAL_CACHE
    cmp = t_id;
    CAS(global_dir_lock, &cmp, UNLOCK);
    *local_dir_lock = 0;
#else
    *local_dir_lock = 0;
#endif
    return true;
}

bool RACE::UpdateRestDir(coro_yield_t &yield, depth_t now_depth)
{
#ifdef GLOBAL_CACHE
    // cout << "update dir!" << endl;
    // while(global_dir_lock->load() != 0)
    // 每个线程派一个代表去竞争
    uint64_t cmp = UNLOCK;
    bool other_update = false;
    while(*local_dir_lock != 0)
    {
        coro_sched->YieldForUpdateDir(yield, coro_id); // yield for other thread
        other_update = true;
    }
    if(other_update)
        return true;
    *local_dir_lock = coro_id;
    // 每个代表都尝试去获取全局更新锁
    if(CAS(global_dir_lock, &cmp, t_id) == false)
    {
        //获得锁失败的代表，需要等待完成
        while(*global_dir_lock != UNLOCK)
        {
            coro_sched->YieldForUpdateDir(yield, coro_id); // yield for other thread
        }
        return true;
    }
#else
    while (*local_dir_lock != 0) // is locking used for split/udpate
    {
        coro_sched->YieldForUpdateDir(yield, coro_id); // yield for other thread
        // return false; // other cor is update/spliting
    }
    *local_dir_lock = coro_id; // lock it for cor
#endif
    node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    depth_t before_gd = cache_dir->global_depth;
    // 从远端读后半部分的dir
    if (likely(before_gd != now_depth)) // other cor update
    {
        size_t NowSize = (pow(2, now_depth) - pow(2, cache_dir->global_depth)) * sizeof(uint64_t); // get depth_t
        char *data_buf = thread_rdma_buffer_alloc->Alloc(NowSize);
        // cout << "init success" << t_id << " coro_id: " << coro_id << endl;
        coro_sched->RDMARead(coro_id, qp, data_buf, global_meta_man->dir_offset + sizeof(uint64_t) + pow(2, cache_dir->global_depth) * sizeof(uint64_t), NowSize);
        // cout << "send success" << t_id << " coro_id: " << coro_id << endl;
        coro_sched->Yield(yield, coro_id);
        // cout << "update success" << t_id << " coro_id: " << coro_id << endl;
        // memcpy(&(cache_dir->seg[(int)pow(2, cache_dir->global_depth)]), data_buf, NowSize);
        int j = 0;
        for(int i = (1 << cache_dir->global_depth); i < (1 << now_depth); i ++)
        {
            CAS(&cache_dir->seg[i], &cache_dir->seg[i], *(uint64_t *)(data_buf + j * SegUnitSize));
            j ++;
        }
        // for(int i = 0; i < (2 << before_gd))
        CAS(&cache_dir->global_depth, &cache_dir->global_depth, now_depth);
        // cache_dir->global_depth = now_depth;
    }
#ifdef GLOBAL_CACHE
    cmp = t_id;
    CAS(global_dir_lock, &cmp, UNLOCK);
    *local_dir_lock = 0;
    cout << "update rest dir success!" << endl;
#else
    *local_dir_lock = 0;
#endif
    return true;
}

bool RACE::CheckDir(coro_yield_t &yield, t_id_t thread)
{
    while(*global_split_lock != UNLOCK)
    {
        coro_sched->Yield(yield, coro_id);
    }
}

ALWAYS_INLINE
bool RACE::Re_read(coro_yield_t &yield, Bucket *buc, size_t hash, size_t *seg_idx, uint64_t *offset, RCQP *qp) // async  re read bucket
{
    // cout << "re_read:" << *seg_idx << endl;
    *seg_idx = hash & ((int)pow(2, cache_dir->global_depth) - 1);
    uint32_t buc_idx = hash >> (64 - BUCKET_SUFFIX);
    do
    {
        *offset = (buc_idx / 2 * 3 + (buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[*seg_idx])) - seg_ptr + seg_offset;
        char *buf = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
        if (!coro_sched->RDMARead(coro_id, qp, buf, *offset, BucketSize * 2))
            return false;
        coro_sched->Yield(yield, coro_id);
        memcpy(buc, (void *)buf, BucketSize * 2);
    } while (*offset != (buc_idx / 2 * 3 + (buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[*seg_idx])) - seg_ptr + seg_offset);
    return true;
}

bool RACE::Insert(coro_yield_t &yield, char *key, char *value, uint32_t key_len, uint32_t value_len, size_t used)
{
    split_flag = false;
    // get qp
    node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    bool write_flag = true;
    uint64_t f_hash = CityHash64WithSeed(key, key_len, f_seed);
    uint64_t s_hash = CityHash64WithSeed(key, key_len, s_seed);
    Pair *p; // only one
RE_START:
    auto f_seg_idx = f_hash & ((int)pow(2, cache_dir->global_depth) - 1);
    uint64_t f_buc_idx = f_hash >> (64 - BUCKET_SUFFIX); // 需要额外减1？
    uint64_t s_seg_idx = s_hash & ((int)pow(2, cache_dir->global_depth) - 1);
    uint64_t s_buc_idx = s_hash >> (64 - BUCKET_SUFFIX);
    // cout << (uint64_t)(f_seg_idx) << "\t" << (uint64_t)(f_buc_idx) << endl;
    uint64_t f_offset = (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[f_seg_idx])) - seg_ptr + seg_offset;
    uint64_t s_offset = (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[s_seg_idx])) - seg_ptr + seg_offset;
    char *buf1 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    char *buf2 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    if (write_flag)
    {
        write_flag = false; // only insert one kv TODO这里偶尔会出现search fail
        std::shared_ptr<ReadWriteKVBatch> doorbell = std::make_shared<ReadWriteKVBatch>();
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

        // recheck whether cache is error
        if (f_offset != (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[f_seg_idx])) - seg_ptr + seg_offset ||
            s_offset != (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[s_seg_idx])) - seg_ptr + seg_offset)
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
        if (f_offset != (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[f_seg_idx])) - seg_ptr + seg_offset ||
            s_offset != (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[s_seg_idx])) - seg_ptr + seg_offset)
        {
            goto RE_START;
        }
    }
    uint64_t slot_addr = (uint64_t)GetPair(p);
    if(slot_addr < kv_ptr)
         cout << "over!" << endl;
    Bucket f_buc[2], s_buc[2];
    memcpy(f_buc, (void *)buf1, BucketSize * 2);
    memcpy(s_buc, (void *)buf2, BucketSize * 2);
    bool blk_flag = false;
RE_INSERT:

    // old

FRE_INSERT:
    // find and write

    for (int i = 0; i < SLOT_NUM; i++) // 比较suffix开始
    {
        for (int j = 0; j < 2; j++)
        {
            if (f_buc[j].slot[i] == nullptr) // 能不能Batch?
            {
                // CheckDir(yield, t_id);
                if (f_buc[0].local_depth != GetDepth(cache_dir->seg[f_seg_idx])) // 若正在分裂
                // if (f_buc[0].local_depth != GetDepth(cache_dir->seg[f_seg_idx])) // 若正在分裂
                {
                    if(!blk_flag)
                    {
                        blk_cnt[t_id] ++;
                        uint64_t ld = GetDepth(cache_dir->seg[f_seg_idx]);
                        uint64_t seg_idx = f_seg_idx & ((int)pow(2, ld) - 1);
                        while(!blk_lock[ld].try_lock());
                        auto it = insert_blked[ld].find(seg_idx);
                        if(it != insert_blked[ld].end())
                            it->second = it->second + 1;
                        else
                            insert_blked[ld].insert(pair<uint64_t, uint64_t>(seg_idx, 1));
                        blk_flag = true;
                        blk_lock[ld].unlock();
                    }
                    UpdateDir(yield);
                    Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp); // 有没有必要？要不要？
                    goto RE_INSERT;                                           // 再去搜一遍新桶
                }
                char *cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair *));
                coro_sched->RDMACAS(coro_id, qp, cas_buf, f_offset + j * BucketSize + i * sizeof(Pair *), 0x0, (uint64_t)p); // 偏移量要加！！
                coro_sched->Yield(yield, coro_id);
                if (*(uint64_t *)cas_buf == 0x0)
                {
                    uint64_t before_offset = f_offset;
                    uint64_t suffix;
                    Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp); // 有可能读到其他段
                    suffix = (f_hash & ((int)pow(2, f_buc[j].local_depth) - 1));
                    // CheckDir(yield, t_id);
                    if ((f_buc[j].local_depth != GetDepth(cache_dir->seg[f_seg_idx]) && f_buc[j].suffix != suffix) || f_offset != before_offset) // 其他的更新成功后会不会影响这里判断
                    // 分裂12阶段不匹配或者其他协程更新了Dir
                    {
                        coro_sched->RDMACAS(coro_id, qp, cas_buf, f_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)p, 0x0);
                        coro_sched->Yield(yield, coro_id);
                        if (*(uint64_t *)cas_buf == (uint64_t)f_buc[j].slot[i]) // swap成功
                        {
                            do
                            {
                                UpdateDir(yield);
                                Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp);
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
                                    coro_sched->RDMARead(coro_id, qp, read_kv_buf, r_offset, sizeof(Pair));
                                    coro_sched->Yield(yield, coro_id);
                                    Pair kv_pair;
                                    memcpy(&kv_pair, (void *)read_kv_buf, sizeof(Pair));
                                    if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len))
                                    {
                                        // delete duplicate key
                                        coro_sched->RDMACAS(coro_id, qp, cas_buf, f_offset + n * BucketSize + m * sizeof(Pair *), (uint64_t)f_buc[n].slot[m], 0x0);
                                        coro_sched->Yield(yield, coro_id);
                                    }
                                }
                            }
                        }
                    }
                    return true;
                }
                else
                {
                    Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp);
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
                if (s_buc[0].local_depth != GetDepth(cache_dir->seg[s_seg_idx])) // 若正在分裂
                {
                    if(!blk_flag)
                    {
                        blk_cnt[t_id] ++;
                        uint64_t ld = GetDepth(cache_dir->seg[s_seg_idx]);
                        uint64_t seg_idx = f_seg_idx & ((int)pow(2, ld) - 1);
                        while(!blk_lock[ld].try_lock());
                        auto it = insert_blked[ld].find(seg_idx);
                        if(it != insert_blked[ld].end())
                            it->second = it->second + 1;
                        else
                            insert_blked[ld].insert(pair<uint64_t, uint64_t>(seg_idx, 1));
                        blk_flag = true;
                        blk_lock[ld].unlock();
                    }
                    UpdateDir(yield);
                    Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp); // 有没有必要？要不要？
                    goto RE_INSERT;                                           // 再去搜一遍新桶
                }
                char *cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair *));
                coro_sched->RDMACAS(coro_id, qp, cas_buf, s_offset + j * BucketSize + i * sizeof(Pair *), 0x0, (uint64_t)p);
                coro_sched->Yield(yield, coro_id);
                if (*(uint64_t *)cas_buf == 0x0)
                {
                    uint64_t before_offset = s_offset;
                    uint64_t suffix;
                    Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
                    suffix = (s_hash & ((int)pow(2, s_buc[j].local_depth) - 1));
                    
                    if ((s_buc[j].local_depth != GetDepth(cache_dir->seg[s_seg_idx]) && s_buc[j].suffix != suffix) || before_offset != s_offset) // 其他的更新成功后会不会影响这里判断
                    // 分裂12阶段不匹配或者其他协程更新了Dir
                    {
                        coro_sched->RDMACAS(coro_id, qp, cas_buf, s_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)p, 0x0);
                        coro_sched->Yield(yield, coro_id);
                        if (*(uint64_t *)cas_buf == (uint64_t)s_buc[j].slot[i]) // swap成功
                        {
                            do
                            {
                                UpdateDir(yield);
                                Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
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
                                    coro_sched->RDMARead(coro_id, qp, read_kv_buf, r_offset, sizeof(Pair));
                                    coro_sched->Yield(yield, coro_id);
                                    Pair kv_pair;
                                    memcpy(&kv_pair, (void *)read_kv_buf, sizeof(Pair));
                                    if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len))
                                    {
                                        // delete duplicate key
                                        coro_sched->RDMACAS(coro_id, qp, cas_buf, s_offset + n * BucketSize + m * sizeof(Pair *), (uint64_t)s_buc[n].slot[m], 0x0);
                                        coro_sched->Yield(yield, coro_id);
                                    }
                                }
                            }
                        }
                    }
                    return true;
                }
                else
                {
                    Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
                    goto RE_INSERT;
                }
            }
        }
    }

    if (f_buc[0].local_depth < s_buc[0].local_depth)
    {
        if (f_buc[0].local_depth != GetDepth(cache_dir->seg[f_seg_idx])) // 若正在分裂
        {
            if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
            {
                UpdateDir(yield);
                Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp);
                goto RE_INSERT; // 再去搜一遍新桶
            }
        }
        switch (Split(yield, f_seg_idx))
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
        Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp);
        goto RE_INSERT; // 还需要Check吗
    }

    if (s_buc[0].local_depth != GetDepth(cache_dir->seg[s_seg_idx])) // 若正在分裂
    {
        if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
        {
            UpdateDir(yield);
            Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
            goto RE_INSERT; // 再去搜一遍新桶
        }
    }
    switch (Split(yield, s_seg_idx))
    {
    case OFFSETUPDATED:
        break;
    case OTHERCORUSED:
        coro_sched->YieldForSplit(yield, coro_id);
        break;
    case SUCCSPLIT:
        break;
    case ERRORDIR: // re read and re try insert
        break;
    default:
        break;
    }

    Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
    goto RE_INSERT; // 还需要Check吗
    return false;   // not available
}
bool RACE::Search(coro_yield_t &yield, char *key, uint32_t key_len, char *re_value)
{
    // get qp
    node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    std::shared_ptr<ReadCombineBatch> doorbell = std::make_shared<ReadCombineBatch>();
    uint64_t f_hash = CityHash64WithSeed(key, key_len, f_seed);
    uint64_t s_hash = CityHash64WithSeed(key, key_len, s_seed);
    uint64_t f_seg_idx = f_hash & ((int)pow(2, cache_dir->global_depth) - 1);
    uint64_t f_buc_idx = f_hash >> (64 - BUCKET_SUFFIX); // 需要额外减1？
    uint64_t s_seg_idx = s_hash & ((int)pow(2, cache_dir->global_depth) - 1);
    uint64_t s_buc_idx = s_hash >> (64 - BUCKET_SUFFIX);
    uint64_t a = sizeof(Segment);
    uint64_t f_offset = (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[f_seg_idx])) - seg_ptr + seg_offset;
    uint64_t s_offset = (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[s_seg_idx])) - seg_ptr + seg_offset;
    char *buf1 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    char *buf2 = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    doorbell->SetReadCombineReq(buf1, f_offset, BucketSize * 2, 0);
    doorbell->SetReadCombineReq(buf2, s_offset, BucketSize * 2, 1);
    doorbell->SendReqs(coro_sched, qp, coro_id);
    coro_sched->Yield(yield, coro_id);

    Bucket f_buc[2], s_buc[2];
    memcpy(f_buc, (void *)buf1, sizeof(Bucket) * 2);
    memcpy(s_buc, (void *)buf2, sizeof(Bucket) * 2);
    // if ((uint64_t)buf1 % 40 != 0 || (uint64_t)buf2 % 40 != 0)
    //     std::cout << "error" << std::endl;

FRE_READ:
    for (int i = 0; i < SLOT_NUM; i++) // 先搜索f_bucket
    {
        for (int j = 0; j < 2; j++)
        {
            if (FINGER(key, key_len) == GetFinger(f_buc[j].slot[i]) && f_buc[j].slot[i] != NULL) // 这里比较失败！
            {
                uint64_t pair_offset = (uint64_t)GetPair(f_buc[j].slot[i]) - dir_ptr;
                if (!coro_sched->RDMARead(coro_id, qp, buf1, pair_offset, sizeof(Pair)))
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
    if (f_buc[0].local_depth != GetDepth(cache_dir->seg[f_seg_idx])) // 若local depth 对不上，代表 directory 更新了
    {
        if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
        {
            UpdateDir(yield);
            Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp);
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
                if (!coro_sched->RDMARead(coro_id, qp, buf1, pair_offset, sizeof(Pair)))
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
    if (s_buc[0].local_depth != GetDepth(cache_dir->seg[s_seg_idx])) // 若local depth 对不上，代表 directory 更新了
    {
        if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
        {
            UpdateDir(yield);
            Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
            goto SRE_READ; // 再去搜一遍新桶
        }
    }
    // cout << key << " no key!" << endl;
    return false;
}

SplitStatus RACE::Split(coro_yield_t &yield, uint64_t seg_num) // only when other cor split dir return fasle;TODO Yiled if false
{

    struct timespec msr_start_1, msr_end_1;
    struct timespec msr_start_2, msr_end_2;
    clock_gettime(CLOCK_REALTIME, &msr_start_1);
    node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    // lock seg
    char *lock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    uint64_t lock_offset = (uint64_t)GetAddr(cache_dir->seg[seg_num]) - seg_ptr + seg_offset + sizeof(Bucket) * BUCKET_NUM; // this time may be stale
    coro_sched->RDMACAS(coro_id, qp, lock_buf, lock_offset, UNLOCK, t_id);                                                  // 可以和查看globaldepth合并
    coro_sched->Yield(yield, coro_id);
    if (*((lock_t *)lock_buf) != UNLOCK && *((lock_t *)lock_buf) != t_id) // 若获取锁失败，目标值为true，代表别的线程在更新该seg
    {
        // std::cout << "blocking.." << std::endl;
        blk_cnt[t_id] ++;
        uint64_t ld = GetDepth(cache_dir->seg[seg_num]);
        uint64_t seg_idx = seg_num & ((int)pow(2, ld) - 1);
        while(!blk_lock[ld].try_lock());
        auto it = insert_blked[ld].find(seg_idx);
        if(it != insert_blked[ld].end())
            it->second = it->second + 1;
        else
            insert_blked[ld].insert(pair<uint64_t, uint64_t>(seg_idx, 1));
        blk_lock[ld].unlock();
        do
        {
            coro_sched->RDMARead(coro_id, qp, lock_buf, dir_offset + sizeof(uint64_t) + MAX_SEG * sizeof(SegUnit), sizeof(lock_t));
            coro_sched->Yield(yield, coro_id);
        } while (*((lock_t *)lock_buf) != UNLOCK);
        UpdateDir(yield);

        return OFFSETUPDATED; // TODO可优化，可以尝试直接split
    }

    clock_gettime(CLOCK_REALTIME, &msr_end_1);

RE_SPLIT:
    if (GetDepth(cache_dir->seg[seg_num]) == cache_dir->global_depth) // this time cache_dir old
    {
        // cout << "Try split: " << seg_num << " t_id: " << t_id << " coro_id: " << coro_id << endl;
        // 本地的目录正在更新
        if (*local_dir_lock != 0) // other cor may used update
        {
            coro_sched->YieldForUpdateDir(yield, coro_id); // yield for other thread
            goto RE_SPLIT;
        }
        // 采用共享，这里可能有多个线程的协程同时进入，并且对目录进行分裂
        // 应该修改为，一次只有一个线程来分裂dir
        *local_dir_lock = coro_id;
        // 使用CAS尝试获取分裂锁，失败则说明别的线程在分裂目录
        uint64_t cmp = UNLOCK;
        if(CAS(global_split_lock, &cmp, t_id) == false)
        {
            while(*global_split_lock != UNLOCK)
            {
                coro_sched->YieldForUpdateDir(yield, coro_id);
                goto RE_SPLIT;
            }
        }
        // 获取目录锁
        coro_sched->RDMACAS(coro_id, qp, lock_buf, dir_offset + sizeof(uint64_t) + MAX_SEG * sizeof(SegUnit), UNLOCK, t_id); // 可以和查看globaldepth合并
        coro_sched->Yield(yield, coro_id);
        if (*((lock_t *)lock_buf) != UNLOCK) // 若获取锁失败，目标值为true，代表别的线程在更新目录
        {
            do
            {
                coro_sched->RDMARead(coro_id, qp, lock_buf, dir_offset + sizeof(uint64_t) + MAX_SEG * sizeof(SegUnit), sizeof(lock_t));
                coro_sched->Yield(yield, coro_id);
            } while (*((lock_t *)lock_buf) != UNLOCK);
            *local_dir_lock = 0;
            UpdateDir(yield);
            goto RE_SPLIT;
        }

        char *depth_buf = thread_rdma_buffer_alloc->Alloc(sizeof(depth_t));
        coro_sched->RDMAReadSync(coro_id, qp, depth_buf, dir_offset, sizeof(depth_t)); // min hold lock time
        // 如果别人已经分裂完目录，直接更新
        if (*(depth_t *)depth_buf != cache_dir->global_depth)
        {
            cache_dir->global_depth = *(depth_t *)depth_buf;
            *local_dir_lock = 0;
            UpdateDir(yield, true);                                                                                              // update sync
                                                                                                                                 // 会不会造成额外split？
            coro_sched->RDMACAS(coro_id, qp, lock_buf, dir_offset + sizeof(uint64_t) + MAX_SEG * sizeof(SegUnit), t_id, UNLOCK); // release dir lock
            coro_sched->Yield(yield, coro_id);
            coro_sched->RDMACAS(coro_id, qp, lock_buf, lock_offset, t_id, UNLOCK); // release seg lock
            coro_sched->Yield(yield, coro_id);
            return ERRORDIR; // 还需要split吗，可能旧的本来就读错了
        }
        cout << "depth: " << cache_dir->global_depth <<  " dir split:" << seg_num << "\tcoro_id:" << coro_id << "\tthread_id:" << t_id << endl;
        cout << "global lock: " << *global_split_lock << endl;
        // 再次去更新目录，防止别人已经更新了
        *local_dir_lock = 0; // not always loop
        UpdateDir(yield);    // enable latest dir
        *local_dir_lock = coro_id;

        // cache_dir->global_depth += 1;
        // start dir split 后续每个seg指向新的
        // uint64_t cmp = 0;
        for (int i = 0; i < pow(2, cache_dir->global_depth); i++) // other thread split
        {
            // CAS(&cache_dir->seg[i + (int)pow(2, cache_dir->global_depth)], &cmp, cache_dir->seg[i]);
            cache_dir->seg[i + (int)pow(2, cache_dir->global_depth)] = cache_dir->seg[i]; // 将新分裂出来的目录的指针指向之前的段
        }

        // write new seg
        char *dir_buf = thread_rdma_buffer_alloc->Alloc(sizeof(SegUnit) * pow(2, cache_dir->global_depth));
        memcpy(dir_buf, &cache_dir->seg[(int)pow(2, cache_dir->global_depth)], sizeof(SegUnit) * pow(2, cache_dir->global_depth));
        coro_sched->RDMAWrite(coro_id, qp, dir_buf, dir_offset + sizeof(uint64_t) + sizeof(SegUnit) * pow(2, cache_dir->global_depth), sizeof(SegUnit) * pow(2, cache_dir->global_depth));
        coro_sched->Yield(yield, coro_id);

        // cmp = cache_dir->global_depth;
        cache_dir->global_depth ++;

        // cas global depth
        coro_sched->RDMACAS(coro_id, qp, lock_buf, dir_offset, (size_t)cache_dir->global_depth - 1, (size_t)cache_dir->global_depth); 
        coro_sched->Yield(yield, coro_id);
        
        cout << "depth: " << cache_dir->global_depth << " first: " << cache_dir->seg[(int)pow(2, cache_dir->global_depth - 1)] << endl;
        // CAS(&cache_dir->global_depth, &cmp, cache_dir->global_depth + 1);

        coro_sched->RDMACAS(coro_id, qp, lock_buf, dir_offset + sizeof(uint64_t) + MAX_SEG * sizeof(SegUnit), t_id, UNLOCK); // release dir lock
        coro_sched->Yield(yield, coro_id);

        cmp = t_id;
        *local_dir_lock = 0; // release lock
        CAS(global_split_lock, &cmp, UNLOCK);
        cout << "thread: " << t_id <<  " finish dir split, cur lock: " << *global_split_lock << endl;
    }

    clock_gettime(CLOCK_REALTIME, &msr_start_2);

    char *check_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    coro_sched->RDMARead(coro_id, qp, check_buf, dir_offset + sizeof(uint64_t) + MAX_SEG * sizeof(SegUnit), sizeof(lock_t));
    coro_sched->Yield(yield, coro_id);
    bool flag = false;
    // 如果有线程在分裂目录，那么等待目录分裂完毕
    while (*(lock_t *)check_buf != UNLOCK) // check dir
    {
        flag = true;
        coro_sched->RDMARead(coro_id, qp, check_buf, dir_offset + sizeof(uint64_t) + MAX_SEG * sizeof(SegUnit), sizeof(lock_t));
        coro_sched->Yield(yield, coro_id);
    }
    if (flag)
    {
        UpdateDir(yield);
    }

    uint64_t l_depth = GetDepth(cache_dir->seg[seg_num]);
    uint64_t act_depth = seg_num & ((int)pow(2, l_depth) - 1); // 实际旧的对应的段的id
    uint64_t new_locate = act_depth + (int)pow(2, l_depth);    // 新段的id

    // if (seg_num == 2702 && cache_dir->global_depth == 13) //|| seg_num == 1970 || seg_num == 2994 || seg_num == 4018
    //     cout << "point3" << endl;

    //  修改旧段的每个桶的local depth
    for (int i = 0; i < BUCKET_NUM; i++) // other cor/thread may finish split this time cache dir become stale
    {
        char *lockseg_buf = thread_rdma_buffer_alloc->Alloc(sizeof(uint64_t));
        uint64_t offset = (uint64_t)GetAddr(cache_dir->seg[act_depth]) - seg_ptr + seg_offset + i * sizeof(Bucket) + SLOT_NUM * sizeof(Pair *);
        // if (offset > 5216665600) //|| seg_num == 1970 || seg_num == 2994 || seg_num == 4018
        //     cout << "offset error!!" << i << endl;
        uint64_t _suffix = act_depth;
        uint64_t cmp = l_depth + (_suffix << 32);
        coro_sched->RDMACAS(coro_id, qp, lockseg_buf, offset, cmp, cmp + 1); // depth+1
        coro_sched->Yield(yield, coro_id);
        if (*(uint64_t *)lockseg_buf != cmp) // this time c
        {
            // if (seg_offset + seg_num * SegSize + sizeof(Bucket) * BUCKET_NUM != lock_offset)
            // RDMA_LOG(ERROR) << "Split Error! Now:" << *(uint64_t *)lockseg_buf << " Want: " << cmp << " Location: " << i << " Seg_num: " << seg_num << " T_id: " << t_id;
            coro_sched->RDMACAS(coro_id, qp, lock_buf, lock_offset, t_id, UNLOCK); // release seg lock
            coro_sched->Yield(yield, coro_id);
            UpdateDir(yield, true); // update dir sync

            return ERRORDIR;
        }
    }

    Segment *new_segment = (Segment *)malloc(SegSize);
    char *new_seg_buf = thread_rdma_buffer_alloc->Alloc(SegSize);

    coro_sched->RDMARead(coro_id, qp, new_seg_buf, (uint64_t)GetAddr(cache_dir->seg[act_depth]) - seg_ptr + seg_offset, SegSize);
    coro_sched->Yield(yield, coro_id);
    memcpy(new_segment, new_seg_buf, SegSize);
    // new_segment = (Segment *)new_seg_buf; // 这里可能会被reset，还是重新分配一块

    uint64_t hash, seg_idx;

    for (int i = 0; i < BUCKET_NUM; i++)
    {

        new_segment->bucket[i].suffix = new_locate;
        new_segment->bucket[i].local_depth = l_depth + 1;
        for (int j = 0; j < SLOT_NUM; j++)
        {
            if (new_segment->bucket[i].slot[j] != NULL)
            {
                char *kv_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
                coro_sched->RDMARead(coro_id, qp, kv_buf, (uint64_t)GetPair(new_segment->bucket[i].slot[j]) - dir_ptr, sizeof(Pair));
                coro_sched->Yield(yield, coro_id);
                uint64_t hash = CityHash64WithSeed(((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, f_seed);
                // 第一种hash情况下的seg_idx
                uint64_t seg_idx = hash & ((int)pow(2, l_depth + 1) - 1);
                uint64_t buc_idx = hash >> (64 - BUCKET_SUFFIX);
                uint64_t b_offset = buc_idx / 2 * 3 + (buc_idx % 2);
                // 如果第一种hash不对
                if ((seg_idx != new_locate && seg_idx != act_depth) || (b_offset != i && b_offset + 1 != i))
                {
                    // use cuckoo
                    hash = CityHash64WithSeed(((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, s_seed);
                    seg_idx = hash & ((int)pow(2, l_depth + 1) - 1);
                }
                // 如果该kv应该存在旧段，那么新段对应的slot置空
                if (seg_idx != new_locate)
                {
                    new_segment->bucket[i].slot[j] = NULL;
                    flag_seg[i][j] = 1; // old don't need delete
                }
                else
                {
                    flag_seg[i][j] = 2; // need delete
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
    coro_sched->RDMAWrite(coro_id, qp, new_seg_buf, seg_offset + sizeof(Segment) * (new_locate), sizeof(Segment));
    coro_sched->Yield(yield, coro_id);
    free(new_segment);

    int seg_loc = act_depth, check = 0;
    // 多个seg指针指向同一个段，对它们进行分离
    // 共享时存在问题，即如seg从15到16，恰好此时目录从16到17
    // 那么可能由于cache_dir->global_depth更新不及时，使得2^16-2^17-1这部分seg未被更新
RE_POINT:
    // cout << "---split---" << seg_num << endl;
    for (seg_loc; seg_loc < pow(2, cache_dir->global_depth); seg_loc += pow(2, l_depth))
    {
        uint64_t offset = dir_offset + sizeof(uint64_t) + seg_loc * sizeof(uint64_t);
        if (GetDepth(cache_dir->seg[seg_loc]) == l_depth) // if not equal been splitted has been copy or other cor has not beend split
        {
            uint64_t cmp;
            if (check % 2 == 0)
            {
                // only modified local_depth
                coro_sched->RDMACAS(coro_id, qp, lock_buf, offset, cache_dir->seg[seg_loc], cache_dir->seg[seg_loc] + (1UL << 48)); // depth+1
                coro_sched->Yield(yield, coro_id);
                // cache_dir->seg[seg_loc] += (1UL << 48);
                cmp = cache_dir->seg[seg_loc];
                CAS(&cache_dir->seg[seg_loc], &cmp, cache_dir->Seg[seg_loc] + (1UL << 48));
            }
            else
            {
                // both change
                uint64_t s = CreateSegUnit((Segment *)((uint64_t)seg_ptr + sizeof(Segment) * (new_locate)), l_depth + 1);
                coro_sched->RDMACAS(coro_id, qp, lock_buf, offset, cache_dir->seg[seg_loc], s);
                coro_sched->Yield(yield, coro_id);
                // cache_dir->seg[seg_loc] = s;
                cmp = cache_dir->seg[seg_loc];
                CAS(&cache_dir->seg[seg_loc], &cmp, s);
            }
            // cout << "seg: " << seg_loc << " " << (cache_dir->seg[seg_loc] >> 48) << endl;
            check++;
        }
    }
    // cout << endl;
    depth_t now_gd = cache_dir->global_depth;
    // check whether dir is splitting
    do
    {
        coro_sched->RDMARead(coro_id, qp, lock_buf, dir_offset + sizeof(uint64_t) + MAX_SEG * sizeof(SegUnit), sizeof(lock_t)); // 可以和查看globaldepth合并
        coro_sched->Yield(yield, coro_id);                                                                                      // 这里gd可能已经被update
    } while (*(lock_t *)lock_buf != UNLOCK);

    char *depth_buf = thread_rdma_buffer_alloc->Alloc(sizeof(depth_t));
    coro_sched->RDMARead(coro_id, qp, depth_buf, dir_offset, sizeof(depth_t)); // 可以和查看globaldepth合并
    coro_sched->Yield(yield, coro_id);

    // 如果发现本地目录已经更新，那么此时漏掉后半段，需要重新处理指针
    if (cache_dir->global_depth != now_gd) // other cor has updated dir
        goto RE_POINT;
    // 其他人分裂目录，但本地目录还未更新，导致本地cache版本不对
    while (*(depth_t *)depth_buf != cache_dir->global_depth)
    {
        // cout << "start" << t_id << " coro_id: " << coro_id << "seg_num" << seg_num << endl;
        UpdateRestDir(yield, *(depth_t *)depth_buf);
        for (; seg_loc < pow(2, cache_dir->global_depth); seg_loc += pow(2, l_depth))
        {
            uint64_t offset = dir_offset + sizeof(uint64_t) + seg_loc * sizeof(uint64_t);
            if (GetDepth(cache_dir->seg[seg_loc]) == l_depth) // if not equal been splitted has been copy or other thread
            {
                uint64_t cmp;
                if (check % 2 == 0)
                {
                    // only modified local_depth
                    coro_sched->RDMACAS(coro_id, qp, lock_buf, offset, cache_dir->seg[seg_loc], cache_dir->seg[seg_loc] + (1UL << 48)); // depth+1
                    coro_sched->Yield(yield, coro_id);
                    // cache_dir->seg[seg_loc] += (1UL << 48);
                    cmp = cache_dir->seg[seg_loc];
                    CAS(&cache_dir->seg[seg_loc], &cmp, cache_dir->Seg[seg_loc] + (1UL << 48));
                }
                else
                {
                    // both change
                    uint64_t s = CreateSegUnit((Segment *)((uint64_t)seg_ptr + sizeof(Segment) * (new_locate)), l_depth + 1);
                    coro_sched->RDMACAS(coro_id, qp, lock_buf, offset, cache_dir->seg[seg_loc], s);
                    coro_sched->Yield(yield, coro_id);
                    // cache_dir->seg[seg_loc] = s;
                    cmp = cache_dir->seg[seg_loc];
                    CAS(&cache_dir->seg[seg_loc], &cmp, s);
                }
            }
            check++;
        }
    }
    
    // finally delete
    Segment *old_segment = (Segment *)malloc(SegSize);
    char *slot_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair *));
    char *old_seg_buf = thread_rdma_buffer_alloc->Alloc(SegSize);
    coro_sched->RDMARead(coro_id, qp, old_seg_buf, (uint64_t)GetAddr(cache_dir->seg[act_depth]) - seg_ptr + seg_offset, SegSize);
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
                    uint64_t offset = (uint64_t)GetAddr(cache_dir->seg[act_depth]) + i * sizeof(Bucket) + j * sizeof(Pair *) - seg_ptr + seg_offset; // BUCKET
                    coro_sched->RDMACAS(coro_id, qp, slot_buf, offset, (uint64_t)old_segment->bucket[i].slot[j], 0x0);
                    coro_sched->Yield(yield, coro_id);
                }
                else if (flag_seg[i][j] == 0)
                {
                    // cout << "empty" << seg_num << "\t" << act_depth << endl;
                    uint64_t offset = (uint64_t)GetPair(old_segment->bucket[i].slot[j]) - dir_ptr;
                    char *kv_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));
                    coro_sched->RDMARead(coro_id, qp, kv_buf, offset, sizeof(Pair));
                    coro_sched->Yield(yield, coro_id);
                    uint64_t hash = CityHash64WithSeed(((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, f_seed);
                    uint64_t seg_idx = hash & ((int)pow(2, l_depth + 1) - 1);
                    uint64_t buc_idx = hash >> (64 - BUCKET_SUFFIX);
                    uint64_t b_offset = buc_idx / 2 * 3 + (buc_idx % 2);
                    if ((seg_idx != new_locate && seg_idx != act_depth) || (b_offset != i && b_offset + 1 != i)) //
                    {
                        // use cuckoo
                        hash = CityHash64WithSeed(((Pair *)kv_buf)->key, ((Pair *)kv_buf)->key_len, s_seed);
                        seg_idx = hash & ((int)pow(2, l_depth + 1) - 1);
                    }
                    if (seg_idx == new_locate)
                    {
                        offset = (uint64_t)GetAddr(cache_dir->seg[act_depth]) + i * sizeof(Bucket) + j * sizeof(Pair *) - seg_ptr + seg_offset; // BUCKET
                        coro_sched->RDMACAS(coro_id, qp, slot_buf, offset, (uint64_t)old_segment->bucket[i].slot[j], 0x0);
                        coro_sched->Yield(yield, coro_id);
                    }
                }
            }
        }
    }
    free(old_segment);

    lock_buf = thread_rdma_buffer_alloc->Alloc(sizeof(lock_t));
    coro_sched->RDMACAS(coro_id, qp, lock_buf, seg_offset + act_depth * SegSize + sizeof(Bucket) * BUCKET_NUM, t_id, UNLOCK); // 可以和查看globaldepth合并
    coro_sched->Yield(yield, coro_id);
    coro_sched->RDMACAS(coro_id, qp, lock_buf, seg_offset + new_locate * SegSize + sizeof(Bucket) * BUCKET_NUM, t_id, UNLOCK); // 可以和查看globaldepth合并
    coro_sched->Yield(yield, coro_id);

    // if ((seg_num == 7390 || seg_num == 3294) && cache_dir->seg[seg_num] >> 48 == 13)
    //     cout << "test success" << endl;
    clock_gettime(CLOCK_REALTIME, &msr_end_2);
    return SUCCSPLIT;
}

bool RACE::Update(coro_yield_t &yield, char *key, char *value, uint32_t key_len, uint32_t value_len, size_t used)
{
    Pair kv;
    node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    char *write_buffer = thread_rdma_buffer_alloc->Alloc(sizeof(Pair));

    memcpy(kv.key, key, key_len);
    memcpy(kv.value, value, value_len);

    // first RTT write KV
    kv.key_len = key_len;
    kv.value_len = value_len;
    kv.crc = CityHash64(value, value_len); // value_CRC

    // 2nd RTT read bucket
    // std::shared_ptr<ReadWriteKVBatch> doorbell = std::make_shared<ReadWriteKVBatch>();
    std::shared_ptr<ReadWriteKVBatch> doorbell = std::make_shared<ReadWriteKVBatch>();

    uint64_t f_hash = CityHash64WithSeed(key, key_len, f_seed);
    uint64_t s_hash = CityHash64WithSeed(key, key_len, s_seed);
    uint64_t f_seg_idx = f_hash & ((int)pow(2, cache_dir->global_depth) - 1);
    uint64_t f_buc_idx = f_hash >> (64 - BUCKET_SUFFIX); // 需要额外减1？
    uint64_t s_seg_idx = s_hash & ((int)pow(2, cache_dir->global_depth) - 1);
    uint32_t s_buc_idx = s_hash >> (64 - BUCKET_SUFFIX);
    uint64_t a = sizeof(Segment);
    uint64_t f_offset = (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[f_seg_idx])) - dir_ptr;
    uint64_t s_offset = (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[s_seg_idx])) - dir_ptr;
    char *f_buffer = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    char *s_buffer = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    char *cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair *));


    doorbell->SetReadCombineReq(f_buffer, f_offset, BucketSize * 2, 0);
    doorbell->SetReadCombineReq(s_buffer, s_offset, BucketSize * 2, 1);

    // 第一个RTT，写KV同时读bucket
    memcpy(write_buffer, &kv, sizeof(Pair));
    doorbell->SetWriteKVReq(write_buffer, sizeof(Pair) * used + kv_offset, sizeof(Pair));
    doorbell->SendReqs(coro_sched, qp, coro_id);
    coro_sched->Yield(yield, coro_id);

    Bucket f_buc[2], s_buc[2];
    Pair *entry = (Pair *)((uint64_t)kv_ptr + used * sizeof(Pair));
    uint8_t finger = FINGER(key, key_len);
    uint8_t kv_len = key_len + value_len;
    entry = CreateSlot(entry, kv_len, finger); // create a available slot
    memcpy(f_buc, (void *)f_buffer, sizeof(Bucket) * 2);
    memcpy(s_buc, (void *)s_buffer, sizeof(Bucket) * 2);

FRE_UPDATE:


    // 遍历f bucket的每个slot，查看是否对应key的slot
    for (int i = 0; i < SLOT_NUM; i++)
    {
        for (int j = 0; j < 2; j++)
        {
            if (FINGER(key, key_len) == GetFinger(f_buc[j].slot[i]) && f_buc[j].slot[i] != NULL) // 这里比较失败！
            {
                uint64_t pair_offset = (uint64_t)GetPair(f_buc[j].slot[i]) - dir_ptr;
                // 第二个RTT，读取slot的信息
                if (!coro_sched->RDMARead(coro_id, qp, f_buffer, pair_offset, sizeof(Pair)))
                    return false;
                coro_sched->Yield(yield, coro_id);
                struct Pair kv_pair;
                memcpy(&kv_pair, (void *)f_buffer, sizeof(Pair));
                if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len)) // 检查value是否符合crc，以免当时在读取时别释放/重新分配
                {
                    if (f_buc[0].local_depth != GetDepth(cache_dir->seg[f_seg_idx]))
                    {
                        if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                        {
                            // restart，需要更新directory
                            UpdateDir(yield, false);
                            Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp);
                            goto FRE_UPDATE;
                        }
                    }
                    // cache还未过期，直接cas slot
                    // 第三个RTT，修改slot的指针
                    coro_sched->RDMACAS(coro_id, qp, cas_buf, f_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)f_buc[j].slot[i], (uint64_t)entry);
                    coro_sched->Yield(yield, coro_id);

                    if (*(uint64_t *)cas_buf != (uint64_t)f_buc[j].slot[i]) // cas失败
                    {
                        // restart
                        //  重新读bucket
                        // 若cas失败原因为split，则更新directory
                        if (f_buc[0].local_depth != GetDepth(cache_dir->seg[f_seg_idx]))
                        {
                            if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                            {
                                // restart，需要更新directory
                                UpdateDir(yield, false);
                                Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp);
                                goto FRE_UPDATE;
                            }
                        }
                        Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp);
                        goto FRE_UPDATE;
                    }
                    return true;
                }
            }
        }
    }
    // 遍历每个slot，查看是否对应key的slot
SRE_UPDATE:

    for (int i = 0; i < SLOT_NUM; i++)
    {
        for (int j = 0; j < 2; j++)
        {
            if (FINGER(key, key_len) == GetFinger(s_buc[j].slot[i]) && s_buc[j].slot[i] != NULL) // 这里比较失败！
            {
                uint64_t pair_offset = (uint64_t)GetPair(s_buc[j].slot[i]) - dir_ptr;
                if (!coro_sched->RDMARead(coro_id, qp, s_buffer, pair_offset, sizeof(Pair)))
                    return false;
                coro_sched->Yield(yield, coro_id);
                struct Pair kv_pair;
                memcpy(&kv_pair, (void *)s_buffer, sizeof(Pair));
                if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len)) // 检查value是否符合crc，以免当时在读取时别释放/重新分配
                {
                    if (s_buc[0].local_depth != GetDepth(cache_dir->seg[s_seg_idx]))
                    {
                        if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                        {
                            // 重新update
                            UpdateDir(yield, false);
                            Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
                            goto SRE_UPDATE;
                        }
                    }
                    // cache还未过期，直接cas slot
                    
                    coro_sched->RDMACAS(coro_id, qp, cas_buf, s_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)s_buc[j].slot[i], (uint64_t)entry);
                    coro_sched->Yield(yield, coro_id);
                    if (*(uint64_t *)cas_buf != (uint64_t)s_buc[j].slot[i]) // cas失败
                    {
                        // restart
                        //  重新读bucket
                        // 若cas失败原因为split，则更新directory
                        if (s_buc[0].local_depth != GetDepth(cache_dir->seg[s_seg_idx]))
                        {
                            if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                            {
                                // restart，需要更新directory
                                UpdateDir(yield, false);
                                Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
                                goto SRE_UPDATE;
                            }
                        }
                        // 竞争
                        Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
                        goto SRE_UPDATE;
                    }
                    return true;
                }
            }
        }
    }
}


bool RACE::Delete(coro_yield_t &yield, char *key, uint32_t key_len)
{
    node_id_t remote_node_id = global_meta_man->GetPrimaryNodeID(DEFAULT_ID);
    RCQP *qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);

    std::shared_ptr<ReadCombineBatch> doorbell = std::make_shared<ReadCombineBatch>();

    uint64_t f_hash = CityHash64WithSeed(key, key_len, f_seed);
    uint64_t s_hash = CityHash64WithSeed(key, key_len, s_seed);

    uint64_t f_seg_idx = f_hash & ((int)pow(2, cache_dir->global_depth) - 1);
    uint64_t f_buc_idx = f_hash >> (64 - BUCKET_SUFFIX); 

    uint64_t s_seg_idx = s_hash & ((int)pow(2, cache_dir->global_depth) - 1);
    uint64_t s_buc_idx = s_hash >> (64 - BUCKET_SUFFIX);

    uint64_t f_offset = (f_buc_idx / 2 * 3 + (f_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[f_seg_idx])) - dir_ptr;
    uint64_t s_offset = (s_buc_idx / 2 * 3 + (s_buc_idx % 2)) * BucketSize + (uint64_t)(GetAddr(cache_dir->seg[s_seg_idx])) - dir_ptr;

    char *f_buffer = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);
    char *s_buffer = thread_rdma_buffer_alloc->Alloc(BucketSize * 2);

    doorbell->SetReadCombineReq(f_buffer, f_offset, BucketSize * 2, 0);
    doorbell->SetReadCombineReq(s_buffer, s_offset, BucketSize * 2, 1);

    // 第一个RTT，读取两个combined bucket
    doorbell->SendReqs(coro_sched, qp, coro_id);
    coro_sched->Yield(yield, coro_id);

    Bucket f_buc[2], s_buc[2];
    char *cas_buf = thread_rdma_buffer_alloc->Alloc(sizeof(Pair *));
FRE_DELETE:
    memcpy(f_buc, (void *)f_buffer, sizeof(Bucket) * 2);

    for(int j = 0; j < 2; j++)
    {
        for(int i = 0; i < SLOT_NUM; i++)
        {
            if(FINGER(key, key_len) == GetFinger(f_buc[j].slot[i]) && f_buc[j].slot[i] != NULL)
            {
                // 存在bug bucket读过来与远端不一致
                uint64_t pair_offset = (uint64_t)GetPair(f_buc[j].slot[i]) - dir_ptr;
                //第二个RTT，读取slot的信息
                if (!coro_sched->RDMARead(coro_id, qp, f_buffer, pair_offset, sizeof(Pair)))
                {
                    return false;
                }
                coro_sched->Yield(yield, coro_id);
                struct Pair kv_pair;
                memcpy(&kv_pair, (void *)f_buffer, sizeof(Pair));
                if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len)) // 检查value是否符合crc
                {
                    if(f_buc[0].local_depth != GetDepth(cache_dir->seg[f_seg_idx]))
                    {
                        if (f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                        {
                            //restart，需要更新directory
                            UpdateDir(yield, false);
                            Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp);
                            goto FRE_DELETE;
                        }
                    }
                    // cache还未过期，直接cas slot
                    //第三个RTT，修改slot的指针
                    coro_sched->RDMACAS(coro_id, qp, cas_buf, f_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)f_buc[j].slot[i], NULL);
                    coro_sched->Yield(yield, coro_id);


                    if (*(uint64_t *)cas_buf != (uint64_t)f_buc[j].slot[i]) // cas失败
                    {
                        // 重新读bucket
                        Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp);
                        // 若cas失败原因为split，则更新directory
                        if(f_buc[0].local_depth != GetDepth(cache_dir->seg[f_seg_idx]))
                        {
                            if(f_buc[0].suffix != (f_hash & ((int)pow(2, f_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                            {
                                //restart，需要更新directory
                                UpdateDir(yield, false);
                                Re_read(yield, f_buc, f_hash, &f_seg_idx, &f_offset, qp);
                                goto FRE_DELETE;
                            }
                        }
                        //若不是因为分裂，则要么是其他client已经删除，此时slot为null；
                        //或是其他client删除完，又插入新的slot，此时slot不为null
                        //这两种情况都返回false
                        return false;
                    }
                    return true;
                }
            }
        }
    }

SRE_DELETE:
    memcpy(s_buc, (void *)s_buffer, sizeof(Bucket) * 2);

    for(int j = 0; j < 2; j++)
    {
        for(int i = 0; i < SLOT_NUM; i++)
        {
            if(FINGER(key, key_len) == GetFinger(s_buc[j].slot[i]) && s_buc[j].slot[i] != NULL)
            {
                uint64_t pair_offset = (uint64_t)GetPair(s_buc[j].slot[i]) - dir_ptr;
                //第二个RTT，读取slot的信息
                if (!coro_sched->RDMARead(coro_id, qp, s_buffer, pair_offset, sizeof(Pair)))
                    return false;
                coro_sched->Yield(yield, coro_id);
                struct Pair kv_pair;
                memcpy(&kv_pair, (void *)s_buffer, sizeof(Pair));
                if (strncmp(key, kv_pair.key, 16) == 0 && kv_pair.crc == CityHash64(kv_pair.value, kv_pair.value_len)) // 检查value是否符合crc
                {
                    if(s_buc[0].local_depth != GetDepth(cache_dir->seg[s_seg_idx]))
                    {
                        if (s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                        {
                            //restart，需要更新directory
                            UpdateDir(yield, false);
                            Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
                            goto SRE_DELETE;
                        }
                    }
                    // cache还未过期，直接cas slot
                    //第三个RTT，修改slot的指针
                    coro_sched->RDMACAS(coro_id, qp, cas_buf, s_offset + j * BucketSize + i * sizeof(Pair *), (uint64_t)s_buc[j].slot[i], NULL);
                    coro_sched->Yield(yield, coro_id);


                    if (*(uint64_t *)cas_buf != (uint64_t)s_buc[j].slot[i]) // cas失败
                    {
                        // 重新读bucket
                        Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
                        // 若cas失败原因为split，则更新directory
                        if(s_buc[0].local_depth != GetDepth(cache_dir->seg[s_seg_idx]))
                        {
                            if(s_buc[0].suffix != (s_hash & ((int)pow(2, s_buc[0].local_depth) - 1))) // 有效位，若不匹配则重新读取，只有当完全不匹配才重新读取
                            {
                                //restart，需要更新directory
                                UpdateDir(yield, false);
                                Re_read(yield, s_buc, s_hash, &s_seg_idx, &s_offset, qp);
                                goto SRE_DELETE;
                            }
                        }
                        //若不是因为分裂，则要么是其他client已经删除，此时slot为null；
                        //或是其他client删除完，又插入新的slot，此时slot不为null
                        //这两种情况都返回false
                        return false;
                    }
                    return true;
                }
            }
        }
    }
    return false;
}