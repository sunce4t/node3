#include "race_db.h"
#include <math.h>

void RACE_DB::LoadIndex(node_id_t node_id,
                        node_id_t num_server,
                        MemStoreAllocParam *mem_store_alloc_param,
                        MemStoreReserveParam *mem_store_reserve_param)
{
    index = new HashIndex(DEFAULT_ID,
                          1024,
                          mem_store_alloc_param);
    primary_index_ptrs.push_back(index);
    Directory *dir = (Directory *)(index->GetDirPtr());
    Segment *seg = (Segment *)(index->GetSegPtr());
    dir->global_depth = DEFAULT_DEPTH;
    for (int i = 0; i < pow(2, dir->global_depth); i++)
    {
        for (int j = 0; j < BUCKET_NUM; j++)
        {
            seg[i].bucket[j].local_depth = DEFAULT_DEPTH;
            seg[i].bucket[j].suffix = i;
        }
        seg[i].seg_lock = UNLOCK;
        dir->seg[i] = CreateSegUnit(&seg[i], DEFAULT_DEPTH);
    }
    dir->dir_lock = UNLOCK;
}
