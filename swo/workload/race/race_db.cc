#include "race_db.h"
#include <math.h>

ALWAYS_INLINE
size_t ConvertLocate(size_t now, size_t depth)
{
    size_t r = 0, move_bit = 0;
    while (depth != 0) // use i (depth -1)
    {
        r += ((now >> move_bit) & (SEG_NUM - 1)) * pow(SEG_NUM, depth - 1);
        depth--;
        move_bit += SEG_DEPTH;
    }
    return r;
}

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
    for (int i = 0; i < pow(SEG_NUM, DEFAULT_DEPTH); i++)
    {
        for (int j = 0; j < BUCKET_NUM; j++)
        {
            seg[i].bucket[j].local_depth = DEFAULT_DEPTH * SEG_DEPTH;
            seg[i].bucket[j].suffix = i;
        }
        seg[i].seg_lock = UNLOCK;
    }
    for (int i = 0; i < DEFAULT_DEPTH - 1; i++)
    {
        for (int j = 0; j < pow(SEG_NUM, i); j++)
        {
            for (int k = 0; k < SEG_NUM; k++)
            {
                dir->level[dir->pool_used].ls_point[k] = ((uint64_t)((pow(SEG_NUM, i + 1) - 1) / (SEG_NUM - 1)) + j * SEG_NUM + k) | ls_mask; // BASE+NEXT_USED+NOW_LOCATE //JUST SET SEG
            }
            dir->pool_used++;
        }
    }
    size_t base = dir->pool_used;
    for (int j = 0; j < pow(SEG_NUM, DEFAULT_DEPTH - 1); j++)
    {
        size_t now_locate = ConvertLocate(j, DEFAULT_DEPTH - 1);
        for (int k = 0; k < SEG_NUM; k++)
        {
            dir->level[dir->pool_used].ls_point[k] = CreateSegUnit(&seg[now_locate + (int)pow(SEG_NUM, DEFAULT_DEPTH - 1) * k], DEFAULT_DEPTH * SEG_DEPTH); // DEFAULT_DEPTH-1
        }
        dir->pool_used++;
    }
    // dir->dir_lock = UNLOCK;
}
