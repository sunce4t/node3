#include <string>
#include <iostream>

#define SLOT_NUM 7
#define BUCKET_NUM 48 //(128 + 128 / 2) // 加上共享bucket  与BUCKET_SUFFIX同步修改
#define BUCKET_SUFFIX 5
#define SEG_NUM 32
#define SEG_DEPTH 5 // 同时修改
#define LS_BIT 63   // point level or seg
#define KEY_LENGTH 16
#define VALUE_LENGTH 32
// Default Index ID for index only 1 machine
#define DEFAULT_ID 0
#define DEFAULT_DEPTH 3UL // DEPTH层，共seg_num^(depth-1)个
#define LEVEL_DEPTH 6UL   // 与leveldepth无关
#define MAX_KV 300000000
#define MAX_SEG 2097152 // 2^21 // 1048576 // 2^20 // 262144 // 2^18 // 1048576 // 65536 1048576分配有问题
#define MAX_LEVEL 65536
#define MAX_NODE 8
#define MAX_UNIT_NUM 8 // 每个线程的读/写操作注册在网卡地址的最大数量
#define MAX_CLIENT 8
#define MAX_THREAD 32
#define MAX_COR 8
#define f_seed 0xc70697UL
#define s_seed 0xc63548UL // s_seed取多少
#define FINGER(x, y) ((uint8_t)(CityHash32((x), (y)) >> 24))
// #define TEST_LATENCY

using namespace std;

// const uint64_t mask = BUCKET_NUM / 3 * 2 - 1; // 127 与Bucket相关
const uint64_t addr_mask = (1UL << 48) - 1;
const uint64_t ls_mask = 1UL << LS_BIT;
const uint64_t len_mask = (255UL << 48);
const uint64_t depth_mask = (((1UL << 16) - 1) << 48);

enum SplitStatus
{
    SUCCSPLIT = 0,
    OTHERCORUSED = 1,
    OTHERCLIENTUSED = 2,
    OFFSETUPDATED = 3, // means global depth is update
    ERRORDIR = 4,      // means the cache dir is stale don't need do split
};

typedef struct Pair
{
    uint32_t key_len;
    uint32_t value_len;
    char key[KEY_LENGTH];
    char value[VALUE_LENGTH];
    uint64_t crc;
} Pair;

typedef struct Slot
{
    uint8_t Fingerprint;
    uint8_t length;
    uint8_t slot[6];
} Slot;

typedef struct Bucket // 改结构需留意，分裂时直接读取了
{
    Pair *slot[SLOT_NUM]; // 初始化为NULL
    depth_t local_depth;
    char empty[2];   // 占位
    uint32_t suffix; // 有效位   TODO 只支持65536

} Bucket;

typedef struct Segment // 改结构需留意，分裂时直接读取了
{
    Bucket bucket[BUCKET_NUM];
    lock_t seg_lock;
} Segment Aligned8;

typedef struct Level
{
    uint64_t ls_point[SEG_NUM];
} Level;

typedef struct SegUnit
{
    uint8_t segment[6];
    depth_t local_depth; // 高位 64位作为L/S位 1为LEVEL
} SegUnit;

typedef struct Directory
{
    uint32_t pool_used;
    char emtpy[4]; // 占位
    Level level[MAX_LEVEL];
} Directory Aligned8;

typedef struct Level_Pool
{
    Level pool[MAX_LEVEL];
} Level_Pool;

typedef struct TimeCount
{
    size_t ready_signl;
    size_t finish_client_num;
    double client_time[MAX_CLIENT];
} TimeCount;

typedef struct SplitArray
{
    uint64_t Split[MAX_CLIENT * MAX_THREAD * MAX_COR];
} SplitArray;
ALWAYS_INLINE
Segment *GetAddr(uint64_t seg)
{
    Segment *r;
    memcpy(&r, &seg, sizeof(Segment *));
    return (Segment *)((uint64_t)r & addr_mask);
}

ALWAYS_INLINE
uint64_t CreateSegUnit(Segment *seg, depth_t local_depth)
{
    uint64_t r;
    size_t r_depth = local_depth;
    size_t _get = ((uint64_t)seg | (r_depth << 48));
    memcpy(&r, &_get, sizeof(uint64_t)); // need test
    return r;
}

ALWAYS_INLINE
depth_t GetDepth(uint64_t seg)
{
    size_t r;
    memcpy(&r, &seg, sizeof(uint64_t));
    return (depth_t)(r >> 48);
}

ALWAYS_INLINE
Pair *CreateSlot(Pair *pair, uint8_t length, uint8_t finger)
{
    uint64_t _len = ((uint64_t)length << 48);
    uint64_t _finger = ((uint64_t)finger << 56);
    Pair *_pair = (Pair *)((uint64_t)pair | _len | _finger);
    //_mm_crc32_u64();
    return _pair;
}

ALWAYS_INLINE
Pair *GetPair(Pair *pair)
{
    return (Pair *)((uint64_t)pair & addr_mask);
}

ALWAYS_INLINE
uint8_t GetLength(Pair *pair)
{
    return (uint8_t)(((uint64_t)pair & len_mask) >> 48);
}

ALWAYS_INLINE
uint8_t GetFinger(Pair *pair)
{
    return (uint8_t)((uint64_t)pair >> 56);
}

const size_t DirSize = sizeof(Directory);
const size_t SegSize = sizeof(Segment);
const size_t SegUnitSize = sizeof(SegUnit);
const size_t LSPointSize = sizeof(uint64_t);
const size_t BucketSize = sizeof(Bucket);
