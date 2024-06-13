// Author: Ming Zhang, Lurong Liu
// Copyright (c) 2022

#pragma once

#include <cassert>
#include <cstdint>
#include <set>
#include <vector>

#include "config/table_type.h"
#include "memstore/hash_index.h"
#include "util/fast_random.h"
#include "util/json_config.h"

// YYYY-MM-DD HH:MM:SS This is supposed to be a date/time field from Jan 1st 1900 -
// Dec 31st 2100 with a resolution of 1 second. See TPC-C 5.11.0.
// static const int DATETIME_SIZE = 14;
// Use uint32 for data and time

class RACE_DB
{
public:
    std::string bench_name;

    // Pre-defined constants, which will be modified for tests

    /* Index */
    HashIndex *index = nullptr;

    std::vector<HashIndex *> primary_index_ptrs;

    // For server and client usage: Provide interfaces to servers for loading tables
    RACE_DB()
    {
        bench_name = "SWO";
    }

    ~RACE_DB()
    {
        delete index;
    }

    // For server-side usage
    void LoadIndex(node_id_t node_id,
                   node_id_t num_server,
                   MemStoreAllocParam *mem_store_alloc_param,
                   MemStoreReserveParam *mem_store_reserve_param);
    ALWAYS_INLINE
    std::vector<HashIndex *> GetPrimaryHashStore()
    {
        return primary_index_ptrs;
    }
};

// consistency Requirements
//
// 1. Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
//        W_YTD = sum(D_YTD)
//    for each warehouse defined by (W_ID = D_W_ID).

// 2. Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
//         D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
//     for each district defined by (D_W_ID = O_W_ID = NO_W_ID) and (D_ID = O_D_ID = NO_D_ID).
//     This condition does not apply to the NEW-ORDER table for any districts which have no outstanding new orders
//     (i.e., the numbe r of rows is zero).

// 3. Entries in the NEW-ORDER table must satisfy the relationship:
//         max(NO_O_ID) - min(NO_O_ID) + 1 = [number of rows in the NEW-ORDER table for this district]
//     for each district defined by NO_W_ID and NO_D_ID.
//     This condition does not apply to any districts which have no outstanding new orders (i.e., the number of rows is zero).

// 4. Entries in the ORDER and ORDER-LINE tables must satisfy the relationship:
//         sum(O_OL_CNT) = [number of rows in the ORDER-LINE table for this district]
//     for each district defined by (O_W_ID = OL_W_ID) and (O_D_ID = OL_D_ID).

// 5. For any row in the ORDER table, O_CARRIER_ID is set to a null value if and only if
//     there is a corresponding row in the NEW-ORDER table defined by (O_W_ID, O_D_ID, O_ID) = (NO_W_ID, NO_D_ID, NO_O_ID).

// 6. For any row in the ORDER table, O_OL_CNT must equal the number of rows in the ORDER-LINE table
//     for the corresponding order defined by (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID).

// 7. For any row in the ORDER-LINE table, OL_DELIVERY_D is set to a null date/ time if and only if
//     the corresponding row in the ORDER table defined by
//     (O_W_ID, O_D_ID, O_ID) = (OL_W_ID, OL_D_ID, OL_O_ID) has O_CARRIER_ID set to a null value.

// 8. Entries in the WAREHOUSE and HISTORY tables must satisfy the relationship: W_YTD = sum(H_AMOUNT)
//     for each warehouse defined by (W_ID = H_W_ID). 3.3.2.9

// 9. Entries in the DISTRICT and HISTORY tables must satisfy the relationship: D_YTD = sum(H_AMOUNT)
//     for each district defined by (D_W_ID, D_ID) = (H_W_ID, H_D_ID). 3.3.2.10

// 10. Entries in the CUSTOMER, HISTORY, ORDER, and ORDER-LINE tables must satisfy the relationship:
//     C_BALANCE = sum(OL_AMOUNT) - sum(H_AMOUNT)
//     where: H_AMOUNT is selected by (C_W_ID, C_D_ID, C_ID) = (H_C_W_ID, H_C_D_ID, H_C_ID) and
//     OL_AMOUNT is selected by: (OL_W_ID, OL_D_ID, OL_O_ID) = (O_W_ID, O_D_ID, O_ID) and
//     (O_W_ID, O_D_ID, O_C_ID) = (C_W_ID, C_D_ID, C_ID) and (OL_DELIVERY_D is not a null value)

// 11. Entries in the CUSTOMER, ORDER and NEW-ORDER tables must satisfy the relationship:
//     (count(*) from ORDER) - (count(*) from NEW-ORDER) = 2100
//     for each district defined by (O_W_ID, O_D_ID) = (NO_W_ID, NO_D_ID) = (C_W_ID, C_D_ID). 3.3.2.12

// 12. Entries in the CUSTOMER and ORDER-LINE tables must satisfy the relationship:
//     C_BALANCE + C_YTD_PAYMENT = sum(OL_AMOUNT)
//     for any randomly selected customers and where OL_DELIVERY_D is not set to a null date/ time.