// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <atomic>
#include <unordered_map>

#include "base/common.h"
#include "memstore/hash_store.h"
#include "memstore/hash_index.h"
#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

// const size_t LOG_BUFFER_SIZE = 1024 * 1024 * 512;

struct RemoteNode
{
  node_id_t node_id;
  std::string ip;
  int port;
};

class MetaManager
{
public:
  MetaManager();

  node_id_t GetMemStoreMeta(std::string &remote_ip, int remote_port);

  void GetMRMeta(const RemoteNode &node);

  /*** Memory Store Metadata ***/
  ALWAYS_INLINE
  const IndexMeta &GetPrimaryIndexMetaWithIndexID(const index_id_t index_id) const
  {
    auto search = primary_hash_metas.find(index_id);
    assert(search != primary_hash_metas.end());
    return search->second;
  }

  /*** Node ID Metadata ***/
  ALWAYS_INLINE
  node_id_t GetPrimaryNodeID(const index_id_t index_id) const
  {
    auto search = primary_index_nodes.find(index_id);
    assert(search != primary_index_nodes.end());
    return search->second;
  }

  /*** RDMA Memory Region Metadata ***/
  ALWAYS_INLINE
  const MemoryAttr &GetRemoteHashMR(const node_id_t node_id) const
  {
    auto mrsearch = remote_hash_mrs.find(node_id);
    assert(mrsearch != remote_hash_mrs.end());
    return mrsearch->second;
  }

private:
  std::unordered_map<index_id_t, IndexMeta> primary_hash_metas;

  std::unordered_map<index_id_t, node_id_t> primary_index_nodes;

  std::vector<node_id_t> backup_table_nodes[MAX_DB_TABLE_NUM];

  std::unordered_map<node_id_t, MemoryAttr> remote_hash_mrs;

  std::unordered_map<node_id_t, MemoryAttr> remote_log_mrs;

  node_id_t local_machine_id;

public:
  // Used by QP manager and RDMA Region
  RdmaCtrlPtr global_rdma_ctrl;

  std::vector<RemoteNode> remote_nodes;

  RNicHandler *opened_rnic;

  // Below are some parameteres from json file
  int64_t txn_system;

  // for race
  uint64_t dir_offset;
  uint64_t seg_offset;
  uint64_t kv_offset;
  uint64_t count_offset;
};
