// Author: Ming Zhang
// Copyright (c) 2022

#include "connection/meta_manager.h"

#include "util/json_config.h"

MetaManager::MetaManager()
{
  // Read config json file
  std::string config_filepath = "../../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto local_node = json_config.get("local_compute_node");
  local_machine_id = (node_id_t)local_node.get("machine_id").get_int64();
  txn_system = local_node.get("txn_system").get_int64();
  RDMA_LOG(INFO) << "Run " << (txn_system == 0 ? "FaRM" : (txn_system == 1 ? "DrTM+H" : (txn_system == 2 ? "FORD" : "FORD-LOCAL")));

  auto pm_nodes = json_config.get("remote_pm_nodes");
  auto remote_ips = pm_nodes.get("remote_ips");               // Array
  auto remote_ports = pm_nodes.get("remote_ports");           // Array Used for RDMA exchanges
  auto remote_meta_ports = pm_nodes.get("remote_meta_ports"); // Array Used for transferring datastore metas

  // Get remote machine's memory store meta via TCP
  for (size_t index = 0; index < remote_ips.size(); index++)
  {
    std::string remote_ip = remote_ips.get(index).get_str();
    int remote_meta_port = (int)remote_meta_ports.get(index).get_int64();
    // RDMA_LOG(INFO) << "get hash meta from " << remote_ip;
    node_id_t remote_machine_id = GetMemStoreMeta(remote_ip, remote_meta_port);
    if (remote_machine_id == -1)
    {
      std::cerr << "Thread " << std::this_thread::get_id() << " GetMemStoreMeta() failed!, remote_machine_id = -1" << std::endl;
    }
    int remote_port = (int)remote_ports.get(index).get_int64();
    remote_nodes.push_back(RemoteNode{.node_id = remote_machine_id, .ip = remote_ip, .port = remote_port});
  }
  RDMA_LOG(INFO) << "All hash meta received";

  // RDMA setup
  int local_port = (int)local_node.get("local_port").get_int64();
  global_rdma_ctrl = std::make_shared<RdmaCtrl>(local_machine_id, local_port);

  // Using the first RNIC's first port
  RdmaCtrl::DevIdx idx;
  idx.dev_id = 0;
  idx.port_id = 1;

  // Open device
  opened_rnic = global_rdma_ctrl->open_device(idx);

  for (auto &remote_node : remote_nodes)
  {
    GetMRMeta(remote_node);
  }
  // RDMA_LOG(INFO) << "client: All remote mr meta received!";
}

node_id_t MetaManager::GetMemStoreMeta(std::string &remote_ip, int remote_port)
{
  // Get remote memory store metadata for remote accesses, via TCP
  /* ---------------Initialize socket---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0)
  {
    RDMA_LOG(ERROR) << "MetaManager inet_pton error: " << strerror(errno);
    return -1;
  }
  server_addr.sin_port = htons(remote_port);
  int client_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (client_socket < 0)
  {
    RDMA_LOG(ERROR) << "MetaManager creates socket error: " << strerror(errno);
    close(client_socket);
    return -1;
  }
  if (connect(client_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
  {
    RDMA_LOG(ERROR) << "MetaManager connect error: " << strerror(errno);
    close(client_socket);
    return -1;
  }

  /* --------------- Receiving hash metadata ----------------- */
  size_t hash_meta_size = (size_t)1024 * 1024 * 1024;
  char *recv_buf = (char *)malloc(hash_meta_size);
  auto retlen = recv(client_socket, recv_buf, hash_meta_size, 0);
  if (retlen < 0)
  {
    RDMA_LOG(ERROR) << "MetaManager receives hash meta error: " << strerror(errno);
    free(recv_buf);
    close(client_socket);
    return -1;
  }
  char ack[] = "[ACK]index_meta_received_from_client";
  send(client_socket, ack, strlen(ack) + 1, 0);
  close(client_socket);
  char *snooper = recv_buf;
  // Get number of meta
  size_t primary_meta_num = *((size_t *)snooper);
  snooper += sizeof(primary_meta_num);
  node_id_t remote_machine_id = *((node_id_t *)snooper);
  if (remote_machine_id >= MAX_REMOTE_NODE_NUM)
  {
    RDMA_LOG(FATAL) << "remote machine id " << remote_machine_id << " exceeds the max machine number";
  }
  snooper += sizeof(remote_machine_id);
  // Get the `end of file' indicator: finish transmitting
  char *eof = snooper + sizeof(IndexMeta) * (primary_meta_num);
  if ((*((uint64_t *)eof)) == MEM_STORE_META_END)
  {
    for (size_t i = 0; i < primary_meta_num; i++)
    {
      IndexMeta meta;
      memcpy(&meta, (IndexMeta *)(snooper + i * sizeof(IndexMeta)), sizeof(IndexMeta));
      dir_offset = meta.dir_offset;
      seg_offset = meta.seg_offset;
      kv_offset = meta.kv_offset;
      count_offset = meta.count_ptr - meta.dir_ptr;
      primary_hash_metas[meta.index_id] = meta;
      primary_index_nodes[meta.index_id] = remote_machine_id;
      // RDMA_LOG(INFO) << "primary_node_ip: " << remote_ip << " table id: " << meta.table_id << " data_ptr: 0x" << std::hex << meta.data_ptr << " base_off: 0x" << meta.base_off << " bucket_num: " << std::dec << meta.bucket_num << " node_size: " << meta.node_size << " B";
    }
    snooper += sizeof(IndexMeta) * primary_meta_num;
  }
  else
  {
    free(recv_buf);
    return -1;
  }
  free(recv_buf);
  return remote_machine_id;
}

void MetaManager::GetMRMeta(const RemoteNode &node)
{
  // Get remote node's memory region information via TCP
  MemoryAttr remote_hash_mr{}, remote_log_mr{};

  while (QP::get_remote_mr(node.ip, node.port, SERVER_HASH_BUFF_ID, &remote_hash_mr) != SUCC)
  {
    usleep(2000);
  }
  remote_hash_mrs[node.node_id] = remote_hash_mr;
}