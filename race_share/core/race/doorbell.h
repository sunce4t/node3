
#pragma once

#include "base/common.h"
#include "rlib/rdma_ctrl.hpp"
#include "scheduler/corotine_scheduler.h"

using namespace rdmaio;

// Two RDMA requests are sent to the QP in a doorbelled (or batched) way.
// These requests are executed within one round trip
// Target: improve performance

class DoorbellBatch
{
public:
  DoorbellBatch()
  {
    // The key of doorbell: set the pointer to link two requests
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = IBV_SEND_SIGNALED;
    sr[1].next = NULL;
  }

  struct ibv_send_wr sr[2];

  struct ibv_sge sge[2];

  struct ibv_send_wr *bad_sr;
};

class ReadCombineBatch : public DoorbellBatch
{
public:
  ReadCombineBatch() : DoorbellBatch() {}

  // SetLockReq and SetReadReq are a doorbelled group
  // First lock, then read
  void SetReadCombineReq(char *local_addr, uint64_t remote_off, size_t size, size_t num);

  // Send doorbelled requests to the queue pair
  bool SendReqs(CoroutineScheduler *coro_sched, RCQP *qp, coro_id_t coro_id);

  // Fill the parameters
  bool FillParams(RCQP *qp);
};

class ReadWriteKVBatch
{
public:
  ReadWriteKVBatch()
  {
    // The key of doorbell: set the pointer to link two requests
    sr[0].num_sge = 1;
    sr[0].sg_list = &sge[0];
    sr[0].send_flags = 0;
    sr[0].next = &sr[1];

    sr[1].num_sge = 1;
    sr[1].sg_list = &sge[1];
    sr[1].send_flags = 0;
    sr[1].next = &sr[2];

    sr[2].num_sge = 1;
    sr[2].sg_list = &sge[2];
    sr[2].send_flags = IBV_SEND_SIGNALED;
    sr[2].next = NULL;
  }

  void SetReadCombineReq(char *local_addr, uint64_t remote_off, size_t size, size_t num);

  void SetWriteKVReq(char *local_addr, uint64_t remote_off, size_t size);

  // Send doorbelled requests to the queue pair
  bool SendReqs(CoroutineScheduler *coro_sched, RCQP *qp, coro_id_t coro_id);

private:
  struct ibv_send_wr sr[3];

  struct ibv_sge sge[3];

  struct ibv_send_wr *bad_sr;
};
