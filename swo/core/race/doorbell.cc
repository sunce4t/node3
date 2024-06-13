// Author: Ming Zhang
// Copyright (c) 2022

#include "race/doorbell.h"

void ReadCombineBatch::SetReadCombineReq(char *local_addr, uint64_t remote_off, size_t size, size_t num)
{
  sr[num].opcode = IBV_WR_RDMA_READ;
  sr[num].wr.rdma.remote_addr = remote_off;
  sge[num].length = size;
  sge[num].addr = (uint64_t)local_addr;
}

bool ReadCombineBatch::SendReqs(CoroutineScheduler *coro_sched, RCQP *qp, coro_id_t coro_id)
{
  sr[0].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[0].wr.rdma.rkey = qp->remote_mr_.key;
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1))
    return false;
  return true;
}

void WriteBatch::SetWriteReq(char *local_addr, uint64_t remote_off, size_t size, size_t num)
{
  sr[num].opcode = IBV_WR_RDMA_WRITE;
  sr[num].wr.rdma.remote_addr = remote_off;
  sge[num].length = size;
  sge[num].addr = (uint64_t)local_addr;
}

bool WriteBatch::SendReqs(CoroutineScheduler *coro_sched, RCQP *qp, coro_id_t coro_id)
{
  sr[0].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[0].wr.rdma.rkey = qp->remote_mr_.key;
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;

  sr[2].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[2].wr.rdma.rkey = qp->remote_mr_.key;
  sge[2].lkey = qp->local_mr_.key;

  if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 2)) // doorbell_num last
    return false;
  return true;
}

void ReadWriteKVBatch::SetReadCombineReq(char *local_addr, uint64_t remote_off, size_t size, size_t num)
{
  sr[num].opcode = IBV_WR_RDMA_READ;
  sr[num].wr.rdma.remote_addr = remote_off;
  sge[num].length = size;
  sge[num].addr = (uint64_t)local_addr;
}
void ReadWriteKVBatch::SetWriteKVReq(char *local_addr, uint64_t remote_off, size_t size)
{
  sr[2].opcode = IBV_WR_RDMA_WRITE;
  sr[2].wr.rdma.remote_addr = remote_off;
  sge[2].length = size;
  sge[2].addr = (uint64_t)local_addr;
  if (size < 64)
  {
    sr[2].send_flags |= IBV_SEND_INLINE;
  }
}

bool ReadWriteKVBatch::SendReqs(CoroutineScheduler *coro_sched, RCQP *qp, coro_id_t coro_id)
{
  sr[0].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[0].wr.rdma.rkey = qp->remote_mr_.key;
  sge[0].lkey = qp->local_mr_.key;

  sr[1].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[1].wr.rdma.rkey = qp->remote_mr_.key;
  sge[1].lkey = qp->local_mr_.key;

  sr[2].wr.rdma.remote_addr += qp->remote_mr_.buf;
  sr[2].wr.rdma.rkey = qp->remote_mr_.key;
  sge[2].lkey = qp->local_mr_.key;

  if (is_passive)
  {
    if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 1)) // doorbell_num last
      return false;
  }
  else
  {
    if (!coro_sched->RDMABatch(coro_id, qp, &(sr[0]), &bad_sr, 2)) // doorbell_num last
      return false;
  }
  return true;
}