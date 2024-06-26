// Author: Ming Zhang
// Copyright (c) 2022

#include "scheduler/corotine_scheduler.h"

#include <cassert>

#include "util/debug.h"

void CoroutineScheduler::PollRegularCompletion()
{
  for (auto it = pending_qps.begin(); it != pending_qps.end();)
  {
    RCQP *qp = *it;
    struct ibv_wc wc;
    auto poll_result = qp->poll_send_completion(wc); // The qp polls its own wc
    if (poll_result == 0)
    {
      it++;
      continue;
    }
    if (unlikely(wc.status != IBV_WC_SUCCESS))
    {
      RDMA_LOG(EMPH) << "Bad completion status: " << wc.status << " with error " << ibv_wc_status_str(wc.status) << " oper: " << wc.opcode << " wr_id(coro_id): " << wc.wr_id << " wc size: " << wc.byte_len << ";@ node " << qp->idx_.node_id;
      if (wc.status != IBV_WC_RETRY_EXC_ERR)
      {
        RDMA_LOG(EMPH) << "completion status != IBV_WC_RETRY_EXC_ERR. abort()";
        abort();
      }
      else
      {
        it++;
        continue;
      }
    }
    auto coro_id = wc.wr_id;
    if (coro_id == 0)
      continue;
    assert(pending_counts[coro_id] > 0);
    pending_counts[coro_id] -= 1;
    if (pending_counts[coro_id] == 0)
    {
      AppendCoroutine(&coro_array[coro_id]);
    }
    it = pending_qps.erase(it);
  }
}

void CoroutineScheduler::PollCompletion()
{
  PollRegularCompletion();
}

bool CoroutineScheduler::CheckLogAck(coro_id_t c_id)
{
  if (pending_log_counts[c_id] == 0)
  {
    return true;
  }
  return pending_log_counts[c_id] == 0;
}