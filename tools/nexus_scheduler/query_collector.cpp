#include "nexus_scheduler/query_collector.h"

#include <glog/logging.h>

namespace nexus {

void QueryCollector::AddModel(size_t reserved_size, long slo_ns) {
  model_ctx_.push_back(std::make_unique<ModelContext>());
  auto& mctx = model_ctx_.back();
  mctx->reserved_size = reserved_size;
  mctx->slo_ns = slo_ns;
  mctx->queries.reset(new QueryContext[reserved_size]);
}

void QueryCollector::ReceivedQuery(size_t model_idx, uint64_t query_id,
                                   int64_t frontend_recv_ns) {
  auto& mctx = model_ctx_.at(model_idx);
  CHECK_LT(query_id, mctx->reserved_size);
  auto& qctx = mctx->queries[query_id];
  qctx.status = QueryStatus::kPending;
  qctx.frontend_recv_ns = frontend_recv_ns;
}

void QueryCollector::GotDroppedReply(size_t model_idx, uint64_t query_id) {
  auto& mctx = model_ctx_.at(model_idx);
  CHECK_LT(query_id, mctx->reserved_size);
  auto& qctx = mctx->queries[query_id];
  qctx.status = QueryStatus::kDropped;
}

void QueryCollector::GotSuccessReply(size_t model_idx, uint64_t query_id,
                                     int64_t finish_ns) {
  auto& mctx = model_ctx_.at(model_idx);
  CHECK_LT(query_id, mctx->reserved_size);
  auto& qctx = mctx->queries[query_id];
  auto deadline_ns = qctx.frontend_recv_ns + mctx->slo_ns;
  if (finish_ns < deadline_ns) {
    qctx.status = QueryStatus::kSuccess;
  } else {
    qctx.status = QueryStatus::kTimeout;
  }
}

}  // namespace nexus
