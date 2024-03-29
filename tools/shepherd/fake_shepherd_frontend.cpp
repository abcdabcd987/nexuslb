#include "shepherd/fake_shepherd_frontend.h"

#include <glog/logging.h>

namespace nexus::shepherd {

FakeShepherdFrontend::FakeShepherdFrontend(int model_id, int slo_ms,
                                           size_t workload_idx,
                                           size_t reserved_size,
                                           int global_id_offset)
    : model_id_(model_id),
      slo_ms_(slo_ms),
      workload_idx_(workload_idx),
      reserved_size_(reserved_size),
      global_id_offset_(global_id_offset) {
  queries_.reset(new QueryContext[reserved_size_]);
}

void FakeShepherdFrontend::MarkQueryDropped(int query_id) {
  auto& qctx = queries_[query_id - global_id_offset_];
  qctx.status = QueryStatus::kDropped;
  ++cnt_bad_;
}

void FakeShepherdFrontend::ReceivedQuery(int query_id,
                                         int64_t frontend_recv_ns) {
  auto& qctx = queries_[query_id - global_id_offset_];
  qctx.status = QueryStatus::kPending;
  qctx.frontend_recv_ns = frontend_recv_ns;
  ++cnt_total_;
}

void FakeShepherdFrontend::GotBatchReply(const BatchPlan& plan) {
  for (auto query_id : plan.query_ids) {
    auto& qctx = queries_[query_id - global_id_offset_];
    auto deadline_ns = qctx.frontend_recv_ns + slo_ms_ * 1000 * 1000;
    if (plan.finish_at.time_since_epoch().count() < deadline_ns) {
      qctx.status = QueryStatus::kSuccess;
    } else {
      VLOG(0) << "Query timeout query_id=" << query_id << " difference_nanos="
              << plan.finish_at.time_since_epoch().count() - deadline_ns;
      qctx.status = QueryStatus::kTimeout;
      ++cnt_bad_;
    }
  }
}

}  // namespace nexus::shepherd
