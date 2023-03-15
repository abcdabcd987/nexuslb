#include "bench_dispatcher/fake_frontend.h"

namespace nexus {
namespace dispatcher {

FakeFrontendDelegate::FakeFrontendDelegate(
    std::function<void(size_t cnt_done, size_t workload_idx)> on_request_done,
    uint32_t node_id, ModelSession model_session, size_t workload_idx,
    size_t reserved_size)
    : FrontendDelegate(node_id),
      on_request_done_(std::move(on_request_done)),
      model_session_(std::move(model_session)),
      workload_idx_(workload_idx),
      reserved_size_(reserved_size) {
  if (reserved_size_) {
    queries_.reset(new QueryContext[reserved_size_]);
  }
}

void FakeFrontendDelegate::Tick() {
  // Ignore
}

void FakeFrontendDelegate::UpdateBackendList(BackendListUpdates&& request) {
  // Ignore
}

void FakeFrontendDelegate::ReportRequestDone(size_t cnt_done) {
  on_request_done_(cnt_done, workload_idx_);
}

void FakeFrontendDelegate::MarkQueriesDroppedByDispatcher(
    DispatchReply&& request) {
  for (auto query : request.query_list()) {
    if (!queries_) break;
    auto& qctx = queries_[query.query_id()];
    qctx.status = QueryStatus::kDropped;
    ++cnt_bad_;
  }
  ReportRequestDone(request.query_list_size());
}

void FakeFrontendDelegate::ReceivedQuery(uint64_t query_id,
                                         int64_t frontend_recv_ns) {
  if (!queries_) return;
  auto& qctx = queries_[query_id];
  qctx.status = QueryStatus::kPending;
  qctx.frontend_recv_ns = frontend_recv_ns;
  ++cnt_total_;
}

void FakeFrontendDelegate::GotBatchReply(const BatchPlanProto& plan) {
  for (const auto& query : plan.queries()) {
    if (!queries_) break;
    auto query_id = query.query_without_input().query_id();
    auto& qctx = queries_[query_id];
    auto deadline_ns =
        qctx.frontend_recv_ns + model_session_.latency_sla() * 1000 * 1000;
    if (plan.expected_finish_time_ns() < deadline_ns) {
      qctx.status = QueryStatus::kSuccess;
    } else {
      qctx.status = QueryStatus::kTimeout;
      ++cnt_bad_;
    }
  }
  ReportRequestDone(plan.queries_size());
}

}  // namespace dispatcher
}  // namespace nexus
