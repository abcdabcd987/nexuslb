#include "bench_dispatcher/fake_frontend.h"

namespace nexus {
namespace dispatcher {

FakeFrontendDelegate::FakeFrontendDelegate(
    std::function<void(size_t cnt_done, size_t workload_idx)> on_request_done,
    uint32_t node_id, ModelSession model_session, size_t workload_idx)
    : FrontendDelegate(node_id),
      on_request_done_(std::move(on_request_done)),
      model_session_(std::move(model_session)),
      workload_idx_(workload_idx) {}

void FakeFrontendDelegate::Tick() {
  // Ignore
}

void FakeFrontendDelegate::UpdateBackendList(BackendListUpdates&& request) {
  // Ignore
}

void FakeFrontendDelegate::ReportRequestDone(size_t cnt_done) {
  on_request_done_(cnt_done, workload_idx_);
}

void FakeFrontendDelegate::MarkQueryDroppedByDispatcher(
    DispatchReply&& request) {
  auto& qctx = queries_[request.query_id()];
  qctx.status = QueryStatus::kDropped;
  ReportRequestDone(1);
}

void FakeFrontendDelegate::Reserve(size_t max_queries) {
  reserved_size_ = max_queries;
  queries_.reset(new QueryContext[max_queries]);
}

void FakeFrontendDelegate::ReceivedQuery(uint64_t query_id,
                                         int64_t frontend_recv_ns) {
  auto& qctx = queries_[query_id];
  qctx.status = QueryStatus::kPending;
  qctx.frontend_recv_ns = frontend_recv_ns;
}

void FakeFrontendDelegate::GotBatchReply(const BatchPlanProto& plan) {
  for (const auto& query : plan.queries()) {
    auto query_id = query.query_without_input().query_id();
    auto& qctx = queries_[query_id];
    auto deadline_ns =
        qctx.frontend_recv_ns + model_session_.latency_sla() * 1000 * 1000;
    if (plan.expected_finish_time_ns() < deadline_ns) {
      qctx.status = QueryStatus::kSuccess;
    } else {
      qctx.status = QueryStatus::kTimeout;
    }
  }
  ReportRequestDone(plan.queries_size());
}

}  // namespace dispatcher
}  // namespace nexus
