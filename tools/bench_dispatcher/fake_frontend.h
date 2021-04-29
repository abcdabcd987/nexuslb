#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "nexus/dispatcher/frontend_delegate.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class FakeFrontendDelegate : public FrontendDelegate {
 public:
  enum class QueryStatus {
    kPending,
    kDropped,
    kTimeout,
    kSuccess,
  };

  struct QueryContext {
    QueryStatus status;
    int64_t frontend_recv_ns;
  };

  FakeFrontendDelegate(
      std::function<void(size_t cnt_done, size_t workload_idx)> on_request_done,
      uint32_t node_id, ModelSession model_session, size_t workload_idx);

  void Tick() override;
  void UpdateBackendList(BackendListUpdates&& request) override;
  void MarkQueryDroppedByDispatcher(DispatchReply&& request) override;

  void ReportRequestDone(size_t cnt_done);
  void Reserve(size_t max_queries);
  void ReceivedQuery(uint64_t query_id, int64_t frontend_recv_ns);
  void GotBatchReply(const BatchPlanProto& plan);

  const ModelSession& model_session() const { return model_session_; }
  const QueryContext* queries() const { return queries_.get(); }

 private:
  std::function<void(size_t cnt_done, size_t workload_idx)> on_request_done_;
  ModelSession model_session_;
  size_t workload_idx_;
  size_t reserved_size_ = 0;
  std::unique_ptr<QueryContext[]> queries_;
};

}  // namespace dispatcher
}  // namespace nexus
