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
      uint32_t node_id, ModelSession model_session, size_t workload_idx,
      size_t reserved_size);

  void Tick() override;
  void UpdateBackendList(BackendListUpdates&& request) override;
  void MarkQueriesDroppedByDispatcher(DispatchReply&& request) override;

  void ReportRequestDone(size_t cnt_done);
  void ReceivedQuery(uint64_t query_id, int64_t frontend_recv_ns);
  void GotBatchReply(const BatchPlanProto& plan);

  const ModelSession& model_session() const { return model_session_; }
  const QueryContext* queries() const { return queries_.get(); }
  size_t reserved_size() const { return reserved_size_; }
  size_t cnt_bad() const { return cnt_bad_; }
  size_t cnt_total() const { return cnt_total_; }

 private:
  std::function<void(size_t cnt_done, size_t workload_idx)> on_request_done_;
  ModelSession model_session_;
  size_t workload_idx_;
  size_t reserved_size_;
  std::unique_ptr<QueryContext[]> queries_;
  size_t cnt_bad_ = 0;
  size_t cnt_total_ = 0;
};

}  // namespace dispatcher
}  // namespace nexus
