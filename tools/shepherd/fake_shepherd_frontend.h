#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>

#include "shepherd/common.h"

namespace nexus::shepherd {

class FakeShepherdFrontend : public FrontendStub {
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

  FakeShepherdFrontend(int model_id, int slo_ms, size_t workload_idx,
                       size_t reserved_size, int global_id_offset);
  const QueryContext* queries() const { return queries_.get(); }
  size_t reserved_size() const { return reserved_size_; }
  size_t cnt_bad() const { return cnt_bad_; }
  size_t cnt_total() const { return cnt_total_; }

  void MarkQueryDropped(int query_id) override;
  void ReceivedQuery(int query_id, int64_t frontend_recv_ns);
  void GotBatchReply(const BatchPlan& plan);

 private:
  int model_id_;
  int slo_ms_;
  int global_id_offset_;
  size_t workload_idx_;
  size_t reserved_size_;
  std::unique_ptr<QueryContext[]> queries_;
  size_t cnt_bad_ = 0;
  size_t cnt_total_ = 0;
};

}  // namespace nexus::shepherd
