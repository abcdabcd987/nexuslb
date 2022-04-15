#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace nexus {

class QueryCollector {
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

  const QueryContext* queries(size_t model_idx) const {
    return model_ctx_.at(model_idx)->queries.get();
  }
  size_t reserved_size(size_t model_idx) const {
    return model_ctx_.at(model_idx)->reserved_size;
  }
  std::atomic<uint64_t>* last_query_id(size_t model_idx) const {
    return &model_ctx_.at(model_idx)->last_query_id;
  }

  void AddModel(size_t reserved_size, long slo_ns);
  void ReceivedQuery(size_t model_idx, uint64_t query_id,
                     int64_t frontend_recv_ns);
  void GotDroppedReply(size_t model_idx, uint64_t query_id);
  void GotSuccessReply(size_t model_idx, uint64_t query_id, int64_t finish_ns);

 private:
  struct ModelContext {
    size_t reserved_size;
    long slo_ns;
    std::atomic<uint64_t> last_query_id = 0;
    std::unique_ptr<QueryContext[]> queries;
  };

  std::vector<std::unique_ptr<ModelContext>> model_ctx_;
};

}  // namespace nexus
