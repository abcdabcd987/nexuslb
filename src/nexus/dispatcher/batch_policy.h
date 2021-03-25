#ifndef NEXUS_DISPATCHER_BATCH_POLICY_H_
#define NEXUS_DISPATCHER_BATCH_POLICY_H_

#include <deque>
#include <memory>
#include <vector>

#include "nexus/common/model_db.h"
#include "nexus/dispatcher/query_context.h"

namespace nexus {
namespace dispatcher {

class BatchPolicyBySlidingWindowWithFreeLunch {
 public:
  struct BatchResult {
    std::deque<std::shared_ptr<QueryContext>> inputs;
    std::vector<std::shared_ptr<QueryContext>> drops, remains;
  };

  BatchResult Getbatch(TimePoint exec_time, uint32_t target_batch_size,
                       const SortedQueryList& queries,
                       const ModelProfile& profile);
};

class IncrementalBatchPolicy {
 public:
  explicit IncrementalBatchPolicy(SortedQueryList& queries);

  IncrementalBatchPolicy(const IncrementalBatchPolicy& other) = delete;
  IncrementalBatchPolicy& operator=(const IncrementalBatchPolicy& other) =
      delete;
  IncrementalBatchPolicy(IncrementalBatchPolicy&& other) = delete;
  IncrementalBatchPolicy& operator=(IncrementalBatchPolicy&& other) = delete;

  const std::deque<std::shared_ptr<QueryContext>>& inputs() const {
    return inputs_;
  }
  const std::vector<std::shared_ptr<QueryContext>>& drops() const {
    return drops_;
  }
  std::deque<std::shared_ptr<QueryContext>> PopInputs();
  std::vector<std::shared_ptr<QueryContext>> PopDrops();
  void SetProfile(const ModelProfile& profile);
  void Update(TimePoint exec_time, uint32_t target_batch_size);

 private:
  SortedQueryList& queries_;
  const ModelProfile* profile_;
  TimePoint last_exec_time_;
  std::deque<std::shared_ptr<QueryContext>> inputs_;
  std::vector<std::shared_ptr<QueryContext>> drops_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
