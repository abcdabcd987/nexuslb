#ifndef NEXUS_DISPATCHER_BATCH_POLICY_H_
#define NEXUS_DISPATCHER_BATCH_POLICY_H_

#include <deque>
#include <memory>
#include <vector>

#include "nexus/common/model_db.h"
#include "nexus/dispatcher/query_context.h"

namespace nexus {
namespace dispatcher {

class IncrementalBatchPolicy {
 public:
  explicit IncrementalBatchPolicy(SortedQueryList& queries);

  IncrementalBatchPolicy(const IncrementalBatchPolicy& other) = delete;
  IncrementalBatchPolicy& operator=(const IncrementalBatchPolicy& other) =
      delete;
  IncrementalBatchPolicy(IncrementalBatchPolicy&& other) = delete;
  IncrementalBatchPolicy& operator=(IncrementalBatchPolicy&& other) = delete;

  const SortedQueryList& inputs() const { return inputs_; }
  const std::vector<std::shared_ptr<QueryContext>>& drops() const {
    return drops_;
  }
  std::vector<std::shared_ptr<QueryContext>> PopInputs();
  std::vector<std::shared_ptr<QueryContext>> PopDrops();
  void SetProfile(const ModelProfile& profile);
  void Update(TimePoint exec_time, uint32_t target_batch_size);

 private:
  SortedQueryList& queries_;
  const ModelProfile* profile_;
  TimePoint last_exec_time_;
  SortedQueryList inputs_;
  std::vector<std::shared_ptr<QueryContext>> drops_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
