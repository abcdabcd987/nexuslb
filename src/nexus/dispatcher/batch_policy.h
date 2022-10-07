#ifndef NEXUS_DISPATCHER_BATCH_POLICY_H_
#define NEXUS_DISPATCHER_BATCH_POLICY_H_

#include <chrono>
#include <deque>
#include <memory>
#include <vector>

#include "nexus/common/model_db.h"
#include "nexus/common/time_util.h"
#include "nexus/dispatcher/query_context.h"

namespace nexus {
namespace dispatcher {

class IncrementalBatchPolicy {
 public:
  IncrementalBatchPolicy(std::chrono::nanoseconds dctrl,
                         std::chrono::nanoseconds ddata,
                         SortedQueryList& queries);

  IncrementalBatchPolicy(const IncrementalBatchPolicy& other) = delete;
  IncrementalBatchPolicy& operator=(const IncrementalBatchPolicy& other) =
      delete;
  IncrementalBatchPolicy(IncrementalBatchPolicy&& other) = delete;
  IncrementalBatchPolicy& operator=(IncrementalBatchPolicy&& other) = delete;

  uint32_t batch_size() const { return batch_size_; }
  TimePoint deadline() const { return deadline_; }
  const std::vector<std::shared_ptr<QueryContext>>& drops() const {
    return drops_;
  }
  SortedQueryList PopInputs();
  std::vector<std::shared_ptr<QueryContext>> PopDrops();
  void SetProfile(const ModelProfile& profile);
  void Update(TimePoint sched_at, TimePoint gpu_free_at,
              uint32_t target_batch_size);

 private:
  std::chrono::nanoseconds dctrl_;
  std::chrono::nanoseconds ddata_;
  SortedQueryList& queries_;
  const ModelProfile* profile_;
  TimePoint last_sched_at_;
  SortedQueryList prefix_;
  SortedQueryList suffix_;
  uint32_t batch_size_;
  TimePoint deadline_;
  std::vector<std::shared_ptr<QueryContext>> drops_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
