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
  IncrementalBatchPolicy(SortedQueryList& queries,
                         std::chrono::duration<long, std::nano> dctrl,
                         std::chrono::duration<long, std::nano> ddata);

  IncrementalBatchPolicy(const IncrementalBatchPolicy& other) = delete;
  IncrementalBatchPolicy& operator=(const IncrementalBatchPolicy& other) =
      delete;
  IncrementalBatchPolicy(IncrementalBatchPolicy&& other) = delete;
  IncrementalBatchPolicy& operator=(IncrementalBatchPolicy&& other) = delete;

  const SortedQueryList& inputs() const { return inputs_; }
  const std::vector<std::shared_ptr<QueryContext>>& drops() const {
    return drops_;
  }
  SortedQueryList PopInputs();
  std::vector<std::shared_ptr<QueryContext>> PopDrops();
  void SetProfile(const ModelProfile& profile);
  void Update(TimePoint sched_at, TimePoint gpu_free_at,
              uint32_t target_batch_size);

 private:
  SortedQueryList& queries_;
  std::chrono::duration<long, std::nano> dctrl_;
  std::chrono::duration<long, std::nano> ddata_;
  const ModelProfile* profile_;
  TimePoint last_sched_at_;
  SortedQueryList inputs_;
  std::vector<std::shared_ptr<QueryContext>> drops_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
