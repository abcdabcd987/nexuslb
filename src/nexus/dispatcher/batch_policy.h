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

// Whether to drop head of queue during batching.
class DropPolicy {
 public:
  enum Value {
    // Do not drop head of queue unless it is not possible to finish in time.
    kDropTimeout = 1,
    // Allow dropping head of queue so that the system can keep big batch size.
    kWindowDrop = 2,
    // Try to keep the target batch size but don't drop the head of queue.
    // All requests will be served in a FCFS manner, even if that means timeout.
    kWindowFCFS = 3,
  };

  constexpr DropPolicy() : value_(kWindowDrop) {}
  constexpr DropPolicy(Value value) : value_(value) {}
  constexpr operator Value() const { return value_; }
  constexpr const char* ToString() const;
  static constexpr const char* ToString(DropPolicy c);
  static constexpr std::optional<DropPolicy> Parse(std::string_view s);

 private:
  Value value_;
};

class IncrementalBatchPolicy {
 public:
  IncrementalBatchPolicy(DropPolicy drop_policy, std::chrono::nanoseconds dctrl,
                         std::chrono::nanoseconds ddata,
                         SortedQueryList& queries);

  IncrementalBatchPolicy(const IncrementalBatchPolicy& other) = delete;
  IncrementalBatchPolicy& operator=(const IncrementalBatchPolicy& other) =
      delete;
  IncrementalBatchPolicy(IncrementalBatchPolicy&& other) = delete;
  IncrementalBatchPolicy& operator=(IncrementalBatchPolicy&& other) = delete;

  uint32_t batch_size() const { return batch_size_; }
  TimePoint earlist_arrival() const;
  TimePoint deadline() const { return deadline_; }
  SortedQueryList PopInputs();
  SortedQueryList PopDrops();
  size_t CountDrops() const;
  void SetProfile(const ModelProfile& profile);
  void Update(TimePoint sched_at, TimePoint gpu_free_at,
              uint32_t target_batch_size);

 private:
  void UpdateInternal(TimePoint sched_at, TimePoint gpu_free_at,
                      uint32_t target_batch_size);

  DropPolicy drop_policy_;
  std::chrono::nanoseconds dctrl_;
  std::chrono::nanoseconds ddata_;
  SortedQueryList& queries_;
  const ModelProfile* profile_;
  TimePoint last_sched_at_;
  SortedQueryList prefix_;
  SortedQueryList suffix_;
  SortedQueryList timeout_;
  uint32_t batch_size_;
  TimePoint deadline_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
