#include "nexus/dispatcher/batch_policy.h"

#include <glog/logging.h>

namespace nexus {
namespace dispatcher {

namespace {

void GetBatchBySlidingWindowWithFreeLunch(
    TimePoint exec_time, uint32_t target_batch_size,
    const SortedQueryList& queries, const ModelProfile& profile,
    std::deque<std::shared_ptr<QueryContext>>& inputs,
    std::vector<std::shared_ptr<QueryContext>>& drops,
    SortedQueryList::iterator& remains_begin) {
  using namespace std::chrono;
  auto proc_elapse = nanoseconds(0);
  // TODO: WithNStd
  proc_elapse +=
      nanoseconds(static_cast<long>(profile.GetPreprocessLatency() * 1e3));
  proc_elapse +=
      nanoseconds(static_cast<long>(profile.GetPostprocessLatency() * 1e3));

  // Drop existing timeout inputs
  size_t qremain = queries.size();
  uint32_t bs = std::min(static_cast<uint32_t>(inputs.size() + qremain),
                         target_batch_size);
  while (!inputs.empty()) {
    auto deadline = inputs[0]->deadline;
    auto fwd_elapse =
        nanoseconds(static_cast<long>(profile.GetForwardLatency(bs) * 1e3));
    auto finish_time = exec_time + fwd_elapse + proc_elapse;
    if (deadline > finish_time) {
      break;
    } else {
      drops.push_back(inputs[0]);
      inputs.pop_front();
      bs = std::min(static_cast<uint32_t>(inputs.size() + qremain),
                    target_batch_size);
    }
  }

  LOG_IF(WARNING, !inputs.empty() && !queries.empty() &&
                      inputs.back()->deadline > (*queries.begin())->deadline)
      << "Out-of-order query";
  LOG_IF(ERROR, !inputs.empty() && !queries.empty() &&
                    inputs[0]->deadline > (*queries.begin())->deadline)
      << "Deadline of the new query is earlier than current input queue "
         "head's.";

  // Sliding window policy
  auto qiter = queries.begin();
  while (inputs.size() < bs && qiter != queries.end()) {
    --qremain;
    const auto& qctx = *qiter;
    auto deadline = inputs.empty() ? qctx->deadline : inputs[0]->deadline;
    auto fwd_elapse =
        nanoseconds(static_cast<long>(profile.GetForwardLatency(bs) * 1e3));
    auto finish_time = exec_time + fwd_elapse + proc_elapse;
    if (deadline > finish_time) {
      inputs.push_back(qctx);
    } else {
      drops.push_back(qctx);
      bs = std::min(static_cast<uint32_t>(inputs.size() + qremain),
                    target_batch_size);
    }
    ++qiter;
  }

  // See if there is any free lunch.
  while (qiter != queries.end()) {
    --qremain;
    const auto& qctx = *qiter;
    auto deadline = inputs.empty() ? qctx->deadline : inputs[0]->deadline;
    bs = inputs.size() + 1;
    auto fwd_elapse =
        nanoseconds(static_cast<long>(profile.GetForwardLatency(bs) * 1e3));
    auto finish_time = exec_time + fwd_elapse + proc_elapse;
    if (deadline > finish_time) {
      inputs.push_back(qctx);
    } else {
      break;
    }
    ++qiter;
  }

  remains_begin = qiter;
}

}  // namespace

BatchPolicyBySlidingWindowWithFreeLunch::BatchResult
BatchPolicyBySlidingWindowWithFreeLunch::Getbatch(
    TimePoint exec_time, uint32_t target_batch_size,
    const SortedQueryList& queries, const ModelProfile& profile) {
  std::deque<std::shared_ptr<QueryContext>> inputs;
  std::vector<std::shared_ptr<QueryContext>> drops, remains;
  SortedQueryList::iterator remains_begin;
  GetBatchBySlidingWindowWithFreeLunch(exec_time, target_batch_size, queries,
                                       profile, inputs, drops, remains_begin);
  remains.assign(remains_begin, queries.end());
  return {std::move(inputs), std::move(drops), std::move(remains)};
}

IncrementalBatchPolicy::IncrementalBatchPolicy(SortedQueryList& queries)
    : queries_(queries), profile_(nullptr), last_exec_time_() {}

void IncrementalBatchPolicy::Update(TimePoint exec_time,
                                    uint32_t target_batch_size) {
  CHECK_NE(profile_, nullptr) << "Profile not set.";
  CHECK_LE(last_exec_time_.time_since_epoch().count(),
           exec_time.time_since_epoch().count())
      << "Time can't go backwards.";
  last_exec_time_ = exec_time;

  SortedQueryList::iterator remains_begin;
  GetBatchBySlidingWindowWithFreeLunch(exec_time, target_batch_size, queries_,
                                       *profile_, inputs_, drops_,
                                       remains_begin);
  queries_.erase(queries_.begin(), remains_begin);
}

void IncrementalBatchPolicy::SetProfile(const ModelProfile& profile) {
  profile_ = &profile;
}

std::deque<std::shared_ptr<QueryContext>> IncrementalBatchPolicy::PopInputs() {
  return std::move(inputs_);
}

std::vector<std::shared_ptr<QueryContext>> IncrementalBatchPolicy::PopDrops() {
  return std::move(drops_);
}

}  // namespace dispatcher
}  // namespace nexus
