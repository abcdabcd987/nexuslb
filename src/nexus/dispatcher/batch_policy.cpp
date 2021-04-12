#include "nexus/dispatcher/batch_policy.h"

#include <glog/logging.h>

namespace nexus {
namespace dispatcher {

namespace {

void GetBatchByExpandedWindow(
    TimePoint exec_time, uint32_t target_batch_size, SortedQueryList& queries,
    const ModelProfile& profile, SortedQueryList& inputs,
    std::vector<std::shared_ptr<QueryContext>>& drops) {
  using namespace std::chrono;
  auto proc_elapse = nanoseconds(0);
  // TODO: WithNStd
  proc_elapse +=
      nanoseconds(static_cast<long>(profile.GetPreprocessLatency() * 1e3));
  proc_elapse +=
      nanoseconds(static_cast<long>(profile.GetPostprocessLatency() * 1e3));

  // Drop existing timeout inputs
  while (!inputs.empty()) {
    auto deadline = (*inputs.begin())->deadline;
    auto fwd_elapse = nanoseconds(
        static_cast<long>(profile.GetForwardLatency(inputs.size()) * 1e3));
    auto finish_time = exec_time + fwd_elapse + proc_elapse;
    if (deadline > finish_time) {
      break;
    } else {
      drops.push_back(*inputs.begin());
      inputs.erase(inputs.begin());
    }
  }

  // Sliding window policy
  // See if there is any free lunch
  while (!queries.empty()) {
    if (inputs.size() < target_batch_size) {
      inputs.insert(*queries.begin());
      queries.erase(queries.begin());
      auto deadline = (*inputs.begin())->deadline;
      auto fwd_elapse = nanoseconds(
          static_cast<long>(profile.GetForwardLatency(inputs.size()) * 1e3));
      auto finish_time = exec_time + fwd_elapse + proc_elapse;
      if (deadline < finish_time) {
        drops.push_back(*inputs.begin());
        inputs.erase(inputs.begin());
      }
    } else {
      auto deadline =
          min((*inputs.begin())->deadline, (*queries.begin())->deadline);
      auto fwd_elapse = nanoseconds(static_cast<long>(
          profile.GetForwardLatency(inputs.size() + 1) * 1e3));
      auto finish_time = exec_time + fwd_elapse + proc_elapse;
      if (finish_time < deadline) {
        inputs.insert(*queries.begin());
        queries.erase(queries.begin());
      } else {
        break;
      }
    }
  }

  // Sanity check
  if (!inputs.empty()) {
    auto fwd_elapse = nanoseconds(
        static_cast<long>(profile.GetForwardLatency(inputs.size()) * 1e3));
    auto finish_time = exec_time + fwd_elapse + proc_elapse;
    CHECK_LE(finish_time.time_since_epoch().count(),
             (*inputs.begin())->deadline.time_since_epoch().count());
  }
}

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
  while (!inputs.empty()) {
    auto deadline = inputs[0]->deadline;
    auto fwd_elapse = nanoseconds(
        static_cast<long>(profile.GetForwardLatency(inputs.size()) * 1e3));
    auto finish_time = exec_time + fwd_elapse + proc_elapse;
    if (deadline > finish_time) {
      break;
    } else {
      drops.push_back(inputs[0]);
      inputs.pop_front();
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
  size_t qremain = queries.size();
  uint32_t bs = std::min(static_cast<uint32_t>(inputs.size() + qremain),
                         target_batch_size);
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

  // Sanity check
  if (!inputs.empty()) {
    auto fwd_elapse = nanoseconds(
        static_cast<long>(profile.GetForwardLatency(inputs.size()) * 1e3));
    auto finish_time = exec_time + fwd_elapse + proc_elapse;
    CHECK_LE(finish_time.time_since_epoch().count(),
             inputs.front()->deadline.time_since_epoch().count());
  }

  remains_begin = qiter;
}

}  // namespace

IncrementalBatchPolicy::IncrementalBatchPolicy(SortedQueryList& queries)
    : queries_(queries), profile_(nullptr), last_exec_time_() {}

void IncrementalBatchPolicy::Update(TimePoint exec_time,
                                    uint32_t target_batch_size) {
  CHECK_NE(profile_, nullptr) << "Profile not set.";
  CHECK_LE(last_exec_time_.time_since_epoch().count(),
           exec_time.time_since_epoch().count())
      << "Time can't go backwards.";
  last_exec_time_ = exec_time;

  GetBatchByExpandedWindow(exec_time, target_batch_size, queries_, *profile_,
                           inputs_, drops_);
}

void IncrementalBatchPolicy::SetProfile(const ModelProfile& profile) {
  profile_ = &profile;
}

std::vector<std::shared_ptr<QueryContext>> IncrementalBatchPolicy::PopInputs() {
  auto inputs = std::vector<std::shared_ptr<QueryContext>>();
  while (!inputs_.empty()) {
    auto query = *inputs_.begin();
    inputs_.erase(inputs_.begin());
    inputs.push_back(std::move(query));
  }
  return inputs;
}

std::vector<std::shared_ptr<QueryContext>> IncrementalBatchPolicy::PopDrops() {
  return std::move(drops_);
}

}  // namespace dispatcher
}  // namespace nexus
