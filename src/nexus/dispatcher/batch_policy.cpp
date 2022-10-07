#include "nexus/dispatcher/batch_policy.h"

#include <glog/logging.h>

#include "nexus/common/time_util.h"

namespace nexus {
namespace dispatcher {

IncrementalBatchPolicy::IncrementalBatchPolicy(std::chrono::nanoseconds dctrl,
                                               std::chrono::nanoseconds ddata,
                                               SortedQueryList& queries)
    : dctrl_(dctrl),
      ddata_(ddata),
      queries_(queries),
      profile_(nullptr),
      last_sched_at_(),
      batch_size_(0),
      deadline_(TimePoint::max()) {}

void IncrementalBatchPolicy::Update(TimePoint sched_at, TimePoint gpu_free_at,
                                    uint32_t target_batch_size) {
  using namespace std::chrono;
  CHECK_NE(profile_, nullptr) << "Profile not set.";
  CHECK(last_sched_at_ <= sched_at)
      << "Time can't go backwards. diff="
      << (last_sched_at_ - sched_at).count() / 1e3 << "us";
  last_sched_at_ = sched_at;

  auto proc_elapse = nanoseconds(0);
  // TODO: WithNStd
  proc_elapse +=
      nanoseconds(static_cast<long>(profile_->GetPreprocessLatency() * 1e3));
  proc_elapse +=
      nanoseconds(static_cast<long>(profile_->GetPostprocessLatency() * 1e3));

  // Add new requests
  for (auto& qctx : queries_) {
    suffix_.insert(qctx);
  }
  queries_.clear();

  // Update suffix
  while (!suffix_.empty()) {
    auto earliest_exec_at =
        sched_at + dctrl_ + ddata_ * static_cast<long>(suffix_.size());
    auto exec_at = std::max(earliest_exec_at, gpu_free_at);
    auto fwd_elapse = nanoseconds(
        static_cast<long>(profile_->GetForwardLatency(suffix_.size()) * 1e3));
    auto finish_at = exec_at + fwd_elapse + proc_elapse;
    auto deadline = (*suffix_.begin())->deadline;
    if (deadline >= finish_at) {
      break;
    } else {
      prefix_.insert(*suffix_.begin());
      suffix_.erase(suffix_.begin());
    }
  }
  batch_size_ =
      std::min(target_batch_size, static_cast<uint32_t>(suffix_.size()));
  if (batch_size_ == 0) {
    deadline_ = TimePoint::max();
    return;
  }

  // Match prefix
  {
    auto earliest_exec_at =
        sched_at + dctrl_ + ddata_ * static_cast<long>(batch_size_);
    auto exec_at = std::max(earliest_exec_at, gpu_free_at);
    auto fwd_elapse = nanoseconds(
        static_cast<long>(profile_->GetForwardLatency(batch_size_) * 1e3));
    auto finish_at = exec_at + fwd_elapse + proc_elapse;
    while (!prefix_.empty()) {
      auto deadline = (*prefix_.begin())->deadline;
      if (deadline >= finish_at) {
        break;
      } else {
        drops_.push_back(*prefix_.begin());
        prefix_.erase(prefix_.begin());
      }
    }
  }
  if (!prefix_.empty()) {
    CHECK((*prefix_.rbegin())->deadline <= (*suffix_.begin())->deadline);
    deadline_ = (*prefix_.begin())->deadline;
  } else {
    deadline_ = (*suffix_.begin())->deadline;
  }

  // Free lunch
  auto total = prefix_.size() + suffix_.size();
  while (batch_size_ < total) {
    auto earliest_exec_at =
        sched_at + dctrl_ + ddata_ * static_cast<long>(batch_size_ + 1);
    auto exec_at = std::max(earliest_exec_at, gpu_free_at);
    auto fwd_elapse = nanoseconds(
        static_cast<long>(profile_->GetForwardLatency(batch_size_ + 1) * 1e3));
    auto finish_at = exec_at + fwd_elapse + proc_elapse;
    if (deadline_ >= finish_at) {
      batch_size_++;
    } else {
      break;
    }
  }
}

void IncrementalBatchPolicy::SetProfile(const ModelProfile& profile) {
  profile_ = &profile;
}

SortedQueryList IncrementalBatchPolicy::PopInputs() {
  CHECK_LE(batch_size_, prefix_.size() + suffix_.size());
  SortedQueryList inputs;
  for (uint32_t i = 0; i < batch_size_; ++i) {
    if (!prefix_.empty()) {
      inputs.insert(*prefix_.begin());
      prefix_.erase(prefix_.begin());
    } else {
      inputs.insert(*suffix_.begin());
      suffix_.erase(suffix_.begin());
    }
  }
  return std::move(inputs);
}

std::vector<std::shared_ptr<QueryContext>> IncrementalBatchPolicy::PopDrops() {
  return std::move(drops_);
}

}  // namespace dispatcher
}  // namespace nexus
