#include "nexus/dispatcher/batch_policy.h"

#include <glog/logging.h>

#include "nexus/common/time_util.h"

namespace nexus {
namespace dispatcher {

IncrementalBatchPolicy::IncrementalBatchPolicy(DropPolicy drop_policy,
                                               std::chrono::nanoseconds dctrl,
                                               std::chrono::nanoseconds ddata,
                                               SortedQueryList& queries)
    : drop_policy_(drop_policy),
      dctrl_(dctrl),
      ddata_(ddata),
      queries_(queries),
      profile_(nullptr),
      last_sched_at_(),
      batch_size_(0),
      deadline_(TimePoint::max()) {}

void IncrementalBatchPolicy::Update(TimePoint sched_at, TimePoint gpu_free_at,
                                    uint32_t target_batch_size) {
  CHECK_NE(profile_, nullptr) << "Profile not set.";
  CHECK(last_sched_at_ <= sched_at)
      << "Time can't go backwards. diff="
      << (last_sched_at_ - sched_at).count() / 1e3 << "us";
  last_sched_at_ = sched_at;

  // Add new requests
  for (auto& qctx : queries_) {
    suffix_.insert(qctx);
  }
  queries_.clear();

  switch (drop_policy_) {
    case DropPolicy::kDropTimeout:
      UpdateInternal(sched_at, gpu_free_at, 1);
      break;
    case DropPolicy::kWindowDrop:
      UpdateInternal(sched_at, gpu_free_at, target_batch_size);
      break;
    case DropPolicy::kWindowFCFS:
      UpdateInternal(sched_at, gpu_free_at, target_batch_size);
      break;
    default:
      LOG(FATAL) << "DropPolicy: not reachable.";
  }
}

void IncrementalBatchPolicy::UpdateInternal(TimePoint sched_at,
                                            TimePoint gpu_free_at,
                                            uint32_t target_batch_size) {
  using namespace std::chrono;
  auto proc_elapse = nanoseconds(0);
  // TODO: WithNStd
  proc_elapse +=
      nanoseconds(static_cast<long>(profile_->GetPreprocessLatency() * 1e3));
  proc_elapse +=
      nanoseconds(static_cast<long>(profile_->GetPostprocessLatency() * 1e3));

  // Whether to include timeout requests.
  const auto cnt_timeout =
      drop_policy_ == DropPolicy::kWindowFCFS ? timeout_.size() : 0;

  // Update suffix
  while (!suffix_.empty()) {
    auto bs = cnt_timeout + suffix_.size();
    auto earliest_exec_at = sched_at + dctrl_ + ddata_ * static_cast<long>(bs);
    auto exec_at = std::max(earliest_exec_at, gpu_free_at);
    auto fwd_elapse =
        nanoseconds(static_cast<long>(profile_->GetForwardLatency(bs) * 1e3));
    auto finish_at = exec_at + fwd_elapse + proc_elapse;
    auto deadline = (*suffix_.begin())->deadline;
    if (deadline >= finish_at) {
      break;
    } else {
      prefix_.insert(*suffix_.begin());
      suffix_.erase(suffix_.begin());
    }
  }
  batch_size_ = std::min(target_batch_size,
                         static_cast<uint32_t>(cnt_timeout + suffix_.size()));
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
        timeout_.insert(*prefix_.begin());
        prefix_.erase(prefix_.begin());
      }
    }
  }
  if (batch_size_ > cnt_timeout) {
    if (!prefix_.empty()) {
      CHECK((*prefix_.rbegin())->deadline <= (*suffix_.begin())->deadline);
      deadline_ = (*prefix_.begin())->deadline;
    } else {
      deadline_ = (*suffix_.begin())->deadline;
    }
  } else {
    deadline_ = TimePoint::max();
    return;
  }

  // Free lunch
  auto total = cnt_timeout + prefix_.size() + suffix_.size();
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

TimePoint IncrementalBatchPolicy::earlist_arrival() const {
  CHECK_GT(batch_size_, 0);
  QueryContext* qctx =
      !prefix_.empty() ? prefix_.begin()->get() : suffix_.begin()->get();
  auto ns = qctx->request.query_without_input().clock().frontend_recv_ns();
  return TimePoint(std::chrono::nanoseconds(ns));
}

SortedQueryList IncrementalBatchPolicy::PopInputs() {
  const auto cnt_timeout =
      drop_policy_ == DropPolicy::kWindowFCFS ? timeout_.size() : 0;
  CHECK_LE(batch_size_, cnt_timeout + prefix_.size() + suffix_.size());
  SortedQueryList inputs;
  while (inputs.size() < batch_size_ && inputs.size() < cnt_timeout) {
    inputs.insert(*timeout_.begin());
    timeout_.erase(timeout_.begin());
  }
  while (inputs.size() < batch_size_ && !prefix_.empty()) {
    inputs.insert(*prefix_.begin());
    prefix_.erase(prefix_.begin());
  }
  while (inputs.size() < batch_size_ && !suffix_.empty()) {
    inputs.insert(*suffix_.begin());
    suffix_.erase(suffix_.begin());
  }
  return std::move(inputs);
}

SortedQueryList IncrementalBatchPolicy::PopDrops() {
  if (drop_policy_ == DropPolicy::kWindowFCFS) {
    return {};
  } else {
    return std::move(timeout_);
  }
}

size_t IncrementalBatchPolicy::CountDrops() const {
  if (drop_policy_ == DropPolicy::kWindowFCFS) {
    return 0;
  } else {
    return timeout_.size();
  }
}

}  // namespace dispatcher
}  // namespace nexus
