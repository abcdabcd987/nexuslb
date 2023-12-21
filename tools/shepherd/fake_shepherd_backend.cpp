#include "shepherd/fake_shepherd_backend.h"

#include <glog/logging.h>

#include <algorithm>
#include <boost/asio/defer.hpp>

#include "shepherd/common.h"
#include "shepherd/fake_shepherd_frontend.h"

namespace nexus::shepherd {

namespace {

bool HeapOrderBatchPlanByExecTimeASC(const BatchPlan& lhs,
                                     const BatchPlan& rhs) {
  return lhs.exec_at > rhs.exec_at;
}

bool BatchPlanIntersects(const BatchPlan& a, const BatchPlan& b) {
  if (a.finish_at <= b.exec_at) return false;
  if (b.finish_at <= a.exec_at) return false;
  return true;
}

}  // namespace

FakeShepherdBackend::FakeShepherdBackend(boost::asio::io_context* io_context,
                                         FakeObjectAccessor* accessor,
                                         int gpu_id, bool save_archive)
    : io_context_(*CHECK_NOTNULL(io_context)),
      accessor_(*CHECK_NOTNULL(accessor)),
      gpu_id_(gpu_id),
      timer_(io_context_),
      stub_(this),
      save_archive_(save_archive) {}

void FakeShepherdBackend::Stop() {
  timer_.cancel();
  DrainBatchPlans();
}

void FakeShepherdBackend::Stub::RunBatch(BatchPlan request,
                                         std::optional<int> preempt_batch_id) {
  super_->RunBatchInternal(std::move(request), preempt_batch_id);
}

void FakeShepherdBackend::RunBatchInternal(
    BatchPlan request, std::optional<int> preempt_batch_id) {
  VLOG(0) << "RunBatch"
          << " batch_id=" << request.batch_id << " gpu_id=" << gpu_id_
          << " preempt=" << preempt_batch_id.value_or(0)
          << " model_id=" << request.model_id
          << " bs=" << request.query_ids.size();
  TimePoint now = Clock::now();
  auto now_ns = now.time_since_epoch().count();

  CHECK_LE(request.exec_time_ns(), request.expected_finish_time_ns())
      << "Incorrect finish time.";
  // CHECK_LE(now_ns, request.exec_time_ns()) << "BatchPlan too late.";

  if (preempt_batch_id.has_value()) {
    CHECK(!batchplans_.empty()) << "Cannot preempt. No current plan.";
    int idx = -1;
    for (size_t i = 0; i < batchplans_.size(); ++i) {
      if (batchplans_[i].batch_id == preempt_batch_id.value()) {
        idx = i;
        break;
      }
    }
    CHECK(idx != -1) << "Cannot preempt. Batchplan not found. preempt_batch_id="
                     << *preempt_batch_id;
    if (idx == 0) {
      std::pop_heap(batchplans_.begin(), batchplans_.end(),
                    HeapOrderBatchPlanByExecTimeASC);
      batchplans_.pop_back();
    } else {
      std::swap(batchplans_[idx], batchplans_.back());
      batchplans_.pop_back();
      std::make_heap(batchplans_.begin(), batchplans_.end(),
                     HeapOrderBatchPlanByExecTimeASC);
    }
  }
  for (const auto& plan : batchplans_) {
    CHECK(!BatchPlanIntersects(plan, request))
        << "Batchplan intersects."
        << " batchplans_.size()=" << batchplans_.size() << "\n"
        << "existing plan: exec_time=base"
        << " finish_time=base+"
        << (plan.expected_finish_time_ns() - plan.exec_time_ns()) << "\n"
        << "new plan: exec_time=base+"
        << (request.exec_time_ns() - plan.exec_time_ns())
        << " finish_time=base+"
        << (request.expected_finish_time_ns() - plan.exec_time_ns());
  }

  batchplans_.emplace_back(std::move(request));
  std::push_heap(batchplans_.begin(), batchplans_.end(),
                 HeapOrderBatchPlanByExecTimeASC);
  SetupTimer();
}

void FakeShepherdBackend::SetupTimer() {
  if (batchplans_.empty()) {
    timer_.cancel();
    return;
  }
  auto finish_at = batchplans_[0].finish_at;
  if (timer_.expiry() != finish_at) {
    timer_.expires_at(finish_at);
    timer_.async_wait([this](boost::system::error_code ec) {
      if (ec) return;
      OnTimer();
    });
  }
}

void FakeShepherdBackend::DrainBatchPlans() {
  for (auto& plan : batchplans_) {
    OnBatchFinish(plan);
    SaveBatchPlan(std::move(plan));
  }
  batchplans_.clear();
}

void FakeShepherdBackend::OnBatchFinish(const BatchPlan& plan) {
  auto frontend = accessor_.GetFrontend(plan.model_id);
  frontend->GotBatchReply(plan);
}

void FakeShepherdBackend::OnTimer() {
  TimePoint now = Clock::now();
  auto now_ns = now.time_since_epoch().count();
  std::vector<BatchPlan> finished_plans;
  while (!batchplans_.empty()) {
    if (batchplans_[0].expected_finish_time_ns() > now_ns) {
      break;
    }
    finished_plans.emplace_back(std::move(batchplans_[0]));
    std::pop_heap(batchplans_.begin(), batchplans_.end(),
                  HeapOrderBatchPlanByExecTimeASC);
    batchplans_.pop_back();
  }
  SetupTimer();
  for (auto& plan : finished_plans) {
    OnBatchFinish(plan);
    SaveBatchPlan(std::move(plan));
  }
}

void FakeShepherdBackend::SaveBatchPlan(BatchPlan plan) {
  if (save_archive_) {
    batchplan_archive_.emplace_back(std::move(plan));
  }
}

}  // namespace nexus::shepherd
