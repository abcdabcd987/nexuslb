#include "shepherd/fake_shepherd_backend.h"

#include <glog/logging.h>

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
      save_archive_(save_archive) {}

void FakeShepherdBackend::Stop() {
  timer_.cancel();
  DrainBatchPlans();
}

void FakeShepherdBackend::RunBatch(BatchPlan request, Preemption preempt) {
  TimePoint now = Clock::now();
  auto now_ns = now.time_since_epoch().count();

  CHECK_LE(request.exec_time_ns(), request.expected_finish_time_ns())
      << "Incorrect finish time.";
  CHECK_LE(now_ns, request.exec_time_ns()) << "BatchPlan too late.";

  std::lock_guard lock(mutex_);
  if (preempt == Preemption::kYes) {
    CHECK(!batchplans_.empty()) << "Cannot preempt. No current plan.";
    auto old_plan = batchplans_.front();
    CHECK_LE(now_ns, old_plan.expected_finish_time_ns())
        << "Cannot preempt. Current plan is not running.";
    std::pop_heap(batchplans_.begin(), batchplans_.end(),
                  HeapOrderBatchPlanByExecTimeASC);
    batchplans_.pop_back();
  }

  for (const auto& plan : batchplans_) {
    CHECK(!BatchPlanIntersects(plan, request))
        << "Batchplan intersects.\n"
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
  if (!batchplans_.empty()) {
    auto finish_at = batchplans_[0].finish_at;
    if (timer_.expiry() != finish_at) {
      timer_.expires_at(finish_at);
      timer_.async_wait([this](boost::system::error_code ec) { OnTimer(ec); });
    }
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

void FakeShepherdBackend::OnTimer(boost::system::error_code ec) {
  if (ec) return;
  TimePoint now = Clock::now();
  auto now_ns = now.time_since_epoch().count();
  std::vector<BatchPlan> finished_plans;
  std::unique_lock lock(mutex_);
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
  lock.unlock();
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
