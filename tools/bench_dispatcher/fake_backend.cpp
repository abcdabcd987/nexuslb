#include "bench_dispatcher/fake_backend.h"

#include <glog/logging.h>

#include "bench_dispatcher/fake_frontend.h"

namespace nexus {
namespace dispatcher {

namespace {

bool HeapOrderBatchPlanByExecTimeASC(const BatchPlanProto& lhs,
                                     const BatchPlanProto& rhs) {
  return lhs.exec_time_ns() > rhs.exec_time_ns();
}

bool BatchPlanIntersects(const BatchPlanProto& a, const BatchPlanProto& b) {
  if (a.expected_finish_time_ns() <= b.exec_time_ns()) return false;
  if (b.expected_finish_time_ns() <= a.exec_time_ns()) return false;
  return true;
}

}  // namespace

FakeBackendDelegate::FakeBackendDelegate(ario::EpollExecutor* executor,
                                         uint32_t node_id,
                                         FakeDispatcherAccessor* accessor)
    : BackendDelegate(NodeId(node_id),
                      {{GpuId(node_id), 0, "FakeGPU", "FakeUUID", 0}}),
      executor_(executor),
      accessor_(accessor),
      timer_(*executor_) {}

void FakeBackendDelegate::Tick() {
  // Ignore
}

void FakeBackendDelegate::SendLoadModelCommand(
    uint32_t gpu_idx, const ModelSession& model_session, uint32_t max_batch,
    ModelIndex model_index) {
  // Ignore
}

void FakeBackendDelegate::EnqueueBatchPlan(BatchPlanProto&& request) {
  TimePoint now = Clock::now();
  auto now_ns = now.time_since_epoch().count();

  CHECK_LE(request.exec_time_ns(), request.expected_finish_time_ns())
      << "Incorrect finish time. " << request.DebugString();
  LOG_IF(ERROR, now_ns > request.exec_time_ns())
      << "BatchPlan too late. " << request.DebugString();

  std::lock_guard lock(mutex_);
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

void FakeBackendDelegate::SetupTimer() {
  if (!batchplans_.empty()) {
    TimePoint finish_time(
        std::chrono::nanoseconds(batchplans_[0].expected_finish_time_ns()));
    if (timer_.timeout() != finish_time) {
      timer_.SetTimeout(finish_time);
      timer_.AsyncWait([this](ario::ErrorCode error) { OnTimer(error); });
    }
  }
}

void FakeBackendDelegate::DrainBatchPlans() {
  for (const auto& plan : batchplans_) {
    OnBatchFinish(plan);
  }
  batchplans_.clear();
}

void FakeBackendDelegate::OnBatchFinish(const BatchPlanProto& plan) {
  auto frontend_id = plan.queries(0).query_without_input().frontend_id();
  auto* frontend = accessor_->GetFrontend(NodeId(frontend_id)).get();
  auto* fake = static_cast<FakeFrontendDelegate*>(frontend);
  fake->GotBatchReply(plan);
}

void FakeBackendDelegate::OnTimer(ario::ErrorCode) {
  TimePoint now = Clock::now();
  auto now_ns = now.time_since_epoch().count();
  std::vector<BatchPlanProto> finished_plans;
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
  for (const auto& plan : finished_plans) {
    OnBatchFinish(plan);
  }
}

}  // namespace dispatcher
}  // namespace nexus
