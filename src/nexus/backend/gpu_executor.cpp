#include "nexus/backend/gpu_executor.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <thread>

#include "nexus/backend/backend_server.h"
#include "nexus/common/device.h"
#include "nexus/common/typedef.h"
#include "nexus/common/util.h"

#ifdef USE_CAFFE
#include "nexus/backend/caffe_model.h"
#endif

DECLARE_int32(occupancy_valid);

namespace {
bool OrderBatchPlanProtoByExecTimeDesc(
    const std::shared_ptr<nexus::backend::BatchPlanContext>& lhs,
    const std::shared_ptr<nexus::backend::BatchPlanContext>& rhs) {
  return lhs->proto().exec_time_ns() > rhs->proto().exec_time_ns();
}
}  // namespace

namespace nexus {
namespace backend {

GpuExecutorPlanFollower::GpuExecutorPlanFollower(int gpu_id,
                                                 ario::PollerType poller_type)
    : gpu_id_(gpu_id), executor_(poller_type), next_timer_(executor_) {}

GpuExecutorPlanFollower::~GpuExecutorPlanFollower() {
  if (thread_.joinable()) {
    LOG(FATAL) << "GpuExecutorPlanFollower is destructed before joined.";
  }
}

void GpuExecutorPlanFollower::Start(int core) {
  thread_ = std::thread([this, core] {
    if (core >= 0) {
      PinCpu(core);
      LOG(INFO) << "GPU executor is pinned on CPU " << core;
    }
    pthread_setname_np(pthread_self(), "GpuExecutor");
    executor_.RunEventLoop();
  });
}

void GpuExecutorPlanFollower::Stop() {
  executor_.StopEventLoop();
  thread_.join();
}

void GpuExecutorPlanFollower::AddModel(std::shared_ptr<ModelExecutor> model) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto model_index = model->model()->model_index();
  if (models_.size() <= model_index.t) {
    models_.resize(model_index + 1);
  }
  CHECK(!models_[model_index.t]);
  models_[model_index.t] = model;
}

void GpuExecutorPlanFollower::RemoveModel(
    std::shared_ptr<ModelExecutor> model) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto model_index = model->model()->model_index();
  CHECK_GT(models_.size(), model_index);
  CHECK(models_[model_index.t] != nullptr);
  models_[model_index.t] = nullptr;
}

void GpuExecutorPlanFollower::AddBatchPlan(
    std::shared_ptr<BatchPlanContext> plan) {
  auto now = Clock::now();
  plan->stats().set_prepared_ns(now.time_since_epoch().count());
  std::lock_guard<std::mutex> lock(mutex_);
  plans_.push_back(plan);
  std::push_heap(plans_.begin(), plans_.end(),
                 OrderBatchPlanProtoByExecTimeDesc);
  UpdateTimer();
}

void GpuExecutorPlanFollower::UpdateTimer() {
  if (plans_.empty()) {
    return;
  }
  const auto& plan = plans_[0];
  TimePoint exec_time(std::chrono::nanoseconds(plan->proto().exec_time_ns()));
  if (exec_time != next_timer_.timeout()) {
    next_timer_.SetTimeout(exec_time);
    next_timer_.AsyncWait([this](ario::ErrorCode error) { OnTimer(error); });
  }
}

void GpuExecutorPlanFollower::OnTimer(ario::ErrorCode error) {
  using namespace std::chrono;
  if (error != ario::ErrorCode::kOk) {
    return;
  }
  auto start_time = Clock::now();
  std::shared_ptr<BatchPlanContext> plan;
  std::shared_ptr<ModelExecutor> model;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (plans_.empty()) {
      LOG(ERROR) << "Woke up without batch plan to run.";
      return;
    }
    std::pop_heap(plans_.begin(), plans_.end(),
                  OrderBatchPlanProtoByExecTimeDesc);
    plan = std::move(plans_.back());
    plans_.pop_back();
  }
  plan->stats().set_actual_exec_ns(start_time.time_since_epoch().count());
  TimePoint exec_time(nanoseconds(plan->proto().exec_time_ns()));
  auto queue_delay_us =
      (start_time.time_since_epoch().count() - plan->stats().prepared_ns()) /
      1000;
  auto start_delay_us =
      duration_cast<microseconds>(start_time - exec_time).count();
  {
    auto model_index = plan->proto().model_index();
    std::lock_guard<std::mutex> lock(mutex_);
    if (models_.size() <= model_index || !models_[model_index]) {
      LOG(ERROR) << "Cannot find ModelIndex " << model_index;
      std::lock_guard<std::mutex> lock(mutex_);
      UpdateTimer();
      return;
    }
    model = models_[model_index];
  }
  const auto& model_name = model->model()->model_session().model_name();
  if (start_delay_us > 1000) {
    LOG(WARNING) << "Huge start  delay. " << model_name
                 << ". plan_id=" << plan->proto().plan_id()
                 << ", queue_delay=" << queue_delay_us << "us"
                 << ", start_delay=" << start_delay_us << "us";
  }

  if (start_delay_us > 20000) {
    LOG(ERROR) << "Intolerable start delay: " << start_delay_us
               << "us. Drop current batch plan_id=" << plan->plan_id();
    model->DropBatchPlan(plan);
    std::lock_guard<std::mutex> lock(mutex_);
    UpdateTimer();
    return;
  }

  VLOG(1) << "Executing BatchPlan: plan_id=" << plan->proto().plan_id()
          << ", model_name=" << model_name
          << ", batch_size=" << plan->proto().queries_size()
          << ", start_delay=" << start_delay_us << "us";
  bool is_executing = is_executing_.test_and_set();
  CHECK(!is_executing)
      << "BUG: the backend has not finished the previous batch.";
  model->ExecuteBatchPlan(plan);
  auto finish_time = Clock::now();
  auto elapse_us =
      duration_cast<microseconds>(finish_time - start_time).count();
  auto expected_elapse_us =
      (plan->proto().expected_finish_time_ns() - plan->proto().exec_time_ns()) /
      1000;
  auto exec_delay_us = elapse_us - expected_elapse_us;
  auto finish_time_ns = finish_time.time_since_epoch().count();
  auto finish_delay_us =
      (finish_time_ns - plan->proto().expected_finish_time_ns()) / 1000;
  VLOG(1) << "BatchPlan finished. plan_id=" << plan->proto().plan_id()
          << ", model_name=" << model_name
          << ", batch_size=" << plan->proto().queries_size()
          << ", start_delay=" << start_delay_us << "us"
          << ", elapse=" << elapse_us << "us"
          << ", finish_delay=" << finish_delay_us << "us";
  if (finish_delay_us > 1000) {
    LOG(WARNING) << "Huge finish delay. " << model_name
                 << ". plan_id=" << plan->proto().plan_id()
                 << ", queue_delay=" << queue_delay_us << "us"
                 << ", start_delay=" << start_delay_us << "us"
                 << ", finish_delay=" << finish_delay_us << "us"
                 << ", exec_delay=" << exec_delay_us << "us";
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    UpdateTimer();
  }
  is_executing_.clear();
}

}  // namespace backend
}  // namespace nexus
