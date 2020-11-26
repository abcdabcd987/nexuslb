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

GpuExecutorMultiBatching::GpuExecutorMultiBatching(int gpu_id)
    : gpu_id_(gpu_id), running_(false), utilization_(-1.) {}

void GpuExecutorMultiBatching::Start(int core) {
  running_ = true;
  thread_ = std::thread(&GpuExecutorMultiBatching::Run, this);
  if (core >= 0) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(thread_.native_handle(), sizeof(cpu_set_t),
                                    &cpuset);
    if (rc != 0) {
      LOG(ERROR) << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
    LOG(INFO) << "GPU executor is pinned on CPU " << core;
  }
}

void GpuExecutorMultiBatching::Stop() {
  running_ = false;
  if (thread_.joinable()) {
    thread_.join();
  }
}

void GpuExecutorMultiBatching::AddModel(std::shared_ptr<ModelExecutor> model) {
  std::lock_guard<std::mutex> lock(models_mu_);
  if (model->backup()) {
    backup_models_.push_back(model);
  } else {
    models_.push_back(model);
  }
}

void GpuExecutorMultiBatching::RemoveModel(
    std::shared_ptr<ModelExecutor> model) {
  std::lock_guard<std::mutex> lock(models_mu_);
  for (auto iter = models_.begin(); iter != models_.end(); ++iter) {
    if (*iter == model) {
      models_.erase(iter);
      break;
    }
  }
}

double GpuExecutorMultiBatching::CurrentUtilization() {
  auto now = Clock::now();
  // std::lock_guard<std::mutex> util_lock(util_mu_);
  // if (utilization_ >= 0) {
  //   auto elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
  //       now - last_check_time_).count();
  //   if (elapse < FLAGS_occupancy_valid) {
  //     return utilization_;
  //   }
  // }
  // last_check_time_ = now;
  if (duty_cycle_us_ == 0) {
    // No model loaded so far
    utilization_ = 0;
    return 0.;
  }
  std::vector<std::shared_ptr<ModelExecutor> > models;
  std::vector<std::shared_ptr<ModelExecutor> > backup_models;
  {
    // std::lock_guard<std::mutex> model_lock(models_mu_);
    models = models_;
    backup_models = backup_models_;
  }
  double exec_cycle = 0.;
  for (auto& model : models) {
    int curr_queue_len = model->NumberOfOpenRequests();
    TimePoint last_exec_time = model->LastExecuteFinishTime();
    double elapse = std::chrono::duration_cast<std::chrono::microseconds>(
                        now - last_exec_time)
                        .count();
    int est_queue_len = (int)std::min(elapse / duty_cycle_us_ * curr_queue_len,
                                      (double)model->model()->max_batch());
    LOG(INFO) << model->model()->model_session_id()
              << " estimate batch size: " << est_queue_len;
    if (est_queue_len > 0) {
      exec_cycle += model->profile()->GetForwardLatency(est_queue_len);
    }
  }
  for (auto& model : backup_models) {
    int queue_len = model->NumberOfOpenRequests();
    if (queue_len > 0) {
      exec_cycle += model->profile()->GetForwardLatency(queue_len);
    }
  }
  // utilization_ = exec_cycle / duty_cycle_us_;
  // LOG(INFO) << "Utilization: " << utilization_ << " (exec/duty: " <<
  //     exec_cycle << " / " << duty_cycle_us_ << " us)";
  double utilization = exec_cycle / duty_cycle_us_;
  LOG(INFO) << "Utilization: " << utilization << " (exec/duty: " << exec_cycle
            << " / " << duty_cycle_us_ << " us)";
  return utilization;
}

void GpuExecutorMultiBatching::Run() {
#ifdef USE_CAFFE
  caffe::Caffe::set_mode(caffe::Caffe::GPU);
  caffe::Caffe::set_release_memory(false);
  caffe::Caffe::SetDevice(gpu_id_);
#endif

  NEXUS_CUDA_CHECK(cudaSetDevice(gpu_id_));
  double min_cycle_us = 50.;  // us
  LOG(INFO) << "GpuExecutor started";
  while (running_) {
    std::vector<std::shared_ptr<ModelExecutor> > models;
    std::vector<std::shared_ptr<ModelExecutor> > backup_models;
    {
      // Take a snapshot
      std::lock_guard<std::mutex> lock(models_mu_);
      models = models_;
      backup_models = backup_models_;
    }
    double exec_cycle_us = 0.;
    for (auto model : models) {
      exec_cycle_us += model->Execute();
    }
    double budget = duty_cycle_us_ - exec_cycle_us;
    for (auto model : backup_models) {
      if (budget <= 0) {
        break;
      }
      uint32_t batch = 1;
      auto profile = model->profile();
      while (true) {
        double fwd_lat = profile->GetForwardLatency(batch);
        if (fwd_lat == 0 || fwd_lat >= budget) {
          --batch;
          break;
        }
        ++batch;
      }
      if (batch > 0) {
        auto lat = model->Execute(batch);
        budget -= lat;
        exec_cycle_us += lat;
      }
    }
    if (exec_cycle_us < min_cycle_us) {
      // ensure the cycle to be at least min_cycle to avoid acquiring lock
      // too frequently in the ModelInstance
      std::this_thread::sleep_for(
          std::chrono::microseconds(int(min_cycle_us - exec_cycle_us)));
    }
  }
  LOG(INFO) << "GpuExecutor stopped";
}

GpuExecutorNoMultiBatching::GpuExecutorNoMultiBatching(int gpu_id)
    : gpu_id_(gpu_id) {}

void GpuExecutorNoMultiBatching::Start(int core) { core_ = core; }

void GpuExecutorNoMultiBatching::Stop() {
  for (auto& iter : threads_) {
    iter.second->Stop();
  }
  threads_.clear();
}

void GpuExecutorNoMultiBatching::AddModel(
    std::shared_ptr<ModelExecutor> model) {
  std::lock_guard<std::mutex> lock(mu_);
  std::unique_ptr<GpuExecutorMultiBatching> exec(
      new GpuExecutorMultiBatching(gpu_id_));
  exec->AddModel(model);
  // Do not bind core when multi-batching is disabled
  exec->Start();
  threads_.emplace(model->model()->model_session_id(), std::move(exec));
}

void GpuExecutorNoMultiBatching::RemoveModel(
    std::shared_ptr<ModelExecutor> model) {
  std::lock_guard<std::mutex> lock(mu_);
  auto sess_id = model->model()->model_session_id();
  threads_.at(sess_id)->Stop();
  threads_.erase(sess_id);
}

double GpuExecutorNoMultiBatching::CurrentUtilization() {
  // Doesn't support utilization
  return -1.;
}

GpuExecutorPlanFollower::GpuExecutorPlanFollower(int gpu_id)
    : gpu_id_(gpu_id),
      io_context_work_guard_(io_context_.get_executor()),
      next_timer_(io_context_) {}

GpuExecutorPlanFollower::~GpuExecutorPlanFollower() {
  if (thread_.joinable()) {
    LOG(FATAL) << "GpuExecutorPlanFollower is destructed before joined.";
  }
}

void GpuExecutorPlanFollower::Start(int core) {
  thread_ = std::thread(&GpuExecutorPlanFollower::Run, this);
  if (core >= 0) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(thread_.native_handle(), sizeof(cpu_set_t),
                                    &cpuset);
    if (rc != 0) {
      LOG(ERROR) << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
    LOG(INFO) << "GPU executor is pinned on CPU " << core;
  }
}

void GpuExecutorPlanFollower::Run() {
  io_context_.run();
  LOG(INFO) << "GpuExecutorPlanFollower exited.";
}

void GpuExecutorPlanFollower::Stop() {
  io_context_work_guard_.reset();
  LOG(INFO) << "io_context_work_guard reset. Waiting worker thread to join.";
  thread_.join();
}

void GpuExecutorPlanFollower::AddModel(std::shared_ptr<ModelExecutor> model) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto& model_session_id = model->model()->model_session_id();
  CHECK_EQ(models_.count(model_session_id), 0);
  models_[model_session_id] = model;
}

void GpuExecutorPlanFollower::RemoveModel(
    std::shared_ptr<ModelExecutor> model) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto& model_session_id = model->model()->model_session_id();
  auto n_erased = models_.erase(model_session_id);
  CHECK_EQ(n_erased, 1);
}

void GpuExecutorPlanFollower::AddBatchPlan(
    std::shared_ptr<BatchPlanContext> plan) {
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
  next_timer_.cancel();
  const auto& plan = plans_[0];
  TimePoint exec_time(std::chrono::nanoseconds(plan->proto().exec_time_ns()));
  next_timer_.async_wait(
      [this](const boost::system::error_code& error) { OnTimer(error); });
}

void GpuExecutorPlanFollower::OnTimer(const boost::system::error_code& error) {
  if (error) {
    LOG(ERROR) << error.message();
    return;
  }
  auto now = Clock::now();
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

    TimePoint exec_time(std::chrono::nanoseconds(plan->proto().exec_time_ns()));
    auto offset_us =
        std::chrono::duration_cast<std::chrono::microseconds>(now - exec_time)
            .count();
    if (std::abs(offset_us) > 10) {
      LOG(ERROR) << "Huge time offset when start executing BatchPlan"
                 << ". plan_id=" << plan->proto().plan_id()
                 << ", exec_time=" << (plan->proto().exec_time_ns() / 1e9)
                 << ", now=" << now.time_since_epoch().count()
                 << ", offset=" << offset_us << "us";
    }

    auto iter = models_.find(plan->proto().model_session_id());
    if (iter == models_.end()) {
      LOG(ERROR) << "Cannot find model_session "
                 << plan->proto().model_session_id();
      UpdateTimer();
      return;
    }
    model = iter->second;
  }
  VLOG(1) << "Executing BatchPlan: plan_id=" << plan->proto().plan_id()
          << ", model_session=" << plan->proto().model_session_id()
          << ", batch_size=" << plan->proto().queries_without_input_size();

  model->ExecuteBatchPlan(plan);
  UpdateTimer();
}

}  // namespace backend
}  // namespace nexus
