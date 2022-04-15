#include "nexus_scheduler/gpu_executor.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>

#include <thread>

#include "nexus/common/device.h"

DECLARE_int32(occupancy_valid);

namespace nexus {
namespace backend {

GpuExecutorMultiBatching::GpuExecutorMultiBatching(int gpu_id)
    : gpu_id_(gpu_id), running_(false) {}

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
  models_.push_back(model);
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

void GpuExecutorMultiBatching::Run() {
  double min_cycle_us = 50.;  // us
  LOG(INFO) << "GpuExecutor started";
  while (running_) {
    std::vector<std::shared_ptr<ModelExecutor> > models;
    {
      // Take a snapshot
      std::lock_guard<std::mutex> lock(models_mu_);
      models = models_;
    }
    double exec_cycle_us = 0.;
    for (auto model : models) {
      exec_cycle_us += model->Execute();
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

}  // namespace backend
}  // namespace nexus
