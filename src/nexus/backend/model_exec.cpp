#include "nexus/backend/model_exec.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>
#include <sstream>

#include "nexus/backend/batch_plan_context.h"
#include "nexus/backend/model_ins.h"
#include "nexus/backend/share_prefix_model.h"
#include "nexus/common/device.h"
#include "nexus/common/model_db.h"

#ifdef USE_TENSORFLOW
#include "nexus/backend/tf_share_model.h"
#endif

namespace nexus {
namespace backend {

DEFINE_int32(backend_count_interval, 1,
             "Interval to count number of requests in sec");
DEFINE_int32(backend_avg_interval, 5, "Moving average interval in sec");
DEFINE_int32(backend_batch_policy, 0, "0: Sliding window; 1: Earliest first;");

ModelExecutor::ModelExecutor(int gpu_id, const ModelInstanceConfig& config,
                             ModelIndex model_index,
                             BlockPriorityQueue<Task>& task_queue)
    : backup_(config.backup()),
      task_queue_(task_queue),
      batch_id_(0),
      open_requests_(0),
      req_rate_(FLAGS_backend_count_interval, FLAGS_backend_avg_interval),
      drop_rate_(FLAGS_backend_count_interval, FLAGS_backend_avg_interval) {
  // Create ModelInstance
  CreateModelInstance(gpu_id, config, model_index, &model_);
#ifdef USE_GPU
  auto gpu_device = DeviceManager::Singleton().GetGPUDevice(gpu_id);
  profile_ = ModelDatabase::Singleton().GetModelProfile(
      gpu_device->device_name(), gpu_device->uuid(), model_->profile_id());
#else
  if (gpu_id != -1) {
    LOG(FATAL)
        << "The code is complied without USE_GPU. Please set `gpu_id` to -1.";
  }
  auto* cpu = DeviceManager::Singleton().GetCPUDevice();
  profile_ = ModelDatabase::Singleton().GetModelProfile(
      cpu->name(), "GenericCPU", model_->profile_id());
#endif
  req_counter_ = MetricRegistry::Singleton().CreateIntervalCounter(
      FLAGS_backend_count_interval);
  drop_counter_ = MetricRegistry::Singleton().CreateIntervalCounter(
      FLAGS_backend_count_interval);
  for (auto const& info : config.backup_backend()) {
    backup_backends_.push_back(info.node_id());
  }

  // Create two input arrays. When one is used by forwarding, memcpy can still
  // happen to another.
  input_arrays_.insert(model_->CreateInputGpuArray());
  input_arrays_.insert(model_->CreateInputGpuArray());
  idle_input_arrays_ = input_arrays_;
}

ModelExecutor::~ModelExecutor() {
  MetricRegistry::Singleton().RemoveMetric(req_counter_);
  MetricRegistry::Singleton().RemoveMetric(drop_counter_);
}

std::shared_ptr<Array> ModelExecutor::AcquireInputArray() {
  LOG_IF(FATAL, idle_input_arrays_.empty()) << "No more idle input arrays";
  auto iter = idle_input_arrays_.begin();
  auto ret = *iter;
  idle_input_arrays_.erase(iter);
  return ret;
}

void ModelExecutor::ReleaseInputArray(std::shared_ptr<Array> array) {
  CHECK(input_arrays_.count(array) > 0) << "Invalid array";
  CHECK(idle_input_arrays_.count(array) == 0) << "Already idle";
  idle_input_arrays_.insert(std::move(array));
}

double ModelExecutor::GetRequestRate() {
  for (auto nreq : req_counter_->GetHistory()) {
    if (req_rate_.rate() < 0 && nreq == 0) {
      continue;
    }
    req_rate_.AddSample(nreq);
  }
  return req_rate_.rate();
}

double ModelExecutor::GetDropRate() {
  for (auto nreq : drop_counter_->GetHistory()) {
    if (drop_rate_.rate() < 0 && nreq == 0) {
      continue;
    }
    drop_rate_.AddSample(nreq);
  }
  return drop_rate_.rate();
}

bool ModelExecutor::IsSharePrefixModel() const {
  // return (dynamic_cast<SharePrefixModel*>(model_.get()) != nullptr);
  return false;
}

bool ModelExecutor::IsTFShareModel() const {
#ifdef USE_TENSORFLOW
  // return (dynamic_cast<TFShareModel*>(model_.get()) != nullptr);
  return false;
#else
  return false;
#endif
}

bool ModelExecutor::HasBackup() {
  std::lock_guard<std::mutex> lock(backup_mu_);
  return (backup_backends_.size() > 0);
}

std::vector<uint32_t> ModelExecutor::BackupBackends() {
  std::lock_guard<std::mutex> lock(backup_mu_);
  return backup_backends_;
}

void ModelExecutor::UpdateBackupBackends(const ModelInstanceConfig& config) {
  std::lock_guard<std::mutex> lock(backup_mu_);
  backup_backends_.clear();
  for (auto& info : config.backup_backend()) {
    backup_backends_.push_back(info.node_id());
  }
}

bool ModelExecutor::Preprocess(std::shared_ptr<Task> task, bool force) {
  int cnt = 1;
  if (task->query.window_size() > 0) {
    cnt = task->query.window_size();
  }
  bool limit = !force && HasBackup();
  if (!IncreaseOpenRequests(cnt, limit)) {
    return false;
  }
  req_counter_->Increase(cnt);
  model_->Preprocess(task);
  if (task->result.status() != CTRL_OK) {
    return false;
  }
  // Add to input queue only if not in a BatchPlan.
  if (!task->plan_id.has_value()) {
    std::lock_guard<std::mutex> lock(task_mu_);
    processing_tasks_.emplace(task->task_id, task);
    for (auto input : task->inputs) {
      input_queue_.push(input);
    }
  }
  return true;
}

bool ModelExecutor::AddPreprocessedTask(std::shared_ptr<Task> task,
                                        bool force) {
  int cnt = task->inputs.size();
  bool limit = !force && HasBackup();
  if (!IncreaseOpenRequests(cnt, limit)) {
    return false;
  }
  req_counter_->Increase(cnt);
  std::lock_guard<std::mutex> lock(task_mu_);
  processing_tasks_.emplace(task->task_id, task);
  for (auto input : task->inputs) {
    input_queue_.push(input);
  }
  return true;
}

void ModelExecutor::Postprocess(std::shared_ptr<Task> task) {
  model_->Postprocess(task);
}

int ModelExecutor::NumberOfOpenRequests() const {
  return open_requests_.load(std::memory_order_relaxed);
}

uint64_t ModelExecutor::GetPeakMemoryUsage() {
  return model_->GetPeakBytesInUse();
}

uint64_t ModelExecutor::GetStaticMemoryUsage() {
  return model_->GetBytesInUse();
}

TimePoint ModelExecutor::LastExecuteFinishTime() {
  std::lock_guard<std::mutex> lock(time_mu_);
  return last_exec_finish_;
}

bool ModelExecutor::IncreaseOpenRequests(int cnt, bool limit_max_batch) {
  if (!limit_max_batch) {
    open_requests_.fetch_add(cnt, std::memory_order_relaxed);
    return true;
  }
  // opportunistic
  int nreqs = open_requests_.load(std::memory_order_relaxed);
  if (nreqs + cnt > model_->batch()) {
    return false;
  }
  open_requests_.fetch_add(cnt, std::memory_order_relaxed);
  return true;
}

void ModelExecutor::DecreaseOpenRequests(int cnt) {
  int prev = open_requests_.fetch_sub(cnt, std::memory_order_relaxed);
  // CHECK_GE(prev, cnt) << "Negative value in open requests";
}

void ModelExecutor::RemoveTask(std::shared_ptr<Task> task) {
  task->stage = kPostprocess;
  task_queue_.push(task);
  processing_tasks_.erase(task->task_id);
}

void ModelExecutor::ExecuteBatchPlan(std::shared_ptr<BatchPlanContext> plan) {
  auto batch_task = GetBatchTaskByBatchPlan(plan);
  int dequeue_cnt = plan->proto().queries_size();
  drop_counter_->Increase(dequeue_cnt - batch_task->batch_size());
  if (batch_task->batch_size() == 0) {
    std::lock_guard<std::mutex> lock(time_mu_);
    DecreaseOpenRequests(dequeue_cnt);
    last_exec_finish_ = Clock::now();
    VLOG(1) << "ExecuteBatchPlan return early. batch_size=0, plan_id="
            << plan->proto().plan_id();
    return;
  }

  // Each time recompute output sizes because it might change for prefix model
  std::unordered_map<std::string, size_t> output_sizes;
  for (auto iter : model_->OutputShapes()) {
    output_sizes.emplace(iter.first, iter.second.NumElements(1));
  }
  batch_task->CreateOutputArrays(output_sizes,
                                 DeviceManager::Singleton().GetCPUDevice());
  model_->Forward(batch_task);
  {
    std::lock_guard<std::mutex> lock(time_mu_);
    last_exec_finish_ = Clock::now();
  }
  DecreaseOpenRequests(dequeue_cnt);

  auto outputs = batch_task->outputs();
  auto& tasks = batch_task->tasks();
  std::vector<std::shared_ptr<Task>> completed_tasks;
  completed_tasks.reserve(outputs.size());
  // Add output to corresponding tasks, and remove tasks that get all outputs
  for (int i = 0; i < outputs.size(); ++i) {
    auto output = outputs[i];
    auto task = tasks[i];
    if (task->AddOutput(output)) {
      completed_tasks.push_back(task);
      task->stage = kPostprocess;
    }
  }
  // task_queue_.push is expensive. So batch push here.
  task_queue_.batch_push(completed_tasks);

  ReleaseInputArray(plan->ReleaseInputArray());

  auto backend_finish_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               Clock::now().time_since_epoch())
                               .count();
  for (auto& task : tasks) {
    task->query.mutable_clock()->set_backend_finish_ns(backend_finish_ns);
  }
}

std::shared_ptr<BatchTask> ModelExecutor::GetBatchTaskByBatchPlan(
    std::shared_ptr<BatchPlanContext> plan) {
  auto backend_exec_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             Clock::now().time_since_epoch())
                             .count();
  auto tasks = plan->PopPreprocessedTasks();
  for (auto& task : tasks) {
    task->query.mutable_clock()->set_backend_exec_ns(backend_exec_ns);
  }
  auto batch_task = plan->batch_task();
  if (batch_task->batch_size() != plan->proto().queries_size()) {
    LOG(ERROR) << "Actual batch size differs from BatchPlan. plan_id="
               << plan->proto().plan_id()
               << ", actual=" << batch_task->batch_size()
               << ", expected=" << plan->proto().queries_size();
  }
  return batch_task;
}

}  // namespace backend
}  // namespace nexus
