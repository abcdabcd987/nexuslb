#include "nexus/backend/model_exec.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>
#include <sstream>

#include "nexus/backend/batch_plan_context.h"
#include "nexus/backend/model_ins.h"
#include "nexus/backend/share_prefix_model.h"
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
                             BlockPriorityQueue<Task>& task_queue)
    : backup_(config.backup()),
      task_queue_(task_queue),
      batch_id_(0),
      open_requests_(0),
      req_rate_(FLAGS_backend_count_interval, FLAGS_backend_avg_interval),
      drop_rate_(FLAGS_backend_count_interval, FLAGS_backend_avg_interval) {
  // Create ModelInstance
  CreateModelInstance(gpu_id, config, &model_);
#ifdef USE_GPU
  auto gpu_device = DeviceManager::Singleton().GetGPUDevice(gpu_id);
  profile_ = ModelDatabase::Singleton().GetModelProfile(
      gpu_device->device_name(), gpu_device->uuid(), model_->profile_id());
#endif
  req_counter_ = MetricRegistry::Singleton().CreateIntervalCounter(
      FLAGS_backend_count_interval);
  drop_counter_ = MetricRegistry::Singleton().CreateIntervalCounter(
      FLAGS_backend_count_interval);
  input_array_ = model_->CreateInputGpuArray();
  for (auto const& info : config.backup_backend()) {
    backup_backends_.push_back(info.node_id());
  }
}

ModelExecutor::~ModelExecutor() {
  MetricRegistry::Singleton().RemoveMetric(req_counter_);
  MetricRegistry::Singleton().RemoveMetric(drop_counter_);
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
  return (dynamic_cast<SharePrefixModel*>(model_.get()) != nullptr);
}

bool ModelExecutor::IsTFShareModel() const {
#ifdef USE_TENSORFLOW
  return (dynamic_cast<TFShareModel*>(model_.get()) != nullptr);
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

uint64_t ModelExecutor::Execute(uint32_t batch) {
  std::shared_ptr<BatchTask> batch_task;
  int dequeue_cnt;
  if (batch == 0) {
    batch = model_->batch();
  }

  auto t1 = std::chrono::high_resolution_clock::now();
  std::tie(batch_task, dequeue_cnt) = GetBatchTask(batch);
  auto t2 = std::chrono::high_resolution_clock::now();

  int num_drops = dequeue_cnt - batch_task->batch_size();
  drop_counter_->Increase(num_drops);

  if (batch_task->batch_size() == 0) {
    DecreaseOpenRequests(dequeue_cnt);
    std::lock_guard<std::mutex> lock(time_mu_);
    last_exec_finish_ = t2;
    return std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1)
        .count();
  }

  uint64_t batch_id = batch_id_.fetch_add(1, std::memory_order_relaxed);
  batch_task->set_batch_id(batch_id);
  // Each time recompute output sizes because it might change for prefix model
  std::unordered_map<std::string, size_t> output_sizes;
  for (auto iter : model_->OutputShapes()) {
    output_sizes.emplace(iter.first, iter.second.NumElements(1));
  }
  batch_task->CreateOutputArrays(output_sizes,
                                 DeviceManager::Singleton().GetCPUDevice());
  model_->Forward(batch_task);
  auto t3 = std::chrono::high_resolution_clock::now();
  {
    std::lock_guard<std::mutex> lock(time_mu_);
    last_exec_finish_ = t3;
  }
  DecreaseOpenRequests(dequeue_cnt);

  auto memcpy_lat =
      std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
  auto forward_lat =
      std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2).count();
  VLOG(1) << model_->model_session_id() << " forwards batch "
          << batch_task->batch_id() << ", size " << batch_task->batch_size()
          << ", memcpy lat " << memcpy_lat << " us, forward lat " << forward_lat
          << " us, drop " << num_drops << " requests";

  auto outputs = batch_task->outputs();
  auto tasks = batch_task->tasks();
  // Add output to corresponding tasks, and remove tasks that get all outputs
  std::lock_guard<std::mutex> lock(task_mu_);
  for (int i = 0; i < outputs.size(); ++i) {
    auto output = outputs[i];
    auto task = tasks[i];
    if (task->AddOutput(output)) {
      RemoveTask(task);
    }
  }
  return memcpy_lat + forward_lat;
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

std::pair<std::shared_ptr<BatchTask>, int>
ModelExecutor::GetBatchTaskSlidingWindow(uint32_t expect_batch_size) {
  auto batch_task = std::make_shared<BatchTask>(model_->max_batch());
  batch_task->SetInputArray(input_array_);
  if (expect_batch_size > model_->max_batch()) {
    expect_batch_size = model_->max_batch();
  }
  if (expect_batch_size > input_queue_.size()) {
    expect_batch_size = input_queue_.size();
  }
  if (expect_batch_size == 0) {
    return {batch_task, 0};
  }

  std::lock_guard<std::mutex> lock(task_mu_);
  TimePoint now = Clock::now();
  TimePoint finish;
  if (profile_ != nullptr) {
    float latency = profile_->GetForwardLatency(expect_batch_size);
    finish = now + std::chrono::microseconds(int(latency));
  }
  int dequeue_cnt = 0;
  int current_batch = 0;
  std::unordered_map<std::string, std::vector<std::shared_ptr<Input> > >
      model_inputs;
  while (current_batch < expect_batch_size && !input_queue_.empty()) {
    auto input = std::move(input_queue_.top());
    input_queue_.pop();
    ++dequeue_cnt;
    auto task = processing_tasks_.at(input->task_id);
    task->timer.Record("exec");
    if (task->result.status() != CTRL_OK ||
        (profile_ != nullptr && input->deadline() < finish)) {
      VLOG(1) << model_->model_session_id() << " drops task " << task->task_id
              << "/" << input->index << ", waiting time "
              << task->timer.GetLatencyMicros("begin", "exec") << " us";
      if (task->AddVirtualOutput(input->index)) {
        RemoveTask(task);
      }
    } else {
      auto& model_sess_id = task->query.model_session_id();
      if (model_inputs.find(model_sess_id) == model_inputs.end()) {
        model_inputs.emplace(model_sess_id,
                             std::vector<std::shared_ptr<Input> >{});
      }
      model_inputs.at(model_sess_id).push_back(input);
      ++current_batch;
    }
    // Check whether there is enough requests left to fill the batch size
    int est_max_batch = current_batch + input_queue_.size();
    if (profile_ != nullptr && expect_batch_size > est_max_batch) {
      expect_batch_size = est_max_batch;
      if (expect_batch_size > 0) {
        float latency = profile_->GetForwardLatency(expect_batch_size);
        finish = now + std::chrono::microseconds(int(latency));
      }
    }
  }
  std::stringstream ss;
  for (auto const& iter : model_inputs) {
    for (auto input : iter.second) {
      auto task = processing_tasks_.at(input->task_id);
      batch_task->AppendInput(input, task);
      ss << task->task_id << " ";
    }
  }
  VLOG(1) << model_->model_session_id() << " batch size "
          << batch_task->batch_size() << ": " << ss.str();
  return {batch_task, dequeue_cnt};
}

std::pair<std::shared_ptr<BatchTask>, int> ModelExecutor::GetBatchTaskEarliest(
    uint32_t expect_batch_size) {
  auto batch_task = std::make_shared<BatchTask>(model_->max_batch());
  batch_task->SetInputArray(input_array_);
  if (expect_batch_size > model_->max_batch()) {
    expect_batch_size = model_->max_batch();
  }
  if (expect_batch_size > input_queue_.size()) {
    expect_batch_size = input_queue_.size();
  }
  if (expect_batch_size == 0) {
    return {batch_task, 0};
  }

  std::lock_guard<std::mutex> lock(task_mu_);
  CHECK(profile_ != nullptr);
  int dequeue_cnt = 0;

  // find the earliest deadline
  TimePoint now = Clock::now();
  TimePoint finish = now;
  finish += std::chrono::microseconds(
      static_cast<int>(profile_->GetForwardLatency(1)));
  finish += std::chrono::microseconds(
      static_cast<int>(profile_->GetPostprocessLatency()));
  while (!input_queue_.empty()) {
    auto& input = input_queue_.top();
    auto& task = processing_tasks_.at(input->task_id);
    if (task->result.status() != CTRL_OK || input->deadline() < finish) {
      task->timer.Record("exec");
      VLOG(1) << model_->model_session_id() << " drops task " << task->task_id
              << "/" << input->index << ", waiting time "
              << task->timer.GetLatencyMicros("begin", "exec") << " us";
      if (task->AddVirtualOutput(input->index)) {
        RemoveTask(task);
      }
      ++dequeue_cnt;
      input_queue_.pop();
    } else {
      finish = input->deadline();
      break;
    }
  }

  // calculate max batch size
  int budget =
      std::chrono::duration_cast<std::chrono::microseconds>(finish - now)
          .count();
  budget -= static_cast<int>(profile_->GetPostprocessLatency());
  uint32_t batch_size = 2;
  while (batch_size <= expect_batch_size &&
         profile_->GetForwardLatency(batch_size) < budget)
    ++batch_size;
  --batch_size;

  // gather inputs
  uint32_t current_batch = 0;
  std::unordered_map<std::string, std::vector<std::shared_ptr<Input> > >
      model_inputs;
  while (current_batch < batch_size && !input_queue_.empty()) {
    auto input = input_queue_.top();
    input_queue_.pop();
    ++dequeue_cnt;

    auto task = processing_tasks_.at(input->task_id);
    task->timer.Record("exec");
    auto& model_sess_id = task->query.model_session_id();
    if (model_inputs.find(model_sess_id) == model_inputs.end()) {
      model_inputs.emplace(model_sess_id,
                           std::vector<std::shared_ptr<Input> >{});
    }
    model_inputs.at(model_sess_id).push_back(input);
    ++current_batch;
  }

  std::stringstream ss;
  for (auto const& iter : model_inputs) {
    for (auto input : iter.second) {
      auto task = processing_tasks_.at(input->task_id);
      batch_task->AppendInput(input, task);
      ss << task->task_id << " ";
    }
  }
  VLOG(1) << model_->model_session_id() << " batch size "
          << batch_task->batch_size() << ": " << ss.str();
  return {batch_task, dequeue_cnt};
}

std::pair<std::shared_ptr<BatchTask>, int> ModelExecutor::GetBatchTask(
    uint32_t expect_batch_size) {
  switch (FLAGS_backend_batch_policy) {
    case 0:
      return GetBatchTaskSlidingWindow(expect_batch_size);
    case 1:
      return GetBatchTaskEarliest(expect_batch_size);
    default:
      LOG(FATAL) << "Unknown FLAGS_backend_batch_policy="
                 << FLAGS_backend_batch_policy;
  }
}

void ModelExecutor::RemoveTask(std::shared_ptr<Task> task) {
  task->stage = kPostprocess;
  task_queue_.push(task);
  processing_tasks_.erase(task->task_id);
}

void ModelExecutor::ExecuteBatchPlan(std::shared_ptr<BatchPlanContext> plan) {
  VLOG(1) << "ExecuteBatchPlan start. plan_id=" << plan->proto().plan_id();
  auto batch_task = GetBatchTaskByBatchPlan(plan);
  int dequeue_cnt = plan->proto().queries_without_input_size();
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
  auto tasks = batch_task->tasks();
  // Add output to corresponding tasks, and remove tasks that get all outputs
  for (int i = 0; i < outputs.size(); ++i) {
    auto output = outputs[i];
    auto task = tasks[i];
    if (task->AddOutput(output)) {
      task->stage = kPostprocess;
      task_queue_.push(task);
    }
  }
  VLOG(1) << "ExecuteBatchPlan finish. plan_id=" << plan->proto().plan_id();
}

std::shared_ptr<BatchTask> ModelExecutor::GetBatchTaskByBatchPlan(
    std::shared_ptr<BatchPlanContext> plan) {
  auto tasks = plan->PopPreprocessedTasks();
  auto batch_task = std::make_shared<BatchTask>(tasks.size());
  batch_task->SetInputArray(input_array_);
  for (const auto& task : tasks) {
    for (auto input : task->inputs) {
      batch_task->AppendInput(input, task);
    }
  }
  if (batch_task->batch_size() != plan->proto().queries_without_input_size()) {
    LOG(WARNING) << "Actual batch size differs from BatchPlan. plan_id="
                 << plan->proto().plan_id()
                 << ", actual=" << batch_task->batch_size()
                 << ", expected=" << plan->proto().queries_without_input_size();
  }
  return batch_task;
}

}  // namespace backend
}  // namespace nexus
