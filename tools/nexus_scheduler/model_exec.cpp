#include "nexus_scheduler/model_exec.h"

#include <glog/logging.h>

#include <sstream>

#include "nexus/common/model_db.h"

namespace nexus {
namespace backend {

ModelExecutor::ModelExecutor(int gpu_id, const ModelInstanceConfig& config,
                             BlockPriorityQueue<Task>& task_queue)
    : task_queue_(task_queue), batch_id_(0) {}

ModelExecutor::~ModelExecutor() {}

bool ModelExecutor::Preprocess(std::shared_ptr<Task> task, bool force) {
  std::lock_guard<std::mutex> lock(task_mu_);
  processing_tasks_.emplace(task->task_id, task);
  for (auto input : task->inputs) {
    input_queue_.push(input);
  }
  return true;
}

bool ModelExecutor::AddPreprocessedTask(std::shared_ptr<Task> task,
                                        bool force) {
  std::lock_guard<std::mutex> lock(task_mu_);
  processing_tasks_.emplace(task->task_id, task);
  for (auto input : task->inputs) {
    input_queue_.push(input);
  }
  return true;
}

uint64_t ModelExecutor::Execute(uint32_t batch) {
  std::shared_ptr<BatchTask> batch_task;
  int dequeue_cnt;
  if (batch == 0) {
    batch = batch_;
  }

  auto t1 = std::chrono::high_resolution_clock::now();
  std::tie(batch_task, dequeue_cnt) = GetBatchTask(batch);
  auto t2 = std::chrono::high_resolution_clock::now();

  int num_drops = dequeue_cnt - batch_task->batch_size();

  if (batch_task->batch_size() == 0) {
    return std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1)
        .count();
  }

  uint64_t batch_id = batch_id_.fetch_add(1, std::memory_order_relaxed);
  batch_task->set_batch_id(batch_id);
  model_->Forward(batch_task);
  auto t3 = std::chrono::high_resolution_clock::now();

  auto memcpy_lat =
      std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
  auto forward_lat =
      std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2).count();

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

std::pair<std::shared_ptr<BatchTask>, int>
ModelExecutor::GetBatchTaskSlidingWindow(uint32_t expect_batch_size) {
  auto batch_task = std::make_shared<BatchTask>(999);
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
  return {batch_task, dequeue_cnt};
}

std::pair<std::shared_ptr<BatchTask>, int> ModelExecutor::GetBatchTask(
    uint32_t expect_batch_size) {
  return GetBatchTaskSlidingWindow(expect_batch_size);
}

void ModelExecutor::RemoveTask(std::shared_ptr<Task> task) {
  task->stage = kPostprocess;
  task_queue_.push(task);
  processing_tasks_.erase(task->task_id);
}

}  // namespace backend
}  // namespace nexus
