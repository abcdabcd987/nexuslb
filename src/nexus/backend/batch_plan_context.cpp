#include "nexus/backend/batch_plan_context.h"

#include <glog/logging.h>

#include "nexus/backend/task.h"

namespace nexus {
namespace backend {

BatchPlanContext::BatchPlanContext(BatchPlanProto proto)
    : proto_(std::move(proto)), plan_id_(proto_.plan_id()) {
  for (const auto& query : proto_.queries()) {
    auto global_id = GlobalId(query.query_without_input().global_id());
    auto pair = global_ids_.insert(global_id);
    if (!pair.second) {
      LOG(ERROR) << "Duplicated query in BatchPlan. global_id=" << global_id.t
                 << ", plan_id=" << plan_id_.t;
    }
  }
  pending_queries_ = global_ids_;
  batch_task_ = std::make_shared<BatchTask>(proto_.queries_size());
  preprocessed_task_.reserve(proto_.queries_size());

  stats_.set_plan_id(proto_.plan_id());
  stats_.set_batch_size(proto_.queries_size());
  stats_.set_deadline_ns(proto_.deadline_ns());
  stats_.set_expected_exec_ns(proto_.exec_time_ns());
  stats_.set_expected_finish_ns(proto_.expected_finish_time_ns());
  const auto& qclock = proto_.queries(0).query_without_input().clock();
  stats_.set_dispatcher_dispatch_ns(qclock.dispatcher_dispatch_ns());
}

BatchPlanContext::~BatchPlanContext() {
  CHECK(input_array_ == nullptr) << "input_array_ was not released";
}

void BatchPlanContext::SetInputArray(std::shared_ptr<Array> input_array) {
  CHECK(input_array_ == nullptr);
  input_array_ = std::move(input_array);
  batch_task_->SetInputArray(input_array_);
}

void BatchPlanContext::SetPinnedMemory(std::shared_ptr<Array> pinned_memory) {
  CHECK(pinned_memory_ == nullptr);
  pinned_memory_ = std::move(pinned_memory);
}

std::shared_ptr<Array> BatchPlanContext::ReleaseInputArray() {
  CHECK(input_array_ != nullptr);
  auto ret = input_array_;
  input_array_ = nullptr;
  return ret;
}

std::shared_ptr<Array> BatchPlanContext::ReleasePinnedMemory() {
  CHECK(pinned_memory_ != nullptr);
  auto ret = pinned_memory_;
  pinned_memory_ = nullptr;
  return ret;
}

bool BatchPlanContext::MarkQueryProcessed(GlobalId global_id) {
  if (!global_ids_.count(global_id)) {
    LOG(ERROR)
        << "MarkQueryProcessed: Query not belong to BatchPlan. global_id="
        << global_id.t << ", plan_id=" << plan_id_.t;
    return false;
  }
  auto iter = pending_queries_.find(global_id);
  if (iter == pending_queries_.end()) {
    LOG(ERROR)
        << "MarkQueryProcessed: Query is not in pending state. global_id="
        << global_id.t << ", plan_id=" << plan_id_.t;
    return false;
  }
  pending_queries_.erase(iter);
  return true;
}

void BatchPlanContext::MarkQueryDropped(GlobalId global_id) {
  if (!MarkQueryProcessed(global_id)) {
    return;
  }
  dropped_queries_.insert(global_id);
}

void BatchPlanContext::AddPreprocessedTask(std::shared_ptr<Task> task) {
  auto global_id = GlobalId(task->query.global_id());
  if (!MarkQueryProcessed(global_id)) {
    return;
  }
  preprocessed_task_.push_back(task);

  // Memory copy
  CHECK(input_array_ != nullptr) << "input_array_ was not set";
  for (auto& input : task->inputs) {
    // TODO: instead of appending, use the index inside batchplan.
    batch_task_->AppendInput(input, task);
  }
}

bool BatchPlanContext::IsReadyToRun() const { return pending_queries_.empty(); }

std::vector<std::shared_ptr<Task>> BatchPlanContext::PopPreprocessedTasks() {
  CHECK_EQ(has_populated_, false)
      << "BatchPlan populated before. plan_id=" << plan_id_.t;
  has_populated_ = true;

  std::vector<std::shared_ptr<Task>> res;
  res.swap(preprocessed_task_);
  return res;
}

}  // namespace backend
}  // namespace nexus
