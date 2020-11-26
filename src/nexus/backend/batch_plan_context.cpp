#include "nexus/backend/batch_plan_context.h"

#include <glog/logging.h>

#include "nexus/backend/task.h"

namespace nexus {
namespace backend {

BatchPlanContext::BatchPlanContext(BatchPlanProto proto)
    : proto_(std::move(proto)), plan_id_(proto.plan_id()) {
  for (const auto& query : proto_.queries_without_input()) {
    auto global_id = GlobalId(query.query_id());
    auto pair = global_ids_.insert(global_id);
    if (!pair.second) {
      LOG(ERROR) << "Duplicated query in BatchPlan. global_id=" << global_id.t
                 << ", plan_id=" << plan_id_.t;
    }
  }
  pending_queries_ = global_ids_;
}

bool BatchPlanContext::MarkQueryProcessed(GlobalId global_id) {
  if (!global_ids_.count(global_id)) {
    LOG(ERROR) << "Query not belong to BatchPlan. global_id=" << global_id.t
               << ", plan_id=" << plan_id_.t;
    return false;
  }
  auto iter = pending_queries_.find(global_id);
  if (iter == pending_queries_.end()) {
    LOG(ERROR) << "Query is not in pending state. global_id=" << global_id.t
               << ", plan_id=" << plan_id_.t;
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
  preprocessed_task_.push_back(std::move(task));
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
