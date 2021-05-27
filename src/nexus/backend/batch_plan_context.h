#ifndef NEXUS_BACKEND_BATCH_PLAN_CONTEXT_H_
#define NEXUS_BACKEND_BATCH_PLAN_CONTEXT_H_

#include <memory>
#include <unordered_set>
#include <vector>

#include "nexus/backend/batch_task.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace backend {

class Task;

class BatchPlanContext {
 public:
  explicit BatchPlanContext(BatchPlanProto proto);
  ~BatchPlanContext();
  const BatchPlanProto& proto() const { return proto_; }
  PlanId plan_id() const { return plan_id_; }
  std::shared_ptr<BatchTask> batch_task() const { return batch_task_; }
  TimePoint enqueue_time() const { return enqueue_time_; }
  void set_enqueue_time(TimePoint enqueue_time) {
    enqueue_time_ = enqueue_time;
  }

  void SetInputArray(std::shared_ptr<Array> input_array);
  std::shared_ptr<Array> ReleaseInputArray();
  void MarkQueryDropped(GlobalId global_id);
  void AddPreprocessedTask(std::shared_ptr<Task> task);
  bool IsReadyToRun() const;
  std::vector<std::shared_ptr<Task>> PopPreprocessedTasks();

 private:
  bool MarkQueryProcessed(GlobalId global_id);

  BatchPlanProto proto_;
  PlanId plan_id_;
  std::unordered_set<GlobalId> global_ids_;
  std::unordered_set<GlobalId> pending_queries_;
  std::unordered_set<GlobalId> dropped_queries_;
  std::vector<std::shared_ptr<Task>> preprocessed_task_;
  std::shared_ptr<Array> input_array_;
  std::shared_ptr<BatchTask> batch_task_;
  bool has_populated_ = false;
  TimePoint enqueue_time_;
};

}  // namespace backend
}  // namespace nexus

#endif
