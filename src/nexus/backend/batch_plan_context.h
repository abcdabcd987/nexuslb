#ifndef NEXUS_BACKEND_BATCH_PLAN_CONTEXT_H_
#define NEXUS_BACKEND_BATCH_PLAN_CONTEXT_H_

#include <memory>
#include <unordered_set>
#include <vector>

#include "nexus/common/typedef.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace backend {

class Task;

class BatchPlanContext {
 public:
  explicit BatchPlanContext(BatchPlanProto proto);
  const BatchPlanProto& proto() const { return proto_; }
  PlanId plan_id() const { return plan_id_; }
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
  bool has_populated_ = false;
};

}  // namespace backend
}  // namespace nexus

#endif
