#ifndef NEXUS_BACKEND_BATCH_PLAN_CONTEXT_H_
#define NEXUS_BACKEND_BATCH_PLAN_CONTEXT_H_

#include <unordered_set>

#include "nexus/common/typedef.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace backend {

class BatchPlanContext {
 public:
  explicit BatchPlanContext(BatchPlanProto proto);
  const BatchPlanProto& proto() const { return proto_; }
  PlanId plan_id() const { return plan_id_; }
  void MarkQueryGotImage(GlobalId global_id);
  bool HaveAllGotImageYet() const;

 private:
  BatchPlanProto proto_;
  PlanId plan_id_;
  std::unordered_set<GlobalId> global_ids_;
  std::unordered_set<GlobalId> queries_pending_image_;
};

}  // namespace backend
}  // namespace nexus

#endif
