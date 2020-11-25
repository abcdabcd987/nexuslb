#include "nexus/backend/batch_plan_context.h"

#include <glog/logging.h>

namespace nexus {
namespace backend {

BatchPlanContext::BatchPlanContext(BatchPlanProto proto)
    : proto_(std::move(proto)), plan_id_(proto.plan_id()) {
  for (const auto& query : proto_.queries_without_input()) {
    global_ids_.insert(GlobalId(query.query_id()));
  }
  queries_pending_image_ = global_ids_;
}

void BatchPlanContext::MarkQueryGotImage(GlobalId global_id) {
  if (!global_ids_.count(global_id)) {
    LOG(ERROR) << "GlobalId " << global_id.t << " not in Plan " << plan_id_.t;
    return;
  }
  auto iter = queries_pending_image_.find(global_id);
  if (iter == queries_pending_image_.end()) {
    LOG(ERROR) << "GlobalId " << global_id.t << " has already marked GotImage.";
    return;
  }
  queries_pending_image_.erase(iter);
}

bool BatchPlanContext::HaveAllGotImageYet() const {
  return queries_pending_image_.empty();
}

}  // namespace backend
}  // namespace nexus
