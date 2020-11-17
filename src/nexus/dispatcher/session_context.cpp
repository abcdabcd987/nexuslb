#include "nexus/dispatcher/session_context.h"

#include "nexus/dispatcher/inst_info.h"

namespace nexus {
namespace dispatcher {

ModelSessionContext::ModelSessionContext(ModelSession model_session)
    : model_session_(std::move(model_session)) {}

std::shared_ptr<InstanceInfo> ModelSessionContext::GetInstanceInfo(
    uint32_t backend_id) const {
  return instances_.at(backend_id);
}

void ModelSessionContext::AddInstanceInfo(uint32_t backend_id,
                                          std::shared_ptr<InstanceInfo> inst) {
  instances_[backend_id] = inst;
}

}  // namespace dispatcher
}  // namespace nexus
