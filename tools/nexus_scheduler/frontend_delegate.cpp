#include "nexus_scheduler/frontend_delegate.h"

#include <sstream>

#include "nexus_scheduler/scheduler.h"

namespace nexus {
namespace scheduler {

FrontendDelegate::FrontendDelegate(uint32_t node_id) : node_id_(node_id) {}

void FrontendDelegate::SubscribeModel(const std::string& model_session_id) {
  subscribe_models_.insert(model_session_id);
}

void FrontendDelegate::UpdateModelRoutesRpc(const ModelRouteUpdates& request) {
  // Do nothing.
}

}  // namespace scheduler
}  // namespace nexus
