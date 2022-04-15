#include "nexus_scheduler/frontend_delegate.h"

#include "nexus_scheduler/fake_nexus_frontend.h"

namespace nexus {
namespace scheduler {

FrontendDelegate::FrontendDelegate(const FakeObjectAccessor* accessor,
                                   uint32_t node_id)
    : accessor_(*accessor), node_id_(node_id) {}

void FrontendDelegate::UpdateModelRoutesRpc(const ModelRouteUpdates& request) {
  auto* frontend = accessor_.frontends.at(node_id_);
  frontend->UpdateModelRoutes(request);
}

}  // namespace scheduler
}  // namespace nexus
