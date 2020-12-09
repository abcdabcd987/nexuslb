#include "nexus/dispatcher/accessor.h"

#include <mutex>

#include "nexus/dispatcher/dispatcher.h"

namespace nexus {
namespace dispatcher {

DispatcherAccessor::DispatcherAccessor(Dispatcher& dispatcher)
    : dispatcher_(dispatcher) {}

std::shared_ptr<BackendDelegate> DispatcherAccessor::GetBackend(
    NodeId backend_id) {
  std::lock_guard<std::mutex> lock(dispatcher_.mutex_);
  auto iter = dispatcher_.backends_.find(backend_id);
  if (iter != dispatcher_.backends_.end()) {
    return iter->second;
  } else {
    return nullptr;
  }
}

std::shared_ptr<FrontendDelegate> DispatcherAccessor::GetFrontend(
    NodeId frontend_id) {
  std::lock_guard<std::mutex> lock(dispatcher_.mutex_);
  auto iter = dispatcher_.frontends_.find(frontend_id);
  if (iter != dispatcher_.frontends_.end()) {
    return iter->second;
  } else {
    return nullptr;
  }
}

}  // namespace dispatcher
}  // namespace nexus
