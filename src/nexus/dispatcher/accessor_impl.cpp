#include "nexus/dispatcher/accessor_impl.h"

#include <mutex>

#include "nexus/dispatcher/dispatcher.h"

namespace nexus {
namespace dispatcher {

DispatcherAccessorImpl::DispatcherAccessorImpl(Dispatcher& dispatcher)
    : dispatcher_(dispatcher) {}

std::shared_ptr<BackendDelegate> DispatcherAccessorImpl::GetBackend(
    NodeId backend_id) {
  std::lock_guard<std::mutex> lock(dispatcher_.mutex_);
  auto iter = dispatcher_.backends_.find(backend_id);
  if (iter != dispatcher_.backends_.end()) {
    return iter->second;
  } else {
    return nullptr;
  }
}

std::shared_ptr<FrontendDelegate> DispatcherAccessorImpl::GetFrontend(
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
