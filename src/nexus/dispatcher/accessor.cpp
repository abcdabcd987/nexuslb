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

UdpRpcServer* DispatcherAccessor::GetUdpRpcServer() {
  return dispatcher_.udp_rpc_servers_.front().get();
}

}  // namespace dispatcher
}  // namespace nexus
