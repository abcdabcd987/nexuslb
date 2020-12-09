#ifndef NEXUS_DISPATCHER_ACCESSOR_H_
#define NEXUS_DISPATCHER_ACCESSOR_H_

#include <memory>

#include "nexus/common/typedef.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/dispatcher/frontend_delegate.h"

namespace nexus {
namespace dispatcher {

class Dispatcher;
class UdpRpcServer;

class DispatcherAccessor {
 public:
  std::shared_ptr<BackendDelegate> GetBackend(NodeId backend_id);
  std::shared_ptr<FrontendDelegate> GetFrontend(NodeId frontend_id);

 private:
  friend class Dispatcher;
  explicit DispatcherAccessor(Dispatcher& dispatcher);
  Dispatcher& dispatcher_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
