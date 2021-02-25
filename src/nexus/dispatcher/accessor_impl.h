#ifndef NEXUS_DISPATCHER_ACCESSOR_IMPL_H_
#define NEXUS_DISPATCHER_ACCESSOR_IMPL_H_

#include <memory>

#include "nexus/common/typedef.h"
#include "nexus/dispatcher/accessor.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/dispatcher/frontend_delegate.h"

namespace nexus {
namespace dispatcher {

class Dispatcher;

class DispatcherAccessorImpl : public DispatcherAccessor {
 public:
  std::shared_ptr<BackendDelegate> GetBackend(NodeId backend_id) override;
  std::shared_ptr<FrontendDelegate> GetFrontend(NodeId frontend_id) override;

 private:
  friend class Dispatcher;
  explicit DispatcherAccessorImpl(Dispatcher& dispatcher);
  Dispatcher& dispatcher_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
