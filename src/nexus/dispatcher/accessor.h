#ifndef NEXUS_DISPATCHER_ACCESSOR_H_
#define NEXUS_DISPATCHER_ACCESSOR_H_

#include <memory>

#include "nexus/common/typedef.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/dispatcher/frontend_delegate.h"

namespace nexus {
namespace dispatcher {

class DispatcherAccessor {
 public:
  virtual ~DispatcherAccessor() = default;
  virtual std::shared_ptr<BackendDelegate> GetBackend(NodeId backend_id) = 0;
  virtual std::shared_ptr<FrontendDelegate> GetFrontend(NodeId frontend_id) = 0;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
