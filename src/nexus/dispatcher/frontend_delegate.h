#ifndef NEXUS_DISPATCHER_FRONTEND_DELEGATE_H_
#define NEXUS_DISPATCHER_FRONTEND_DELEGATE_H_

#include <cstdint>
#include <cstdlib>

#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {

class FrontendDelegate {
 public:
  explicit FrontendDelegate(uint32_t node_id) : node_id_(node_id) {}
  uint32_t node_id() const { return node_id_; }

  virtual ~FrontendDelegate() = default;
  virtual void Tick() = 0;
  virtual void UpdateBackendList(BackendListUpdates&& request) = 0;
  virtual void MarkQueriesDroppedByDispatcher(DispatchReply&& request) = 0;

 protected:
  uint32_t node_id_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
