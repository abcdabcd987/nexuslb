#ifndef NEXUS_SCHEDULER_FRONTEND_DELEGATE_H_
#define NEXUS_SCHEDULER_FRONTEND_DELEGATE_H_

#include "nexus/proto/nexus.pb.h"
#include "nexus_scheduler/fake_object_accessor.h"

namespace nexus {
namespace scheduler {

class FrontendDelegate {
 public:
  FrontendDelegate(const FakeObjectAccessor* accessor, uint32_t node_id);

  uint32_t node_id() const { return node_id_; }

  void UpdateModelRoutesRpc(const ModelRouteUpdates& request);

 private:
  const FakeObjectAccessor& accessor_;
  uint32_t node_id_;
};

}  // namespace scheduler
}  // namespace nexus

#endif  // NEXUS_SCHEDULER_FRONTEND_DELEGATE_H_
