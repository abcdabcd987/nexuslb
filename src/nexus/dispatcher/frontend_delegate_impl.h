#ifndef NEXUS_DISPATCHER_FRONTEND_DELEGATE_IMPL_H_
#define NEXUS_DISPATCHER_FRONTEND_DELEGATE_IMPL_H_

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>

#include "ario/ario.h"
#include "nexus/common/rdma_sender.h"
#include "nexus/dispatcher/frontend_delegate.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {

class FrontendDelegateImpl : public FrontendDelegate {
 public:
  FrontendDelegateImpl(uint32_t node_id, std::string ip, uint16_t port,
                       int beacon_sec, ario::RdmaQueuePair* conn,
                       RdmaSender rdma_sender);

  void Tick() override;
  void UpdateBackendList(BackendListUpdates&& request) override;
  void MarkQueryDroppedByDispatcher(DispatchReply&& request) override;

 private:
  std::string ip_;
  uint16_t port_;
  int beacon_sec_;
  long timeout_ms_;
  ario::RdmaQueuePair* conn_;
  RdmaSender rdma_sender_;
  std::chrono::time_point<std::chrono::system_clock> last_time_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
