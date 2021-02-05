#include "nexus/dispatcher/frontend_delegate.h"

#include <glog/logging.h>

#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {

FrontendDelegate::FrontendDelegate(uint32_t node_id, std::string ip,
                                   uint16_t port, int beacon_sec,
                                   ario::RdmaQueuePair* conn,
                                   RdmaSender rdma_sender)
    : node_id_(node_id),
      ip_(std::move(ip)),
      port_(port),
      beacon_sec_(beacon_sec),
      timeout_ms_(beacon_sec * 3 * 1000),
      conn_(conn),
      rdma_sender_(rdma_sender) {
  Tick();
}

void FrontendDelegate::Tick() { last_time_ = std::chrono::system_clock::now(); }

void FrontendDelegate::UpdateBackendList(BackendListUpdates&& request) {
  ControlMessage req;
  *req.mutable_update_backend_list() = std::move(request);
  rdma_sender_.SendMessage(conn_, req);
  Tick();
}

void FrontendDelegate::MarkQueryDroppedByDispatcher(DispatchReply&& request) {
  ControlMessage req;
  *req.mutable_dispatch_reply() = std::move(request);
  rdma_sender_.SendMessage(conn_, req);
  Tick();
}

}  // namespace dispatcher
}  // namespace nexus
