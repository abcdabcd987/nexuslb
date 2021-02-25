#include "nexus/dispatcher/backend_delegate_impl.h"

#include <glog/logging.h>

namespace nexus {
namespace dispatcher {

BackendDelegateImpl::BackendDelegateImpl(
    uint32_t node_id, std::string ip, uint16_t port, std::string gpu_device,
    std::string gpu_uuid, size_t gpu_available_memory, int beacon_sec,
    ario::RdmaQueuePair* conn, RdmaSender rdma_sender)
    : BackendDelegate(node_id, std::move(gpu_device), std::move(gpu_uuid),
                      gpu_available_memory),
      ip_(std::move(ip)),
      port_(port),
      beacon_sec_(beacon_sec),
      timeout_ms_(beacon_sec * 3 * 1000),
      conn_(conn),
      rdma_sender_(rdma_sender) {
  Tick();

  backend_info_.set_node_id(node_id_);
  *backend_info_.mutable_ip() = ip_;
  backend_info_.set_port(port_);
}

void BackendDelegateImpl::Tick() {
  last_time_ = std::chrono::system_clock::now();
}

void BackendDelegateImpl::SendLoadModelCommand(
    const ModelSession& model_session, uint32_t max_batch) {
  ControlMessage req;
  auto* request = req.mutable_load_model();
  *request->mutable_model_session() = model_session;
  request->set_max_batch(max_batch);
  rdma_sender_.SendMessage(conn_, req);
  Tick();
}

void BackendDelegateImpl::EnqueueBatchPlan(BatchPlanProto&& request) {
  ControlMessage req;
  *req.mutable_enqueue_batchplan() = std::move(request);
  rdma_sender_.SendMessage(conn_, req);
  Tick();
}

}  // namespace dispatcher
}  // namespace nexus
