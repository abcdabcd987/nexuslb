#include "nexus/dispatcher/backend_delegate_impl.h"

#include <glog/logging.h>

namespace nexus {
namespace dispatcher {

BackendDelegateImpl::BackendDelegateImpl(
    NodeId backend_id, std::string ip, uint16_t port, std::vector<GpuInfo> gpus,
    int beacon_sec, ario::RdmaQueuePair* conn, RdmaSender rdma_sender)
    : BackendDelegate(backend_id, std::move(gpus)),
      ip_(std::move(ip)),
      port_(port),
      beacon_sec_(beacon_sec),
      timeout_ms_(beacon_sec * 3 * 1000),
      conn_(conn),
      rdma_sender_(rdma_sender) {
  Tick();

  backend_info_.set_node_id(backend_id.t);
  *backend_info_.mutable_ip() = ip_;
  backend_info_.set_port(port_);
}

void BackendDelegateImpl::Tick() {
  last_time_ = std::chrono::system_clock::now();
}

void BackendDelegateImpl::SendLoadModelCommand(
    uint32_t gpu_idx, const ModelSession& model_session, uint32_t max_batch,
    ModelIndex model_index) {
  ControlMessage req;
  auto* request = req.mutable_load_model();
  request->set_gpu_idx(gpu_idx);
  *request->mutable_model_session() = model_session;
  request->set_max_batch(max_batch);
  request->set_model_index(model_index.t);
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
