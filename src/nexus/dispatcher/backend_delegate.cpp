#include "nexus/dispatcher/backend_delegate.h"

#include <glog/logging.h>
#include <grpc++/grpc++.h>

#include <sstream>

namespace nexus {
namespace dispatcher {

BackendDelegate::BackendDelegate(uint32_t node_id, std::string ip,
                                 uint16_t port, std::string gpu_device,
                                 std::string gpu_uuid,
                                 size_t gpu_available_memory, int beacon_sec,
                                 ario::RdmaQueuePair* conn,
                                 RdmaSender rdma_sender)
    : node_id_(node_id),
      ip_(std::move(ip)),
      port_(port),
      gpu_device_(std::move(gpu_device)),
      gpu_uuid_(std::move(gpu_uuid)),
      gpu_available_memory_(gpu_available_memory),
      beacon_sec_(beacon_sec),
      timeout_ms_(beacon_sec * 3 * 1000),
      conn_(conn),
      rdma_sender_(rdma_sender) {
  Tick();

  backend_info_.set_node_id(node_id_);
  *backend_info_.mutable_ip() = ip_;
  backend_info_.set_port(port_);
}

void BackendDelegate::Tick() { last_time_ = std::chrono::system_clock::now(); }

void BackendDelegate::SendLoadModelCommand(const ModelSession& model_session,
                                           uint32_t max_batch) {
  BackendRequest req;
  auto* request = req.mutable_load_model();
  *request->mutable_model_session() = model_session;
  request->set_max_batch(max_batch);
  rdma_sender_.SendMessage(conn_, req);
  Tick();
}

void BackendDelegate::EnqueueBatchPlan(BatchPlanProto&& request) {
  BackendRequest req;
  *req.mutable_enqueue_batchplan() = std::move(request);
  rdma_sender_.SendMessage(conn_, req);
  Tick();
}

}  // namespace dispatcher
}  // namespace nexus
