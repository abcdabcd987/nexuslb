#include "nexus/dispatcher/backend_delegate.h"

#include <glog/logging.h>
#include <grpc++/grpc++.h>

#include <sstream>

namespace nexus {
namespace dispatcher {

BackendDelegate::BackendDelegate(uint32_t node_id, const std::string& ip,
                                 const std::string& server_port,
                                 const std::string& rpc_port,
                                 const std::string& gpu_device,
                                 const std::string& gpu_uuid,
                                 size_t gpu_available_memory, int beacon_sec)
    : node_id_(node_id),
      ip_(ip),
      server_port_(server_port),
      rpc_port_(rpc_port),
      gpu_device_(gpu_device),
      gpu_uuid_(gpu_uuid),
      gpu_available_memory_(gpu_available_memory),
      beacon_sec_(beacon_sec),
      timeout_ms_(beacon_sec * 3 * 1000) {
  std::stringstream rpc_addr;
  rpc_addr << ip_ << ":" << rpc_port_;
  auto channel =
      grpc::CreateChannel(rpc_addr.str(), grpc::InsecureChannelCredentials());
  stub_ = BackendCtrl::NewStub(channel);
  last_time_ = std::chrono::system_clock::now();

  backend_info_.set_node_id(node_id_);
  *backend_info_.mutable_ip() = ip_;
  *backend_info_.mutable_server_port() = server_port_;
  *backend_info_.mutable_rpc_port() = rpc_port_;
}

void BackendDelegate::Tick() { last_time_ = std::chrono::system_clock::now(); }

bool BackendDelegate::IsAlive() {
  auto elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now() - last_time_)
                    .count();
  if (elapse < timeout_ms_) {
    return true;
  }
  CheckAliveRequest request;
  RpcReply reply;
  request.set_node_type(BACKEND_NODE);
  request.set_node_id(node_id_);

  // Invoke CheckAlive RPC
  grpc::ClientContext context;
  grpc::Status status = stub_->CheckAlive(&context, request, &reply);
  if (!status.ok()) {
    LOG(ERROR) << status.error_code() << ": " << status.error_message();
    return false;
  }
  last_time_ = std::chrono::system_clock::now();
  return true;
}

void BackendDelegate::SendLoadModelCommand(const ModelSession& model_session,
                                           uint32_t max_batch) {
  grpc::ClientContext context;
  BackendLoadModelCommand request;
  *request.mutable_model_session() = model_session;
  request.set_max_batch(max_batch);
  RpcReply reply;
  grpc::Status status = stub_->LoadModel(&context, request, &reply);
  if (!status.ok()) {
    LOG(ERROR) << "SendLoadModelCommand error " << status.error_code() << ": "
               << status.error_message();
  }
  last_time_ = std::chrono::system_clock::now();
}

void BackendDelegate::EnqueueBatchPlan(const BatchPlanProto& req) {
  // TODO: replace this communication channel. It's on critical path.
  grpc::ClientContext context;
  RpcReply reply;
  grpc::Status status = stub_->EnqueueBatchPlan(&context, req, &reply);
  if (!status.ok()) {
    LOG(ERROR) << "EnqueueBatchPlan error " << status.error_code() << ": "
               << status.error_message();
  }
  if (reply.status() != CtrlStatus::CTRL_OK) {
    LOG(ERROR) << "EnqueueBatchPlan error at Backend " << node_id_ << " ("
               << ip_ << "), plan_id=" << req.plan_id()
               << ", status=" << CtrlStatus_Name(reply.status());
  }
  last_time_ = std::chrono::system_clock::now();
}

}  // namespace dispatcher
}  // namespace nexus
