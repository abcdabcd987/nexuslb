#include "nexus/dispatcher/frontend_delegate.h"

#include <glog/logging.h>
#include <grpc++/grpc++.h>

#include <sstream>

namespace nexus {
namespace dispatcher {

FrontendDelegate::FrontendDelegate(uint32_t node_id, const std::string& ip,
                                   const std::string& server_port,
                                   const std::string& rpc_port, int beacon_sec)
    : node_id_(node_id),
      ip_(ip),
      server_port_(server_port),
      rpc_port_(rpc_port),
      beacon_sec_(beacon_sec),
      timeout_ms_(beacon_sec * 3 * 1000) {
  std::stringstream rpc_addr;
  rpc_addr << ip_ << ":" << rpc_port_;
  auto channel =
      grpc::CreateChannel(rpc_addr.str(), grpc::InsecureChannelCredentials());
  stub_ = FrontendCtrl::NewStub(channel);
  last_time_ = std::chrono::system_clock::now();
}

void FrontendDelegate::Tick() { last_time_ = std::chrono::system_clock::now(); }

bool FrontendDelegate::IsAlive() {
  long elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now() - last_time_)
                    .count();
  if (elapse < timeout_ms_) {
    return true;
  }
  CheckAliveRequest request;
  request.set_node_type(FRONTEND_NODE);
  request.set_node_id(node_id_);
  RpcReply reply;

  // Inovke RPC CheckAlive
  grpc::ClientContext context;
  grpc::Status status = stub_->CheckAlive(&context, request, &reply);
  if (!status.ok()) {
    LOG(ERROR) << status.error_code() << ": " << status.error_message();
    return false;
  }
  last_time_ = std::chrono::system_clock::now();
  return true;
}

void FrontendDelegate::UpdateBackendList(const BackendListUpdates& request) {
  RpcReply reply;
  grpc::ClientContext context;
  grpc::Status status = stub_->UpdateBackendList(&context, request, &reply);
  if (!status.ok()) {
    LOG(ERROR) << status.error_code() << ": " << status.error_message();
    return;
  }
  last_time_ = std::chrono::system_clock::now();
}

}  // namespace dispatcher
}  // namespace nexus
