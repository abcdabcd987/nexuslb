#include "nexus/app/rpc_service.h"

#include "nexus/app/frontend.h"

namespace nexus {
namespace app {

INSTANTIATE_RPC_CALL(AsyncService, UpdateBackendList, BackendListUpdates,
                     RpcReply);
INSTANTIATE_RPC_CALL(AsyncService, CheckAlive, CheckAliveRequest, RpcReply);

RpcService::RpcService(Frontend* frontend, std::string port, size_t nthreads)
    : AsyncRpcServiceBase(port, nthreads), frontend_(frontend) {}

void RpcService::HandleRpcs() {
  new UpdateBackendList_Call(
      &service_, cq_.get(),
      [this](const grpc::ServerContext&, const BackendListUpdates& req,
             RpcReply* reply) { frontend_->UpdateBackendList(req, reply); });
  new CheckAlive_Call(&service_, cq_.get(),
                      [](const grpc::ServerContext&, const CheckAliveRequest&,
                         RpcReply* reply) { reply->set_status(CTRL_OK); });
  void* tag;
  bool ok;
  while (running_) {
    cq_->Next(&tag, &ok);
    if (ok) {
      static_cast<RpcCallBase*>(tag)->Proceed();
    }
  }
}

}  // namespace app
}  // namespace nexus
