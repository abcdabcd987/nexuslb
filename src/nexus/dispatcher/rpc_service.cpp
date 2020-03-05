#include "nexus/dispatcher/rpc_service.h"
#include "nexus/dispatcher/dispatcher.h"

namespace nexus {
namespace dispatcher {

INSTANTIATE_RPC_CALL(AsyncService, UpdateModelRoutes, ModelRouteUpdates,
                     RpcReply);
INSTANTIATE_RPC_CALL(AsyncService, CheckAlive, CheckAliveRequest, RpcReply);

RpcService::RpcService(Dispatcher* dispatcher, std::string port,
                       size_t nthreads)
    : AsyncRpcServiceBase(port, nthreads), dispatcher_(dispatcher) {}

void RpcService::HandleRpcs() {
  new UpdateModelRoutes_Call(
      &service_, cq_.get(),
      [this](const grpc::ServerContext&, const ModelRouteUpdates& req,
             RpcReply* reply) { dispatcher_->UpdateModelRoutes(req, reply); });
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

}  // namespace dispatcher
}  // namespace nexus
