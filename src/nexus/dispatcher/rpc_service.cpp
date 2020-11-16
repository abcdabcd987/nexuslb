#include "nexus/dispatcher/rpc_service.h"

#include "nexus/dispatcher/dispatcher.h"

namespace nexus {
namespace dispatcher {

INSTANTIATE_RPC_CALL(AsyncService, Register, RegisterRequest, RegisterReply);
INSTANTIATE_RPC_CALL(AsyncService, Unregister, UnregisterRequest, RpcReply);
INSTANTIATE_RPC_CALL(AsyncService, LoadModel, LoadModelRequest, LoadModelReply);
INSTANTIATE_RPC_CALL(AsyncService, KeepAlive, KeepAliveRequest, RpcReply);

RpcService::RpcService(Dispatcher* dispatcher, std::string port,
                       size_t nthreads)
    : AsyncRpcServiceBase(port, nthreads), dispatcher_(dispatcher) {}

void RpcService::HandleRpcs() {
  using namespace std::placeholders;
  new Register_Call(
      &service_, cq_.get(),
      std::bind(&Dispatcher::HandleRegister, dispatcher_, _1, _2, _3));
  new Unregister_Call(
      &service_, cq_.get(),
      std::bind(&Dispatcher::HandleUnregister, dispatcher_, _1, _2, _3));
  new LoadModel_Call(
      &service_, cq_.get(),
      std::bind(&Dispatcher::HandleLoadModel, dispatcher_, _1, _2, _3));
  new KeepAlive_Call(
      &service_, cq_.get(),
      std::bind(&Dispatcher::HandleKeepAlive, dispatcher_, _1, _2, _3));
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
