#ifndef NEXUS_DISPATCHER_RPC_SERVICE_H_
#define NEXUS_DISPATCHER_RPC_SERVICE_H_

#include <grpc++/grpc++.h>

#include "nexus/common/rpc_call.h"
#include "nexus/common/rpc_service_base.h"
#include "nexus/proto/control.grpc.pb.h"

namespace nexus {
namespace dispatcher {

using AsyncService = nexus::FrontendCtrl::AsyncService;

class Dispatcher;

class RpcService : public AsyncRpcServiceBase<AsyncService> {
 public:
  RpcService(Dispatcher* dispatcher, std::string port, size_t nthreads);

 protected:
  void HandleRpcs() final;

 private:
  Dispatcher* dispatcher_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
