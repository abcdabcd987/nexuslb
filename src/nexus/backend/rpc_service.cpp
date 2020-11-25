#include "nexus/backend/rpc_service.h"

#include <gflags/gflags.h>

#include <future>

#include "nexus/backend/backend_server.h"
#include "nexus/common/rpc_call.h"

DECLARE_int32(occupancy_valid);

namespace nexus {
namespace backend {

INSTANTIATE_RPC_CALL(AsyncService, LoadModel, BackendLoadModelCommand,
                     RpcReply);
INSTANTIATE_RPC_CALL(AsyncService, EnqueueQuery, EnqueueQueryCommand, RpcReply);
INSTANTIATE_RPC_CALL(AsyncService, EnqueueBatchPlan, BatchPlanProto, RpcReply);
INSTANTIATE_RPC_CALL(AsyncService, CheckAlive, CheckAliveRequest, RpcReply);

BackendRpcService::BackendRpcService(BackendServer* backend, std::string port,
                                     size_t nthreads)
    : AsyncRpcServiceBase(port, nthreads), backend_(backend) {}

void BackendRpcService::HandleRpcs() {
  new LoadModel_Call(
      &service_, cq_.get(),
      [this](const grpc::ServerContext&, const BackendLoadModelCommand& req,
             RpcReply* reply) {
        backend_->LoadModelEnqueue(req);
        reply->set_status(CTRL_OK);
      });
  new EnqueueQuery_Call(
      &service_, cq_.get(),
      [this](const grpc::ServerContext& ctx, const EnqueueQueryCommand& req,
             RpcReply* reply) {
        backend_->HandleEnqueueQuery(ctx, req, reply);
      });
  new EnqueueBatchPlan_Call(&service_, cq_.get(),
                            [this](const grpc::ServerContext& ctx,
                                   const BatchPlanProto& req, RpcReply* reply) {
                              backend_->HandleEnqueueBatchPlan(ctx, req, reply);
                            });
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

}  // namespace backend
}  // namespace nexus
