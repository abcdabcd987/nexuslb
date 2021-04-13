#ifndef NEXUS_DISPATCHER_DISPATCHER_H_
#define NEXUS_DISPATCHER_DISPATCHER_H_

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "ario/epoll.h"
#include "nexus/common/connection.h"
#include "nexus/common/rdma_sender.h"
#include "nexus/common/server_base.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/global_id_issuer.h"
#include "nexus/dispatcher/model_worker.h"
#include "nexus/dispatcher/rankmt_scheduler.h"
#include "nexus/dispatcher/scheduler.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {

class FrontendDelegate;
class BackendDelegate;
class ModelSessionContext;

class Dispatcher {
 public:
  Dispatcher(std::string rdma_dev, uint16_t port, std::vector<int> pin_cpus);
  virtual ~Dispatcher();

  void Run();
  void Stop();

 private:
  class RdmaHandler : public ario::RdmaEventHandler {
   public:
    void OnConnected(ario::RdmaQueuePair* conn) override;
    void OnRemoteMemoryRegionReceived(ario::RdmaQueuePair* conn, uint64_t addr,
                                      size_t size) override;
    void OnRdmaReadComplete(ario::RdmaQueuePair* conn, ario::WorkRequestID wrid,
                            ario::OwnedMemoryBlock buf) override;
    void OnRecv(ario::RdmaQueuePair* conn, ario::OwnedMemoryBlock buf) override;
    void OnSent(ario::RdmaQueuePair* conn, ario::OwnedMemoryBlock buf) override;
    void OnError(ario::RdmaQueuePair* conn, ario::RdmaError error) override;

   private:
    friend class Dispatcher;
    explicit RdmaHandler(Dispatcher& outer);
    Dispatcher& outer_;
  };

  void HandleRegister(ario::RdmaQueuePair* conn, const RegisterRequest& request,
                      RegisterReply* reply);
  void HandleUnregister(const UnregisterRequest& request, RpcReply* reply);
  void HandleLoadModel(const LoadModelRequest& request, LoadModelReply* reply);
  void HandleInformAlive(const KeepAliveRequest& request);

  ModelWorker& GetModelWorker(const ModelSession& model_session) const;

  std::string rdma_dev_;
  uint16_t tcp_server_port_;
  std::vector<int> pin_cpus_;

  ario::EpollExecutor main_executor_;
  RdmaHandler rdma_handler_;
  ario::MemoryBlockAllocator small_buffers_;
  ario::RdmaManager rdma_;
  RdmaSender rdma_sender_;

  /*! \brief Indicator whether the dispatcher is running */
  std::atomic_bool running_;
  /*! \brief Interval to update stats to scheduler in seconds */
  uint32_t beacon_interval_sec_;
  /*! \brief Frontend node ID */
  NodeId node_id_;
  GlobalIdIssuer global_id_issuer_;
  /*! \brief Mapping from frontend node id to frontend client */
  std::unordered_map<NodeId, std::shared_ptr<FrontendDelegate>> frontends_;
  /*! \brief Mapping from backend node id to backend client */
  std::unordered_map<NodeId, std::shared_ptr<BackendDelegate>> backends_;
  /*! \brief Mapping from model session ID to session information */
  std::unordered_map<std::string, std::shared_ptr<ModelSessionContext>>
      sessions_;

  // Scheduler
  ario::EpollExecutor rank_thread_executor_;
  MultiThreadRankScheduler scheduler_;
  std::thread rank_thread_;
  std::vector<std::unique_ptr<ModelWorker>> model_workers_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
