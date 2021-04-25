#ifndef NEXUS_APP_FRONTEND_H_
#define NEXUS_APP_FRONTEND_H_

#include <atomic>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <unordered_map>
#include <unordered_set>

#include "ario/ario.h"
#include "nexus/app/model_handler.h"
#include "nexus/app/query_processor.h"
#include "nexus/app/request_context.h"
#include "nexus/app/user_session.h"
#include "nexus/app/worker.h"
#include "nexus/common/backend_pool.h"
#include "nexus/common/block_queue.h"
#include "nexus/common/connection.h"
#include "nexus/common/model_def.h"
#include "nexus/common/rdma_sender.h"
#include "nexus/common/server_base.h"
#include "nexus/common/spinlock.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace app {

class Frontend : public ServerBase, public MessageHandler {
 public:
  Frontend(ario::PollerType poller_type, std::string rdma_dev,
           uint16_t rdma_tcp_server_port, std::string nexus_server_port,
           std::string sch_addr);

  virtual ~Frontend();

  // virtual void Process(const RequestProto& request, ReplyProto* reply) = 0;

  NodeId node_id() const { return node_id_; }

  void Run(QueryProcessor* qp, size_t nthreads);

  void Stop() override;
  /*! \brief Accepts new user connection */
  void HandleAccept() final;
  /*!
   * \brief Handles new messages from user or backend connections
   * \param conn Shared pointer of Connection
   * \param message Received message
   */
  void HandleMessage(std::shared_ptr<Connection> conn,
                     std::shared_ptr<Message> message) final;
  /*!
   * \brief Handles connection error
   * \param conn Shared pointer of Connection
   * \param ec Boost error code
   */
  void HandleError(std::shared_ptr<Connection> conn,
                   boost::system::error_code ec) final;

  void HandleConnected(std::shared_ptr<Connection> conn) override;

  std::shared_ptr<UserSession> GetUserSession(uint32_t uid);

 protected:
  std::shared_ptr<ModelHandler> LoadModel(LoadModelRequest req);

  std::shared_ptr<ModelHandler> LoadModel(LoadModelRequest req,
                                          LoadBalancePolicy lb_policy);

  void ComplexQuerySetup(const ComplexQuerySetupRequest& req);

  void ComplexQueryAddEdge(const ComplexQueryAddEdgeRequest& req);

 private:
  void Register();

  void Unregister();

  void RegisterUser(std::shared_ptr<UserSession> user_sess,
                    const RequestProto& request, ReplyProto* reply);

  void Daemon();

  void ReportWorkload(const WorkloadStatsProto& request);

  // ControlMessage handlers
  void UpdateBackendList(const BackendListUpdates& request);
  void HandleDispatcherReply(const DispatchReply& request);
  void HandleQueryResult(const QueryResultProto& result);

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
    friend class Frontend;
    explicit RdmaHandler(Frontend& outer);
    Frontend& outer_;
  };

 private:
  std::string dispatcher_ip_;
  uint16_t rdma_tcp_server_port_;

  ario::EpollExecutor executor_;
  RdmaHandler rdma_handler_;
  ario::MemoryBlockAllocator small_buffers_;
  ario::MemoryBlockAllocator large_buffers_;
  ario::RdmaManager rdma_;
  RdmaSender rdma_sender_;
  std::thread rdma_ev_thread_;
  ario::RdmaQueuePair* dispatcher_conn_ = nullptr;
  uint16_t model_worker_port_ = 0;
  ario::RdmaQueuePair* model_worker_conn_ = nullptr;

  // Ugly promises because ario doesn't have RPC
  std::promise<ario::RdmaQueuePair*> promise_dispatcher_conn_;
  std::promise<RegisterReply> promise_register_reply_;
  std::promise<RpcReply> promise_unregister_reply_;
  std::promise<LoadModelReply> promise_add_model_reply_;
  std::promise<ario::RdmaQueuePair*> promise_model_worker_conn_;

  /*! \brief Indicator whether backend is running */
  std::atomic_bool running_;
  /*! \brief Interval to update stats to scheduler in seconds */
  uint32_t beacon_interval_sec_;
  /*! \brief Frontend node ID */
  NodeId node_id_;
  /*! \brief Backend pool */
  BackendPool backend_pool_;
  /*!
   * \brief Map from backend ID to model sessions servered at this backend.
   * Guarded by backend_sessions_mu_
   */
  std::unordered_map<uint32_t, std::unordered_set<std::string> >
      backend_sessions_;
  std::mutex backend_sessions_mu_;
  /*! \brief Request pool */
  RequestPool request_pool_;
  /*! \brief Worker pool for processing requests */
  std::vector<std::unique_ptr<Worker> > workers_;
  /*! \brief User connection pool. Guarded by user_mutex_. */
  std::unordered_set<std::shared_ptr<Connection> > connection_pool_;
  /*! \brief Map from user id to user session. Guarded by user_mutex_. */
  std::unordered_map<uint32_t, std::shared_ptr<UserSession> > user_sessions_;
  /*!
   * \brief Map from model session ID to model handler.
   */
  std::unordered_map<std::string, std::shared_ptr<ModelHandler> > model_pool_;

  // for UpdateBackendList
  std::mutex connecting_backends_mutex_;
  std::unordered_map<std::string, BackendInfo> connecting_backends_;

  std::thread daemon_thread_;
  /*! \brief Mutex for connection_pool_ and user_sessions_ */
  std::mutex user_mutex_;

  /*! \brief Random number generator */
  std::random_device rd_;
  std::mt19937 rand_gen_;
};

}  // namespace app
}  // namespace nexus

#endif  // NEXUS_APP_APP_BASE_H_
