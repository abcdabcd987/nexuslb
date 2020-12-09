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

#include "nexus/common/connection.h"
#include "nexus/common/server_base.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/delayed_scheduler.h"
#include "nexus/dispatcher/rpc_service.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {

class Dispatcher;
class FrontendDelegate;
class BackendDelegate;
class ModelSessionContext;

struct RequestContext {
  std::array<uint8_t, 1400> buf;
  size_t len;
  boost::asio::ip::udp::endpoint endpoint;
};

class UdpRpcServer {
 public:
  UdpRpcServer(int udp_rpc_port, Dispatcher* dispatcher, int rx_cpu,
               int worker_cpu);
  ~UdpRpcServer();
  void Run();
  void Stop();
  void SendDispatchReply(boost::asio::ip::udp::endpoint endpoint,
                         const DispatchReply& reply);

 private:
  void AsyncReceive();
  void WorkerThread();
  void HandleRequest(std::unique_ptr<RequestContext> ctx);

  const int udp_rpc_port_;
  const int rx_cpu_;
  const int worker_cpu_;
  // TODO: refactor
  Dispatcher* const dispatcher_;

  boost::asio::io_context io_context_;
  boost::asio::ip::udp::socket rx_socket_;
  boost::asio::ip::udp::socket tx_socket_;
  std::thread worker_thread_;
  std::atomic<bool> running_{false};

  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::deque<std::unique_ptr<RequestContext>> queue_;
  std::unique_ptr<RequestContext> incoming_request_;
  // TODO: memory pool maybe?
};

class Dispatcher {
 public:
  Dispatcher(std::string rpc_port, int udp_port, int num_udp_threads,
             std::vector<int> pin_cpus);

  virtual ~Dispatcher();

  void Run();

  void Stop();

  CtrlStatus DispatchRequest(QueryProto query_without_input,
                             boost::asio::ip::udp::endpoint frontend_endpoint);

  // gRPC handlers

  void HandleRegister(const grpc::ServerContext& ctx,
                      const RegisterRequest& request, RegisterReply* reply);
  void HandleUnregister(const grpc::ServerContext& ctx,
                        const UnregisterRequest& request, RpcReply* reply);
  void HandleLoadModel(const grpc::ServerContext& ctx,
                       const LoadModelRequest& request, LoadModelReply* reply);
  void HandleKeepAlive(const grpc::ServerContext& ctx,
                       const KeepAliveRequest& request, RpcReply* reply);

 private:
  friend class DispatcherAccessor;
  const int udp_port_;
  const int num_udp_threads_;
  const std::vector<int> pin_cpus_;

  /*! \brief Indicator whether the dispatcher is running */
  std::atomic_bool running_;
  /*! \brief Interval to update stats to scheduler in seconds */
  uint32_t beacon_interval_sec_;
  /*! \brief Frontend node ID */
  NodeId node_id_;
  /*! \brief RPC service */
  RpcService rpc_service_;
  /*! \brief Mapping from frontend node id to frontend client */
  std::unordered_map<NodeId, std::shared_ptr<FrontendDelegate>>
      frontends_ /* GUARDED_BY(mutex_) */;
  /*! \brief Mapping from backend node id to backend client */
  std::unordered_map<NodeId, std::shared_ptr<BackendDelegate>>
      backends_ /* GUARDED_BY(mutex_) */;
  /*! \brief Mapping from model session ID to session information */
  std::unordered_map<std::string, std::shared_ptr<ModelSessionContext>>
      sessions_ /* GUARDED_BY(mutex_) */;

  // Big lock
  std::mutex mutex_;
  // Maps model session ID to backend list of the model
  std::atomic<uint64_t> next_global_id_{1};

  delayed::DelayedScheduler scheduler_;
  std::vector<std::thread> scheduler_threads_;

  // UDP RPC Server
  std::vector<std::thread> workers_;
  std::vector<std::unique_ptr<UdpRpcServer>> udp_rpc_servers_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
