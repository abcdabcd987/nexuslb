#ifndef NEXUS_DISPATCHER_DISPATCHER_H_
#define NEXUS_DISPATCHER_DISPATCHER_H_

#include <cstdint>
#include <array>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "nexus/common/connection.h"
#include "nexus/common/server_base.h"
#include "nexus/dispatcher/rpc_service.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {

class ModelRoute {
 public:
  void Update(const ModelRouteProto& route);
  BackendInfo GetBackend();

 private:
  // Basic infomation from the proto
  std::string model_session_id_;
  std::vector<ModelRouteProto::BackendRate> backends_;
  double total_throughput_ = 0;

  // Members for deficit round robin
  std::unordered_map<uint32_t, double> backend_quanta_;
  double min_rate_ = 0;
  size_t current_drr_index_ = 0;
};

struct RequestContext {
  std::array<uint8_t, 1400> buf;
  size_t len;
  boost::asio::ip::udp::endpoint endpoint;
};

class Dispatcher;

class UdpRpcServer {
 public:
  UdpRpcServer(int udp_rpc_port, Dispatcher* dispatcher);
  ~UdpRpcServer();
  void Run();
  void Stop();

 private:
  void AsyncReceive();
  void WorkerThread();
  void HandleRequest(std::unique_ptr<RequestContext> ctx);

  const int udp_rpc_port_;
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
  Dispatcher(std::string rpc_port, std::string sch_addr, int udp_port,
             int num_udp_threads);

  virtual ~Dispatcher();

  void Run();

  void Stop();

  void UpdateModelRoutes(const ModelRouteUpdates& request, RpcReply* reply);

  void GetBackend(const std::string& model_sess_id, DispatchReply* reply);

 private:
  void Register();

  void Unregister();

  const int udp_port_;
  const int num_udp_threads_;

  /*! \brief Indicator whether the dispatcher is running */
  std::atomic_bool running_;
  /*! \brief Interval to update stats to scheduler in seconds */
  uint32_t beacon_interval_sec_;
  /*! \brief Frontend node ID */
  uint32_t node_id_;
  /*! \brief RPC service */
  RpcService rpc_service_;
  /*! \brief RPC client connected to scheduler */
  std::unique_ptr<SchedulerCtrl::Stub> sch_stub_;

  // Big lock for the model routes
  std::mutex mutex_;
  // Maps model session ID to backend list of the model
  std::unordered_map<std::string, ModelRoute> models_;

  // UDP RPC Server
  std::vector<std::thread> workers_;
  std::vector<std::unique_ptr<UdpRpcServer>> udp_rpc_servers_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
