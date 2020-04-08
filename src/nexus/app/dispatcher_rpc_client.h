#ifndef NEXUS_APP_DISPATCHER_RPC_CLIENT_H_
#define NEXUS_APP_DISPATCHER_RPC_CLIENT_H_

#include <atomic>
#include <boost/asio.hpp>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "nexus/proto/control.pb.h"

namespace nexus {
namespace app {

class ModelHandler;

class DispatcherRpcClient {
 public:
  DispatcherRpcClient(boost::asio::io_context* io_context,
                      std::string dispatcher_addr, uint32_t rpc_timeout_us);
  ~DispatcherRpcClient();
  void Start();
  void Stop();
  void AsyncQuery(ModelSession model_session, uint64_t query_id,
                  ModelHandler* model_handler);

 private:
  void DoReceive();

  struct UdpRpcPendingResponse {
    ModelHandler* model_handler;
  };

  std::atomic<bool> running_{false};
  boost::asio::io_context* const io_context_;
  const std::string dispatcher_addr_;
  const uint32_t rpc_timeout_us_;
  boost::asio::ip::udp::endpoint dispatcher_endpoint_;
  boost::asio::ip::udp::socket tx_socket_;
  boost::asio::ip::udp::socket rx_socket_;
  uint32_t rx_port_ = 0;
  boost::asio::ip::udp::endpoint rx_endpoint_;
  uint8_t rx_buf_[1400];

  std::mutex mutex_;
  std::unordered_map<uint64_t, UdpRpcPendingResponse> pending_responses_;
};

}  // namespace app
}  // namespace nexus

#endif