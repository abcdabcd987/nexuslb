#ifndef NEXUS_APP_DISPATCHER_RPC_CLIENT_H_
#define NEXUS_APP_DISPATCHER_RPC_CLIENT_H_

#include <atomic>
#include <cstdint>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <boost/asio.hpp>

#include "nexus/proto/control.pb.h"

namespace nexus {
namespace app {

class DispatcherRpcClient {
 public:
  DispatcherRpcClient(boost::asio::io_context* io_context,
                      std::string dispatcher_addr,
                      uint32_t rpc_timeout_us);
  ~DispatcherRpcClient();
  void Start();
  void Stop();
  DispatchReply Query(ModelSession model_session);

 private:
  void DoReceive();

  struct UdpRpcPendingResponse {
    std::mutex mutex;
    std::condition_variable cv;
    bool ready = false;
    DispatchReply reply;
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
  uint64_t next_request_id_ = 0;
  std::unordered_map<uint64_t, UdpRpcPendingResponse> pending_responses_;

  // For debugging purpose:
  class Debug;
  std::unique_ptr<Debug> debug_;
};

}  // namespace app
}  // namespace nexus

#endif