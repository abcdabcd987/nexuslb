#include "nexus/app/dispatcher_rpc_client.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdio>
#include <mutex>
#include <tuple>
#include <utility>

#include "nexus/app/model_handler.h"
#include "nexus/common/config.h"
#include "nexus/common/model_def.h"

using boost::asio::ip::udp;

namespace nexus {
namespace app {

DispatcherRpcClient::DispatcherRpcClient(boost::asio::io_context* io_context,
                                         std::string dispatcher_addr,
                                         uint32_t rpc_timeout_us)
    : io_context_(io_context),
      dispatcher_addr_(std::move(dispatcher_addr)),
      rpc_timeout_us_(rpc_timeout_us),
      timer_interval_ns_(static_cast<uint64_t>(rpc_timeout_us_) * 1000 / 10),
      tx_socket_(*io_context_),
      rx_socket_(*io_context_),
      timeout_timer_(*io_context_) {}

DispatcherRpcClient::~DispatcherRpcClient() {
  if (running_) {
    Stop();
  }
}

void DispatcherRpcClient::Start() {
  CHECK_NE(timer_interval_ns_, 0);

  // Resolve dispatcher server address
  CHECK(!dispatcher_addr_.empty()) << "Dispatcher address is empty.";
  std::string addr, port;
  auto colon_idx = dispatcher_addr_.find(':');
  if (colon_idx == std::string::npos) {
    addr = dispatcher_addr_;
    port = std::to_string(DISPATCHER_RPC_DEFAULT_PORT);
  } else {
    addr = dispatcher_addr_.substr(0, colon_idx);
    port = dispatcher_addr_.substr(colon_idx + 1);
  }
  udp::resolver resolver(*io_context_);
  auto resolve_result = resolver.resolve(udp::v4(), addr, port);
  CHECK(!resolve_result.empty()) << "Failed to resolve dispatcher address";
  dispatcher_endpoint_ = *resolve_result.begin();

  // Start tx/rx socket on the client sdie
  tx_socket_.open(udp::v4());
  rx_socket_.open(udp::v4());
  tx_socket_.bind(udp::endpoint(udp::v4(), 0));
  rx_socket_.bind(udp::endpoint(udp::v4(), 0));
  rx_port_ = rx_socket_.local_endpoint().port();
  LOG(INFO) << "Dispatcher RPC client is sending from "
            << tx_socket_.local_endpoint().address().to_string() << ":"
            << tx_socket_.local_endpoint().port() << " to "
            << dispatcher_endpoint_.address().to_string() << ":"
            << dispatcher_endpoint_.port() << " and receiving from "
            << rx_socket_.local_endpoint().address().to_string() << ":"
            << rx_socket_.local_endpoint().port();

  // Set up async tasks
  running_ = true;
  DoReceive();
  SetTimeoutTimer();
}

void DispatcherRpcClient::Stop() { running_ = false; }

void DispatcherRpcClient::DoReceive() {
  if (!running_) {
    return;
  }
  rx_socket_.async_receive_from(
      boost::asio::buffer(rx_buf_, 1400), rx_endpoint_,
      [this](boost::system::error_code ec, std::size_t len) {
        // Set up async task
        DoReceive();

        // Validate response
        DispatchReply reply;
        bool ok = reply.ParseFromString(std::string(rx_buf_, rx_buf_ + len));
        if (!ok) {
          LOG(ERROR)
              << "Bad response. Failed to ParseFromString. Total length = "
              << len;
          return;
        }

        // Get ModelHandler and remove from pending_responses_
        ModelHandler* model_handler = nullptr;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          auto iter = pending_responses_.find(reply.query_id());
          if (iter == pending_responses_.end()) {
            // Ignore timed-out requests.
            return;
          }
          auto& pending_response = iter->second;
          model_handler = pending_response.model_handler;
          pending_responses_.erase(iter);
        }

        // Handle the dispatcher RPC
        CHECK(model_handler != nullptr);
        model_handler->HandleDispatcherReply(reply);
      });
}

void DispatcherRpcClient::AsyncQuery(ModelSession model_session,
                                     uint64_t query_id,
                                     ModelHandler* model_handler) {
  CHECK(model_handler != nullptr);
  DispatchRequest request;
  *request.mutable_model_session() = std::move(model_session);
  request.set_udp_rpc_port(rx_port_);

  // Add to the pending list
  UdpRpcPendingResponse* response = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    request.set_query_id(query_id);
    auto res = pending_responses_.emplace(std::piecewise_construct,
                                          std::make_tuple(request.query_id()),
                                          std::make_tuple());
    response = &res.first->second;
  }
  response->model_handler = model_handler;

  // Send request
  auto buf = request.SerializeAsString();
  auto buf_len = buf.size();
  if (buf_len > 1400) {
    LOG(WARNING) << "UDP RPC client request size is too big. Size = " << buf_len
                 << ". message: " << request.DebugString();
  }
  tx_socket_.async_send_to(
      boost::asio::buffer(std::move(buf)), dispatcher_endpoint_,
      [this, buf_len](boost::system::error_code ec, std::size_t len) {
        if (ec && ec != boost::asio::error::operation_aborted) {
          LOG(WARNING) << "Error when sending to Dispatcher: " << ec;
        }
        if (len != buf_len) {
          LOG(WARNING) << "UDP RPC client request sent " << len
                       << " bytes, expecting " << buf_len << " bytes";
        }
      });

  // Set timeout timer
  auto now = std::chrono::steady_clock::now();
  auto deadline = now + std::chrono::milliseconds(rpc_timeout_us_);
  auto ctx = TimerContext{query_id, deadline};
  {
    std::lock_guard<std::mutex> lock(mutex_);
    timer_queue_.push_back(ctx);
  }
}

void DispatcherRpcClient::SetTimeoutTimer() {
  if (!running_) {
    return;
  }
  timeout_timer_.expires_after(std::chrono::nanoseconds(timer_interval_ns_));
  timeout_timer_.async_wait([this](boost::system::error_code) {
    struct QueueItem {
      uint64_t query_id;
      ModelHandler* model_handler;
    };
    static std::vector<QueueItem> timeout_requests;
    static DispatchReply reply;

    // Find all timeout requests.
    auto now = std::chrono::steady_clock::now();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      while (!timer_queue_.empty()) {
        const auto& ctx = timer_queue_.front();
        if (ctx.deadline < now) {
          break;
        }
        auto iter = pending_responses_.find(ctx.query_id);
        if (iter != pending_responses_.end()) {
          QueueItem item;
          item.query_id = ctx.query_id;
          item.model_handler = iter->second.model_handler;
          timeout_requests.push_back(item);
          pending_responses_.erase(iter);
        }
        timer_queue_.pop_front();
      }
    }

    // Handle the timeout
    for (const auto& ctx : timeout_requests) {
      reply.set_query_id(ctx.query_id);
      reply.set_status(CtrlStatus::TIMEOUT);
      ctx.model_handler->HandleDispatcherReply(reply);
    }
    timeout_requests.clear();

    // Set up async task
    SetTimeoutTimer();
  });
}

}  // namespace app
}  // namespace nexus
