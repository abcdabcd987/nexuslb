#include "nexus/app/dispatcher_rpc_client.h"

#include <cstdio>
#include <chrono>
#include <mutex>
#include <tuple>
#include <utility>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "nexus/common/config.h"
#include "nexus/common/model_def.h"

// ========== For debugging purpose BEGIN ==========
DEFINE_string(_debug_record_dispatcher_rpc_latency, "",
              "DEBUG: Save the latency of each Dispatcher RPC to the path "
              "specified by this flag.");

namespace {
class RpcLatencyRecorder {
 public:
  explicit RpcLatencyRecorder(const std::string& path) {
    LOG(INFO) << "Logging latency of Dispatcher RPC calls to " << path;
    file_ = fopen(path.c_str(), "wb");
    CHECK(file_ != nullptr) << "Failed to open " << path;
  }

  ~RpcLatencyRecorder() { fclose(file_); }

  void RecordDispatcherRpcLatency(uint32_t latency_ns) {
    auto n = fwrite(&latency_ns, sizeof(latency_ns), 1, file_);
    CHECK_EQ(n, 1);
  }

 private:
  FILE* file_ = nullptr;
};
}  // namespace

class nexus::app::DispatcherRpcClient::Debug {
 public:
  void RecordDispatcherRpcLatency(uint32_t latency_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    rpc_latency_recorder_->RecordDispatcherRpcLatency(latency_ns);
  }

  static std::unique_ptr<Debug> NewIfEnabled() {
    std::unique_ptr<Debug> debug(new Debug());
    bool enabled = false;
    if (!FLAGS__debug_record_dispatcher_rpc_latency.empty()) {
      enabled = true;
      debug->rpc_latency_recorder_.reset(
          new RpcLatencyRecorder(FLAGS__debug_record_dispatcher_rpc_latency));
    }
    if (!enabled) {
      delete debug.release();
    }
    return debug;
  }

 private:
  Debug() = default;
  std::mutex mutex_;
  std::unique_ptr<RpcLatencyRecorder> rpc_latency_recorder_;
};

// ========== For debugging purpose END ==========

using boost::asio::ip::udp;

namespace nexus {
namespace app {

DispatcherRpcClient::DispatcherRpcClient(boost::asio::io_context* io_context,
                                         std::string dispatcher_addr)
    : io_context_(io_context),
      dispatcher_addr_(std::move(dispatcher_addr)),
      tx_socket_(*io_context_),
      rx_socket_(*io_context_),
      debug_(Debug::NewIfEnabled()) {}

DispatcherRpcClient::~DispatcherRpcClient() {
  if (running_) {
    Stop();
  }
}

void DispatcherRpcClient::Start() {
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
  running_ = true;
  rx_thread_ = std::thread(&DispatcherRpcClient::RxThread, this);
  LOG(INFO) << "Dispatcher RPC client is sending from "
            << tx_socket_.local_endpoint().address().to_string() << ":"
            << tx_socket_.local_endpoint().port() << " to "
            << dispatcher_endpoint_.address().to_string() << ":"
            << dispatcher_endpoint_.port() << " and receiving from "
            << rx_socket_.local_endpoint().address().to_string() << ":"
            << rx_socket_.local_endpoint().port();
}

void DispatcherRpcClient::Stop() {
  running_ = false;
  // TODO let unique_ptr delete it, once the rx_thread can join.
  delete debug_.release();
  rx_thread_.join();
}

void DispatcherRpcClient::RxThread() {
  uint8_t buf[1400];
  udp::endpoint remote_endpoint;
  DispatchReply reply;
  while (running_) {
    // Receive response
    size_t len = rx_socket_.receive_from(boost::asio::buffer(buf, 1400),
                                         remote_endpoint);

    // Validate response
    reply.Clear();
    bool ok = reply.ParseFromString(std::string(buf, buf + len));
    if (!ok) {
      LOG(ERROR) << "Bad response. Failed to ParseFromString. Total length = "
                 << len;
      continue;
    }

    // Wake up worker
    do {
      std::lock_guard<std::mutex> lock(mutex_);
      auto iter = pending_responses_.find(reply.request_id());
      if (iter == pending_responses_.end()) {
        LOG(WARNING) << "Received unexpected response. request_id: "
                     << reply.request_id() << ", model_session: "
                     << ModelSessionToModelID(reply.model_session());
        break;
      }
      auto& pending_response = iter->second;
      {
        std::lock_guard<std::mutex> response_lock(pending_response.mutex);
        pending_response.reply = std::move(reply);
        pending_response.ready = true;
      }
      pending_response.cv.notify_one();
    } while (false);
  }
}

DispatchReply DispatcherRpcClient::Query(ModelSession model_session) {
  DispatchRequest request;
  *request.mutable_model_session() = std::move(model_session);
  request.set_udp_rpc_port(rx_port_);

  // Add to the pending list
  UdpRpcPendingResponse* response = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    request.set_request_id(++next_request_id_);
    auto res = pending_responses_.emplace(std::piecewise_construct,
                                          std::make_tuple(request.request_id()),
                                          std::make_tuple());
    response = &res.first->second;
  }

  // Send request
  auto start = std::chrono::high_resolution_clock::now();
  auto request_msg = request.SerializeAsString();
  if (request_msg.size() > 1400) {
    LOG(WARNING) << "UDP RPC client request size is too big. Size = "
                 << request_msg.size();
  }
  size_t sent_bytes = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    sent_bytes = tx_socket_.send_to(boost::asio::buffer(request_msg),
                                    dispatcher_endpoint_);
  }
  if (sent_bytes != request_msg.size()) {
    LOG(WARNING) << "UDP RPC client request sent " << sent_bytes
                 << " bytes, expecting " << request_msg.size() << " bytes";
  }

  // Wait for response
  bool response_ready = false;
  {
    auto timeout = std::chrono::microseconds(2000);
    std::unique_lock<std::mutex> response_lock(response->mutex);
    response_ready = response->cv.wait_for(
        response_lock, timeout, [response] { return response->ready; });
  }
  if (debug_) {
    auto end = std::chrono::high_resolution_clock::now();
    uint32_t latency_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count();
    debug_->RecordDispatcherRpcLatency(latency_ns);
  }

  // Remove from pending list
  DispatchReply reply;
  reply.set_request_id(request.request_id());
  std::lock_guard<std::mutex> lock(mutex_);
  auto iter = pending_responses_.find(request.request_id());
  if (iter == pending_responses_.end()) {
    LOG(ERROR) << "Cannot find pending response. request_id: "
               << request.request_id();
    reply.set_status(CtrlStatus::INPUT_TYPE_INCORRECT);
  } else {
    if (response_ready) {
      reply = std::move(iter->second.reply);
    } else {
      reply.set_status(CtrlStatus::TIMEOUT);
    }
    pending_responses_.erase(iter);
  }
  return reply;
}

}  // namespace app
}  // namespace nexus
