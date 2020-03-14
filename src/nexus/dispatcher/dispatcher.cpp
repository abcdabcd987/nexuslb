#include "nexus/dispatcher/dispatcher.h"

#include <sys/socket.h>

#include <algorithm>
#include <boost/asio.hpp>

#include "nexus/common/config.h"
#include "nexus/common/model_def.h"

using boost::asio::ip::udp;

namespace nexus {
namespace dispatcher {

UdpRpcServer::UdpRpcServer(int udp_rpc_port, Dispatcher* dispatcher)
    : udp_rpc_port_(udp_rpc_port),
      dispatcher_(dispatcher),
      rx_socket_(io_context_),
      tx_socket_(io_context_) {}

UdpRpcServer::~UdpRpcServer() {
  if (running_) {
    LOG(WARNING) << "Calling Stop() in ~UdpRpcServer()";
    Stop();
  }
}

void UdpRpcServer::Run() {
  rx_socket_.open(udp::v4());
#ifdef SO_REUSEPORT
  typedef boost::asio::detail::socket_option::boolean<SOL_SOCKET, SO_REUSEPORT>
      reuse_port;
  rx_socket_.set_option(reuse_port(true));
#endif
  rx_socket_.bind(udp::endpoint(udp::v4(), udp_rpc_port_));
  tx_socket_.open(udp::v4());
  tx_socket_.bind(udp::endpoint(udp::v4(), 0));
  LOG(INFO) << "UDP RPC server is listening on " << rx_socket_.local_endpoint()
            << " and sending from " << tx_socket_.local_endpoint();

  running_ = true;
  worker_thread_ = std::thread(&UdpRpcServer::WorkerThread, this);
  incoming_request_.reset(new RequestContext);
  AsyncReceive();

  // Block until done
  io_context_.run();
}

void UdpRpcServer::Stop() {
  running_ = false;
  io_context_.stop();
  rx_socket_.cancel();
  tx_socket_.cancel();
  worker_thread_.join();
}

void UdpRpcServer::AsyncReceive() {
  rx_socket_.async_receive_from(
      boost::asio::buffer(incoming_request_->buf), incoming_request_->endpoint,
      [this](boost::system::error_code ec, size_t len) {
        if (ec == boost::asio::error::operation_aborted) {
          return;
        }
        if (ec || !len) {
          AsyncReceive();
          return;
        }
        incoming_request_->len = len;
        {
          std::unique_lock<std::mutex> lock(queue_mutex_);
          queue_.emplace_back(std::move(incoming_request_));
          queue_cv_.notify_one();
        }
        incoming_request_.reset(new RequestContext);
        AsyncReceive();
      });
}

void UdpRpcServer::WorkerThread() {
  std::deque<std::unique_ptr<RequestContext>> q;
  while (running_) {
    // Move requests from the global queue to the local queue
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      if (queue_.empty()) {
        // Wait on the CV only when the queue is empty.
        // Hopefully this could reduce the times of context switching.
        queue_cv_.wait(lock, [this] { return !queue_.empty(); });
      }
      while (!queue_.empty()) {
        auto request = std::move(queue_.front());
        queue_.pop_front();
        q.emplace_back(std::move(request));
      }
    }

    // Handle requests
    while (!q.empty()) {
      HandleRequest(std::move(q.front()));
      q.pop_front();
    }
  }
}

namespace {

int ns(const std::chrono::time_point<std::chrono::high_resolution_clock>& x,
       const std::chrono::time_point<std::chrono::high_resolution_clock>& y) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(y - x).count();
}

}  // namespace

void UdpRpcServer::HandleRequest(std::unique_ptr<RequestContext> ctx) {
  auto t1 = std::chrono::high_resolution_clock::now();
  DispatchRequest request;
  // Validate request
  bool ok = request.ParseFromString(
      std::string(ctx->buf.data(), ctx->buf.data() + ctx->len));
  if (!ok) {
    LOG_EVERY_N(ERROR, 128)
        << "Bad request. Failed to ParseFromString. Total length = "
        << ctx->len;
    return;
  }
  auto t2 = std::chrono::high_resolution_clock::now();
  auto client_endpoint = boost::asio::ip::udp::endpoint(ctx->endpoint.address(),
                                                        request.udp_rpc_port());

  // Handle request
  DispatchReply reply;
  *reply.mutable_model_session() = request.model_session();
  reply.set_request_id(request.request_id());
  auto t3 = std::chrono::high_resolution_clock::now();
  std::string model_sess_id = ModelSessionToString(request.model_session());
  auto t4 = std::chrono::high_resolution_clock::now();
  dispatcher_->GetBackend(model_sess_id, &reply);
  auto t5 = std::chrono::high_resolution_clock::now();

  // Send reply. I think using blocking APIs should be okay here?
  auto msg = reply.SerializeAsString();
  auto t6 = std::chrono::high_resolution_clock::now();
  if (msg.empty()) {
    LOG(ERROR) << "Failed to reply.SerializeAsString()";
    return;
  }

  auto len = tx_socket_.send_to(boost::asio::buffer(msg), client_endpoint);
  if (len != msg.size()) {
    LOG(WARNING) << "UDP RPC server reply sent " << len << " bytes, expecting "
                 << msg.size() << " bytes";
  }

  auto t7 = std::chrono::high_resolution_clock::now();
  if (request.request_id() % 1024 == 0) {
    VLOG(1) << "t2: " << ns(t1, t2) << ", "
            << "t3: " << ns(t2, t3) << ", "
            << "t4: " << ns(t3, t4) << ", "
            << "t5: " << ns(t4, t5) << ", "
            << "t6: " << ns(t5, t6) << ", "
            << "t7: " << ns(t6, t7) << ", "
            << "total: " << ns(t1, t7) << " ns";
  }
}

Dispatcher::Dispatcher(std::string rpc_port, std::string sch_addr, int udp_port,
                       int num_udp_threads)
    : udp_port_(udp_port),
      num_udp_threads_(num_udp_threads),
      rpc_service_(this, rpc_port, 1) {
#ifndef SO_REUSEPORT
  CHECK_EQ(num_udp_threads, 1) << "SO_REUSEPORT is not supported. UDP RPC "
                                  "server must be run in single threaded mode.";
#endif

  // Init scheduler client
  if (sch_addr.find(':') == std::string::npos) {
    // Add default scheduler port if no port specified
    sch_addr += ":" + std::to_string(SCHEDULER_DEFAULT_PORT);
  }
  auto channel =
      grpc::CreateChannel(sch_addr, grpc::InsecureChannelCredentials());
  sch_stub_ = SchedulerCtrl::NewStub(channel);
}

Dispatcher::~Dispatcher() {
  if (running_) {
    Stop();
  }
}

void Dispatcher::Run() {
  running_ = true;

  // Start RPC service
  rpc_service_.Start();
  // Init Node ID and register frontend to scheduler
  Register();

  // Run UDP RPC server
  for (int i = 0; i < num_udp_threads_; ++i) {
    udp_rpc_servers_.emplace_back(new UdpRpcServer(udp_port_, this));
    workers_.emplace_back(&UdpRpcServer::Run, udp_rpc_servers_.back().get());
  }

  // Nothing to do here
  for (;;) {
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(24h);
  }
}

void Dispatcher::Stop() {
  LOG(INFO) << "Shutting down the dispatcher.";
  running_ = false;
  // Unregister frontend
  Unregister();
  // Stop RPC service
  rpc_service_.Stop();

  // Stop UDP RPC server
  for (auto& server : udp_rpc_servers_) {
    server->Stop();
  }
  for (auto& thread : workers_) {
    thread.join();
  }
}

void Dispatcher::GetBackend(const std::string& model_sess_id,
                            DispatchReply* reply) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto iter = models_.find(model_sess_id);
  if (iter == models_.end()) {
    reply->set_status(CtrlStatus::MODEL_NOT_FOUND);
  } else {
    *reply->mutable_backend() = iter->second.GetBackend();
    reply->set_status(CtrlStatus::CTRL_OK);
  }
}

void Dispatcher::Register() {
  // Init node id
  std::random_device rd;
  std::mt19937 rand_gen(rd());
  std::uniform_int_distribution<uint32_t> dis(
      1, std::numeric_limits<uint32_t>::max());
  node_id_ = dis(rand_gen);

  // Prepare request
  RegisterRequest request;
  request.set_node_type(NodeType::DISPATCHER_NODE);
  request.set_node_id(node_id_);
  request.set_rpc_port(rpc_service_.port());

  while (true) {
    grpc::ClientContext context;
    RegisterReply reply;
    grpc::Status status = sch_stub_->Register(&context, request, &reply);
    if (!status.ok()) {
      LOG(FATAL) << "Failed to connect to scheduler: " << status.error_message()
                 << "(" << status.error_code() << ")";
    }
    CtrlStatus ret = reply.status();
    if (ret == CTRL_OK) {
      beacon_interval_sec_ = reply.beacon_interval_sec();
      return;
    }
    if (ret != CTRL_FRONTEND_NODE_ID_CONFLICT) {
      LOG(FATAL) << "Failed to register frontend to scheduler: "
                 << CtrlStatus_Name(ret);
    }
    // Frontend ID conflict, need to generate a new one
    node_id_ = dis(rand_gen);
    request.set_node_id(node_id_);
  }
}

void Dispatcher::Unregister() {
  UnregisterRequest request;
  request.set_node_type(NodeType::DISPATCHER_NODE);
  request.set_node_id(node_id_);

  grpc::ClientContext context;
  RpcReply reply;
  grpc::Status status = sch_stub_->Unregister(&context, request, &reply);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to connect to scheduler: " << status.error_message()
               << "(" << status.error_code() << ")";
    return;
  }
  CtrlStatus ret = reply.status();
  if (ret != CTRL_OK) {
    LOG(ERROR) << "Failed to unregister frontend: " << CtrlStatus_Name(ret);
  }
}

void Dispatcher::UpdateModelRoutes(const ModelRouteUpdates& request,
                                   RpcReply* reply) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& model_route : request.model_route()) {
    auto iter = models_.find(model_route.model_session_id());
    if (iter == models_.end()) {
      auto res = models_.emplace(model_route.model_session_id(), ModelRoute());
      iter = res.first;
    }
    iter->second.Update(model_route);
  }
  reply->set_status(CTRL_OK);
}

void ModelRoute::Update(const ModelRouteProto& route) {
  LOG(INFO) << "Update model route for " << route.model_session_id();

  // Save the current DRR backend
  const auto current_drr_backend_id =
      backends_.empty() ? 0 : backends_[current_drr_index_].info().node_id();

  // Update from the proto
  model_session_id_.assign(route.model_session_id());
  backends_.assign(route.backend_rate().begin(), route.backend_rate().end());
  total_throughput_ = 0.;

  // Calculate quantum:rate ratio
  min_rate_ = std::numeric_limits<double>::max();
  for (const auto& backend : backends_) {
    min_rate_ = std::min(min_rate_, backend.throughput());
  }

  // Give quantum to new backends
  std::unordered_map<uint32_t, size_t> backend_idx;
  for (size_t i = 0; i < backends_.size(); ++i) {
    const auto& backend = backends_[i];
    const auto backend_id = backend.info().node_id();
    const auto rate = backend.throughput();
    total_throughput_ += rate;
    LOG(INFO) << "  backend " << backend_id << ": " << rate << " rps";
    backend_quanta_.emplace(backend_id, rate);
    backend_idx.emplace(backend_id, i);
  }
  LOG(INFO) << "  total throughput: " << total_throughput_ << " rps";

  // Remove quantum of old backends
  for (auto iter = backend_quanta_.begin(); iter != backend_quanta_.end();) {
    if (backend_idx.count(iter->first) == 0) {
      iter = backend_quanta_.erase(iter);
    } else {
      ++iter;
    }
  }

  // Recover the current DRR backend
  auto backend_idx_iter = backend_idx.find(current_drr_backend_id);
  if (backend_idx_iter != backend_idx.end()) {
    current_drr_index_ = backend_idx_iter->second;
  } else {
    if (backends_.empty()) {
      current_drr_index_ = 0;
    } else {
      current_drr_index_ %= backends_.size();
    }
  }
}

BackendInfo ModelRoute::GetBackend() {
  for (size_t i = 0;; ++i) {
    const auto& backend = backends_[current_drr_index_];
    const uint32_t backend_id = backend.info().node_id();
    if (backend_quanta_.at(backend_id) >= min_rate_) {
      backend_quanta_[backend_id] -= min_rate_;
      return backend.info();
    } else {
      const auto rate = backend.throughput();
      backend_quanta_[backend_id] += rate;
      current_drr_index_ = (current_drr_index_ + 1) % backends_.size();
    }

    CHECK_LE(i, backends_.size()) << "DRR could not decide.";
  }
}

}  // namespace dispatcher
}  // namespace nexus