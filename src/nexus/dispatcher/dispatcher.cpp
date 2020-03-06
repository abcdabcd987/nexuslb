#include <algorithm>

#include "nexus/dispatcher/dispatcher.h"
#include "nexus/common/config.h"

namespace nexus {
namespace dispatcher {

Dispatcher::Dispatcher(std::string port, std::string rpc_port,
                       std::string sch_addr)
    : ServerBase(port), rpc_service_(this, rpc_port, 1), rand_gen_(rd_()) {
  // Start RPC service
  rpc_service_.Start();
  // Init scheduler client
  if (sch_addr.find(':') == std::string::npos) {
    // Add default scheduler port if no port specified
    sch_addr += ":" + std::to_string(SCHEDULER_DEFAULT_PORT);
  }
  auto channel =
      grpc::CreateChannel(sch_addr, grpc::InsecureChannelCredentials());
  sch_stub_ = SchedulerCtrl::NewStub(channel);
  // Init Node ID and register frontend to scheduler
  Register();
}

Dispatcher::~Dispatcher() {
  if (running_) {
    Stop();
  }
}

void Dispatcher::Run() {
  running_ = true;
  LOG(INFO) << "Dispatcher server (id: " << node_id_ << ") is listening on "
            << address();
  io_context_.run();
}

void Dispatcher::Stop() {
  running_ = false;
  // Unregister frontend
  Unregister();
  // Stop all accept new connections
  ServerBase::Stop();
  // Stop RPC service
  rpc_service_.Stop();
  LOG(INFO) << "Dispatcher server stopped";
}

void Dispatcher::Register() {
  // Init node id
  std::uniform_int_distribution<uint32_t> dis(
      1, std::numeric_limits<uint32_t>::max());
  node_id_ = dis(rand_gen_);

  // Prepare request
  RegisterRequest request;
  request.set_node_type(FRONTEND_NODE);
  request.set_node_id(node_id_);
  request.set_server_port(port());
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
    node_id_ = dis(rand_gen_);
    request.set_node_id(node_id_);
  }
}

void Dispatcher::Unregister() {
  UnregisterRequest request;
  request.set_node_type(FRONTEND_NODE);
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
      backends_[current_drr_index_].info().node_id();

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
  if (backend_idx_iter == backend_idx.end()) {
    current_drr_index_ %= backends_.size();
  } else {
    current_drr_index_ = backend_idx_iter->second;
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