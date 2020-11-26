#include "nexus/backend/backend_server.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>

#include <memory>
#include <mutex>
#include <unordered_set>

#include "nexus/backend/gpu_executor.h"
#include "nexus/backend/share_prefix_model.h"
#include "nexus/backend/tf_share_model.h"
#include "nexus/common/config.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/proto/control.pb.h"

DEFINE_int32(occupancy_valid, 10, "Backup backend occupancy valid time in ms");

namespace nexus {
namespace backend {

BackendServer::BackendServer(std::string port, std::string rpc_port,
                             std::string sch_addr, int gpu_id,
                             size_t num_workers, std::vector<int> cores)
    : ServerBase(port),
      gpu_id_(gpu_id),
      running_(false),
      rpc_service_(this, rpc_port),
      rand_gen_(rd_()) {
  // Start RPC service
  rpc_service_.Start();
  // Init scheduler client
  if (sch_addr.find(':') == std::string::npos) {
    // Add default scheduler port if no port specified
    sch_addr += ":" + std::to_string(SCHEDULER_DEFAULT_PORT);
  }
  auto channel =
      grpc::CreateChannel(sch_addr, grpc::InsecureChannelCredentials());
  sch_stub_ = DispatcherCtrl::NewStub(channel);

#ifdef USE_GPU
  // Init GPU executor
  LOG(INFO) << "Using PlanFollower as GpuExecutor";
  gpu_executor_.reset(new GpuExecutorPlanFollower(gpu_id));
  if (cores.empty()) {
    gpu_executor_->Start();
  } else {
    gpu_executor_->Start(cores.back());
    cores.pop_back();
    // Pin IO thread to core
    // cpu_set_t cpuset;
    // CPU_ZERO(&cpuset);
    // int io_core = cores.back();
    // CPU_SET(io_core, &cpuset);
    // int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t),
    // &cpuset); if (rc != 0) {
    //   LOG(ERROR) << "Error calling pthread_setaffinity_np: " << rc << "\n";
    // }
    // LOG(INFO) << "IO thread is pinned on CPU " << io_core;
    // cores.pop_back();
  }
#else
  LOG(FATAL) << "backend needs the USE_GPU flag set at compile-time.";
#endif

  // Init workers
  if (num_workers == 0) {
    if (cores.empty()) {
      num_workers = 4;
    } else {
      num_workers = cores.size();
    }
  }
  for (size_t i = 0; i < num_workers; ++i) {
    std::unique_ptr<Worker> worker(new Worker(i, this, task_queue_));
    if (cores.empty()) {
      worker->Start();
    } else {
      worker->Start(cores[i % cores.size()]);
    }
    workers_.push_back(std::move(worker));
  }
}

BackendServer::~BackendServer() {
  if (running_) {
    Stop();
  }
}

void BackendServer::Run() {
  running_ = true;
  // Init node id and register backend to global scheduler
  Register();
  // Start the daemon thread
  model_table_thread_ = std::thread(&BackendServer::ModelTableDaemon, this);
  daemon_thread_ = std::thread(&BackendServer::Daemon, this);
  LOG(INFO) << "Backend server (id: " << node_id_ << ") is listening on "
            << address();
  // Start the IO service
  io_context_.run();
}

void BackendServer::Stop() {
  running_ = false;
  // Unregister backend server
  Unregister();
  // Stop accept new connections
  ServerBase::Stop();
  // Stop all frontend connections
  for (auto conn : all_connections_) {
    conn->Stop();
  }
  all_connections_.clear();
  node_connections_.clear();
  map_connection_nodeid_.clear();
#ifdef USE_GPU
  // Stop GPU executor
  gpu_executor_->Stop();
#endif
  // Stop workers
  for (auto& worker : workers_) {
    worker->Stop();
  }
  workers_.clear();
  // Stop daemon thread
  if (daemon_thread_.joinable()) {
    daemon_thread_.join();
  }
  if (model_table_thread_.joinable()) {
    model_table_thread_.join();
  }
  // Stop RPC service
  rpc_service_.Stop();
  LOG(INFO) << "Backend server stopped";
}

void BackendServer::HandleAccept() {
  VLOG(1) << "HandleAccept";
  std::lock_guard<std::mutex> lock(mu_connections_);
  auto conn = std::make_shared<Connection>(std::move(socket_), this);
  all_connections_.insert(conn);
  conn->Start();
}

void BackendServer::HandleConnected(std::shared_ptr<Connection> conn) {
  // Do nothing.
}

void BackendServer::HandleMessage(std::shared_ptr<Connection> conn,
                                  std::shared_ptr<Message> message) {
  switch (message->type()) {
    case kConnFrontBack: {
      TellNodeIdMessage msg;
      message->DecodeBody(&msg);
      auto node_id = NodeId(msg.node_id());
      VLOG(1) << "kConnFrontBack: frontend_id=" << node_id.t;
      node_connections_[node_id] = conn;
      map_connection_nodeid_[conn] = node_id;
      break;
    }
    case kBackendRelayReply: {
      std::static_pointer_cast<BackupClient>(conn)->Reply(std::move(message));
      break;
    }
    case kFetchImageReply: {
      FetchImageReply reply;
      message->DecodeBody(&reply);
      HandleFetchImageReply(std::move(reply));
      break;
    }
    default:
      LOG(ERROR) << "Wrong message type: " << message->type();
  }
}

void BackendServer::HandleError(std::shared_ptr<Connection> conn,
                                boost::system::error_code ec) {
  if (ec == boost::asio::error::eof ||
      ec == boost::asio::error::connection_reset) {
    LOG(INFO) << "Frontend disconnected.";
    // frontend disconnects
  } else {
    LOG(ERROR) << "Connection error (" << ec << "): " << ec.message();
  }
  std::lock_guard<std::mutex> lock(mu_connections_);
  auto iter = map_connection_nodeid_.find(conn);
  if (iter != map_connection_nodeid_.end()) {
    node_connections_.erase(iter->second);
    map_connection_nodeid_.erase(iter);
  }
  all_connections_.erase(conn);
  conn->Stop();
}

void BackendServer::HandleFetchImageReply(FetchImageReply reply) {
  auto global_id = GlobalId(reply.global_id());
  VLOG(1) << "HandleFetchImageReply: global_id=" << global_id.t;
  std::shared_ptr<Task> task;
  {
    std::lock_guard<std::mutex> lock(mu_tasks_pending_fetch_image_);
    auto iter = tasks_pending_fetch_image_.find(global_id);
    if (iter == tasks_pending_fetch_image_.end()) {
      LOG(ERROR) << "Cannot find Task pending FetchImage. "
                 << "global_id=" << global_id.t;
      return;
    }
    task = iter->second;
    tasks_pending_fetch_image_.erase(iter);
  }
  if (reply.status() != CtrlStatus::CTRL_OK) {
    LOG(ERROR) << "FetchImageReply not ok. status="
               << CtrlStatus_Name(reply.status())
               << ", global_id=" << global_id.t;
    task->result.set_status(reply.status());
    // TODO: SendReply
    return;
  }
  task->query.mutable_input()->Swap(reply.mutable_input());
  task->stage = Stage::kPreprocess;
  task_queue_.push(std::move(task));
}

void BackendServer::LoadModelEnqueue(const BackendLoadModelCommand& request) {
  VLOG(1) << "LoadModelEnqueue: model_session="
          << ModelSessionToString(request.model_session());
  auto req = std::make_shared<BackendLoadModelCommand>();
  req->CopyFrom(request);
  model_table_requests_.push(std::move(req));
}

void BackendServer::LoadModel(const BackendLoadModelCommand& request) {
#ifdef USE_GPU
  // Start to update model table
  std::lock_guard<std::mutex> lock(model_table_mu_);
  auto model_sess_id = ModelSessionToString(request.model_session());
  VLOG(1) << "LoadModel: model_session=" << model_sess_id;
  auto model_iter = model_table_.find(model_sess_id);
  if (model_iter != model_table_.end()) {
    LOG(INFO) << "Skip loading model session " << model_sess_id
              << " because already loaded.";
    return;
  }

  // Temporary adaptor to use existing ModelExecutor constructor.
  ModelInstanceConfig config;
  *config.add_model_session() = request.model_session();
  config.set_batch(1);
  config.set_max_batch(request.max_batch());
  auto gpu_device = DeviceManager::Singleton().GetGPUDevice(gpu_id_);
  auto profile_id = ModelSessionToProfileID(request.model_session());
  auto* profile = ModelDatabase::Singleton().GetModelProfile(
      gpu_device->device_name(), gpu_device->uuid(), profile_id);
  if (!profile) return;
  auto memory_usage = profile->GetMemoryUsage(request.max_batch());
  config.set_memory_usage(memory_usage);

  // Load new model instance
  auto model = std::make_shared<ModelExecutor>(gpu_id_, config, task_queue_);
  model_table_.emplace(model_sess_id, model);
  gpu_executor_->AddModel(model);
  LOG(INFO) << "Load model instance " << model_sess_id
            << ", max_batch: " << config.max_batch();

  // Update duty cycle (Deprecated)
  // auto duty_cycle = request.model_session().latency_sla() * 1e3 / 2;
  // gpu_executor_->SetDutyCycle(duty_cycle);
  // LOG(INFO) << "Duty cycle: " << duty_cycle << " us";
#else
  LOG(FATAL) << "backend needs the USE_GPU flag set at compile-time.";
#endif
}

void BackendServer::HandleEnqueueQuery(const grpc::ServerContext&,
                                       const EnqueueQueryCommand& req,
                                       RpcReply* reply) {
  auto task = std::make_shared<Task>(nullptr);
  task->SetQuery(req.query_without_input());
  bool ok = EnqueueQuery(task);
  reply->set_status(ok ? CtrlStatus::CTRL_OK
                       : CtrlStatus(task->result.status()));
}

bool BackendServer::EnqueueQuery(std::shared_ptr<Task> task) {
  VLOG(1) << "EnqueueQuery: frontend_id=" << task->query.frontend_id()
          << ", model_session=" << task->query.model_session_id()
          << ", query_id=" << task->query.query_id()
          << ", global_id=" << task->query.global_id();
  std::shared_ptr<Connection> frontend_conn;
  {
    auto frontend_id = NodeId(task->query.frontend_id());
    auto iter = node_connections_.find(frontend_id);
    if (iter == node_connections_.end()) {
      LOG(ERROR) << "Cannot find connection to Frontend " << frontend_id.t
                 << ". Ignore the incoming query. "
                 << "model_session=" << task->query.model_session_id()
                 << ", query_id=" << task->query.query_id()
                 << ", global_id=" << task->query.global_id();
      task->result.set_status(CtrlStatus::CTRL_FRONTEND_CONNECTION_NOT_FOUND);
      // TODO: SendReply
      return false;
    }
    frontend_conn = iter->second;
  }
  task->SetConnection(frontend_conn);
  auto global_id = GlobalId(task->query.global_id());
  {
    std::lock_guard<std::mutex> lock(mu_tasks_pending_fetch_image_);
    if (tasks_pending_fetch_image_.count(global_id)) {
      LOG(ERROR) << "GlobalId of the incoming request is not unique. Skip. "
                 << "global_id=" << global_id.t;
      task->result.set_status(CtrlStatus::CTRL_GLOBAL_ID_CONFLICT);
      // TODO: SendReply
      return false;
    }
    tasks_pending_fetch_image_[global_id] = task;
  }

  // Send FetchImage rpc
  FetchImageRequest request;
  *request.mutable_model_session_id() = task->query.model_session_id();
  request.set_query_id(task->query.query_id());
  request.set_global_id(global_id.t);
  VLOG(1) << "Send FetchImageRequest: global_id=" << global_id.t;
  auto msg = std::make_shared<Message>(MessageType::kFetchImageRequest,
                                       request.ByteSizeLong());
  msg->EncodeBody(request);
  frontend_conn->Write(std::move(msg));
  return true;
}

void BackendServer::HandleEnqueueBatchPlan(const grpc::ServerContext&,
                                           const BatchPlanProto& req,
                                           RpcReply* reply) {
  // Add batchplan
  auto plan = std::make_shared<BatchPlanContext>(req);
  {
    std::lock_guard<std::mutex> lock(mu_pending_plans_);
    if (pending_plans_.count(plan->plan_id())) {
      LOG(ERROR) << "HandleEnqueueBatchPlan: already exists. plan_id="
                 << plan->proto().plan_id();
      return;
    }
    pending_plans_[plan->plan_id()] = plan;
  }

  // Enqueue queries
  bool all_ok = true;
  for (const auto& query : plan->proto().queries_without_input()) {
    auto task = std::make_shared<Task>(nullptr);
    task->SetQuery(query);
    task->SetPlanId(plan->plan_id());
    bool ok = EnqueueQuery(task);
    if (!ok) {
      all_ok = false;
      plan->MarkQueryDropped(GlobalId(query.global_id()));
    }
  }
  reply->set_status(all_ok ? CtrlStatus::CTRL_OK
                           : CtrlStatus::CTRL_UNSPECIFIED_ERROR);
}

void BackendServer::MarkBatchPlanQueryPreprocessed(std::shared_ptr<Task> task) {
  CHECK(task->plan_id.has_value());
  auto plan_id = task->plan_id.value();
  std::shared_ptr<BatchPlanContext> ready_plan;
  {
    std::lock_guard<std::mutex> lock(mu_pending_plans_);
    auto iter = pending_plans_.find(plan_id);
    if (iter == pending_plans_.end()) {
      LOG(ERROR) << "Cannot find pending plan. plan_id=" << plan_id.t
                 << ", global_id=" << task->query.global_id();
      return;
    }
    auto plan = iter->second;
    plan->AddPreprocessedTask(task);
    if (plan->IsReadyToRun()) {
      ready_plan = plan;
      pending_plans_.erase(iter);
    }
  }
  if (ready_plan) {
    gpu_executor_->AddBatchPlan(ready_plan);
  }
}

ModelExecutorPtr BackendServer::GetModel(const std::string& model_session_id) {
  std::lock_guard<std::mutex> lock(model_table_mu_);
  auto itr = model_table_.find(model_session_id);
  if (itr == model_table_.end()) {
    LOG(WARNING) << "Model session is not loaded: " << model_session_id;
    return nullptr;
  }
  return itr->second;
}

BackendServer::ModelTable BackendServer::GetModelTable() {
  std::lock_guard<std::mutex> lock(model_table_mu_);
  return model_table_;
}

std::shared_ptr<BackupClient> BackendServer::GetBackupClient(
    uint32_t backend_id) {
  return nullptr;
}

void BackendServer::Daemon() {
  while (running_) {
    auto next_time = Clock::now() + std::chrono::seconds(beacon_interval_sec_);
    KeepAlive();
    ModelTable model_table;
    {
      std::lock_guard<std::mutex> lock(model_table_mu_);
      model_table = model_table_;
    }
    for (auto iter : model_table) {
      double rps = iter.second->GetRequestRate();
      double drop_rate = iter.second->GetDropRate();
      if (rps > 0.1) {
        int drop_percent = static_cast<int>(100. * drop_rate / rps);
        if (drop_percent >= 1) {
          LOG(WARNING) << iter.first << " request rate: " << rps
                       << ", drop rate: " << drop_rate << " (" << drop_percent
                       << "%)";
        } else {
          VLOG(1) << iter.first << " request rate: " << rps
                  << ", drop rate: " << drop_rate << " (" << drop_percent
                  << "%)";
        }
      }
    }
    std::this_thread::sleep_until(next_time);
  }
}

void BackendServer::ModelTableDaemon() {
  auto timeout = std::chrono::milliseconds(500);
  while (running_) {
    auto req = model_table_requests_.pop(timeout);
    if (req == nullptr) {
      continue;
    }
    LoadModel(*req);
  }
}

void BackendServer::Register() {
#ifdef USE_GPU
  // Init node id
  std::uniform_int_distribution<uint32_t> dis(
      1, std::numeric_limits<uint32_t>::max());
  node_id_ = dis(rand_gen_);

  // Prepare request
  RegisterRequest request;
  request.set_node_type(BACKEND_NODE);
  request.set_node_id(node_id_);
  request.set_server_port(port());
  request.set_rpc_port(rpc_service_.port());
  GPUDevice* gpu_device = DeviceManager::Singleton().GetGPUDevice(gpu_id_);
  request.set_gpu_device_name(gpu_device->device_name());
  request.set_gpu_uuid(gpu_device->uuid());
  request.set_gpu_available_memory(gpu_device->FreeMemory());

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
      VLOG(1) << "Register done.";
      return;
    }
    if (ret != CTRL_BACKEND_NODE_ID_CONFLICT) {
      LOG(FATAL) << "Failed to register backend to scheduler: "
                 << CtrlStatus_Name(ret);
    }
    // Backend ID conflict, need to generate a new one
    node_id_ = dis(rand_gen_);
    request.set_node_id(node_id_);
  }
#else
  LOG(FATAL) << "backend needs the USE_GPU flag set at compile-time.";
#endif
}

void BackendServer::Unregister() {
  UnregisterRequest request;
  request.set_node_type(BACKEND_NODE);
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
    LOG(ERROR) << "Unregister error: " << CtrlStatus_Name(ret);
  }
}

void BackendServer::KeepAlive() {
  grpc::ClientContext context;
  KeepAliveRequest req;
  req.set_node_type(BACKEND_NODE);
  req.set_node_id(node_id_);
  RpcReply reply;
  grpc::Status status = sch_stub_->KeepAlive(&context, req, &reply);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to connect to scheduler: " << status.error_message()
               << "(" << status.error_code() << ")";
    return;
  }
  CtrlStatus ret = reply.status();
  if (ret != CTRL_OK) {
    LOG(ERROR) << "KeepAlive error: " << CtrlStatus_Name(ret);
  }
}

}  // namespace backend
}  // namespace nexus
