#include "nexus/backend/backend_server.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pthread.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <opencv2/opencv.hpp>
#include <string>
#include <unordered_set>

#include "nexus/backend/gpu_executor.h"
#include "nexus/backend/share_prefix_model.h"
#include "nexus/backend/tf_share_model.h"
#include "nexus/common/config.h"
#include "nexus/common/device.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/sleep_profile.h"
#include "nexus/common/typedef.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

DEFINE_int32(occupancy_valid, 10, "Backup backend occupancy valid time in ms");

namespace nexus {
namespace backend {

BackendServer::GpuContext::GpuContext() {}

BackendServer::ConnContext::ConnContext(ario::RdmaQueuePair* conn)
    : conn(*CHECK_NOTNULL(conn)) {}

BackendServer::BackendServer(ario::PollerType poller_type, std::string rdma_dev,
                             uint16_t port, std::string sch_addr,
                             std::vector<int> cuda_indexes, size_t num_workers,
                             std::vector<int> cores)
    : rdma_dev_(std::move(rdma_dev)),
      rdma_port_(port),
      executor_(poller_type),
      rdma_handler_(*this),
      small_buffers_(kSmallBufferPoolBits, kSmallBufferBlockBits),
      large_buffers_(kLargeBufferPoolBits, kLargeBufferBlockBits),
      rdma_(rdma_dev_, &executor_, &rdma_handler_, &small_buffers_),
      rdma_sender_(&small_buffers_),
      helper_executor_(ario::PollerType::kBlocking),
      running_(false),
      rand_gen_(rd_()) {
  gpus_.reserve(cuda_indexes.size());
  for (size_t i = 0; i < cuda_indexes.size(); ++i) {
    auto& g = *gpus_.emplace_back(std::make_unique<GpuContext>());
    g.gpu_idx = i;
    g.cuda_idx = cuda_indexes[i];
#ifdef USE_GPU
    auto* gpu = DeviceManager::Singleton().GetGPUDevice(cuda_indexes[i]);
    g.gpu_name = gpu->device_name();
    g.gpu_uuid = gpu->uuid();
    g.gpu_memory = gpu->FreeMemory();
#else
    auto* cpu = DeviceManager::Singleton().GetCPUDevice();
    g.gpu_name = cpu->name();
    g.gpu_uuid = "GenericCPU";
    g.gpu_memory = 0;
#endif
  }

  rdma_.RegisterLocalMemory(&large_buffers_);
  rdma_.ListenTcp(port);

  // Init scheduler client
  uint16_t sch_port;
  auto sch_colon_idx = sch_addr.find(':');
  if (sch_colon_idx == std::string::npos) {
    sch_port = SCHEDULER_DEFAULT_PORT;
  } else {
    sch_port = std::stoi(sch_addr.substr(sch_colon_idx + 1));
    sch_addr.resize(sch_colon_idx);
  }
  rdma_.ConnectTcp(sch_addr, sch_port);
  executor_threads_.emplace_back([this] {
    pthread_setname_np(pthread_self(), "MainExecutor");
    executor_.RunEventLoop();
  });
  if (num_workers == 0) {
    if (cores.empty()) {
      num_workers = 7;
    } else {
      num_workers = cores.size();
    }
  }
  for (size_t i = 0; i < num_workers; ++i) {
    executor_threads_.emplace_back([this] {
      pthread_setname_np(pthread_self(), "HelperExecutor");
      helper_executor_.RunEventLoop();
    });
  }
  dispatcher_conn_ = promise_dispatcher_conn_.get_future().get();

  // Init GPU executor
  LOG(INFO) << "Using PlanFollower as GpuExecutor";
  for (auto& gpu : gpus_) {
    gpu->gpu_executor =
        std::make_unique<GpuExecutorPlanFollower>(gpu->cuda_idx, poller_type);
    if (cores.empty()) {
      gpu->gpu_executor->Start();
    } else {
      CHECK(!cores.empty());
      gpu->gpu_executor->Start(cores.back());
      cores.pop_back();
    }
  }

  // Init workers
  for (size_t i = 0; i < num_workers; ++i) {
    std::unique_ptr<Worker> worker(
        new Worker(i, this, rdma_sender_, task_queue_));
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
            << "port " << rdma_port_;

  // Block forever
  for (auto& t : executor_threads_) {
    t.join();
  }
}

void BackendServer::Stop() {
  running_ = false;
  // Unregister backend server
  Unregister();
  // Stop accept new connections
  executor_.StopEventLoop();
  rdma_.Stop();
  // Stop all frontend connections
  for (auto conn : all_connections_) {
    conn->Shutdown();
  }
  all_connections_.clear();
  node_connections_.clear();
  map_connection_nodeid_.clear();

  for (auto& gpu : gpus_) {
    gpu->gpu_executor->Stop();
  }
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
  for (auto& t : executor_threads_) {
    t.join();
  }

  LOG(INFO) << "Backend server stopped";
}

BackendServer::RdmaHandler::RdmaHandler(BackendServer& outer) : outer_(outer) {}

void BackendServer::RdmaHandler::OnConnected(ario::RdmaQueuePair* conn) {
  VLOG(1) << "BackendServer::RdmaHandler::OnConnected";
  if (!outer_.dispatcher_conn_) {
    outer_.promise_dispatcher_conn_.set_value(conn);
  } else {
    std::lock_guard<std::mutex> lock(outer_.mu_connections_);
    outer_.all_connections_.insert(conn);
  }
}

void BackendServer::RdmaHandler::OnRemoteMemoryRegionReceived(
    ario::RdmaQueuePair* conn, uint64_t addr, size_t size) {
  // Do nothing for now. TODO
}

void BackendServer::RdmaHandler::OnRdmaReadComplete(
    ario::RdmaQueuePair* conn, ario::WorkRequestID wrid,
    ario::OwnedMemoryBlock buf) {
  // Use helper executor to handle messages. Prevent starving RDMA event loop.
  auto now = Clock::now();
  auto pbuf = std::make_shared<ario::OwnedMemoryBlock>(std::move(buf));
  outer_.helper_executor_.PostBigCallback(
      [this, wrid, pbuf, now = now](ario::ErrorCode) {
        outer_.HandleFetchImageReply(wrid, std::move(*pbuf), now);
      },
      ario::ErrorCode::kOk);

  // Post remaining READ ops.
  std::shared_ptr<ConnContext> frontend_connctx;
  {
    std::lock_guard lock(outer_.mu_connections_);
    auto it = outer_.map_connection_nodeid_.find(conn);
    if (it != outer_.map_connection_nodeid_.end()) {
      auto frontend_id = it->second;
      auto iter = outer_.node_connections_.find(frontend_id);
      if (iter != outer_.node_connections_.end()) {
        frontend_connctx = iter->second;
      }
    }
  }
  if (!frontend_connctx) {
    LOG(ERROR) << "Lost connection to Frontend " << conn->peer_ip() << ":"
               << conn->peer_tcp_port() << "."
               << " TODO: clear pending tasks.";
    return;
  }
}

void BackendServer::RdmaHandler::OnRecv(ario::RdmaQueuePair* conn,
                                        ario::OwnedMemoryBlock buf) {
  // Use helper executor to handle messages. Prevent starving RDMA event loop.
  auto now = Clock::now();
  auto pbuf = std::make_shared<ario::OwnedMemoryBlock>(std::move(buf));
  outer_.helper_executor_.PostBigCallback(
      [this, conn, pbuf](ario::ErrorCode) {
        OnRecvInternal(conn, std::move(*pbuf));
      },
      ario::ErrorCode::kOk);
}

void BackendServer::RdmaHandler::OnRecvInternal(ario::RdmaQueuePair* conn,
                                                ario::OwnedMemoryBlock buf) {
  auto view = buf.AsMessageView();
  ControlMessage req;
  bool ok = req.ParseFromArray(view.bytes(), view.bytes_length());
  if (!ok) {
    LOG(ERROR) << "ParseFromArray failed";
    return;
  }
  switch (req.message_case()) {
    case ControlMessage::MessageCase::kRegisterReply: {
      // from Dispatcher
      outer_.promise_register_reply_.set_value(
          std::move(*req.mutable_register_reply()));
      break;
    }
    case ControlMessage::MessageCase::kUnregisterReply: {
      // from Dispatcher
      outer_.promise_unregister_reply_.set_value(
          std::move(*req.mutable_unregister_reply()));
      break;
    }
    case ControlMessage::MessageCase::kCheckAlive: {
      // from Dispatcher
      outer_.KeepAlive();
      break;
    }
    case ControlMessage::MessageCase::kLoadModel: {
      // from Dispatcher
      outer_.LoadModelEnqueue(req.load_model());

      ControlMessage resp;
      resp.mutable_load_model_reply()->set_status(CtrlStatus::CTRL_OK);
      outer_.rdma_sender_.SendMessage(conn, resp);
      break;
    }
    case ControlMessage::MessageCase::kEnqueueBatchplan: {
      // from Dispatcher
      ControlMessage resp;
      outer_.HandleEnqueueBatchPlan(std::move(*req.mutable_enqueue_batchplan()),
                                    resp.mutable_enqueue_batchplan_reply());
      outer_.rdma_sender_.SendMessage(conn, resp);
      break;
    }
    case ControlMessage::MessageCase::kTellNodeId: {
      // from Frontend
      const auto& msg = req.tell_node_id();
      auto node_id = NodeId(msg.node_id());
      VLOG(1) << "kConnFrontBack: frontend_id=" << node_id.t;
      std::lock_guard<std::mutex> lock(outer_.mu_connections_);
      outer_.node_connections_[node_id] = std::make_shared<ConnContext>(conn);
      outer_.map_connection_nodeid_[conn] = node_id;
      break;
    }
    default:
      LOG(FATAL) << "Unhandled message: " << req.DebugString();
  }
}

void BackendServer::RdmaHandler::OnSent(ario::RdmaQueuePair* conn,
                                        ario::OwnedMemoryBlock buf) {
  // Do nothing.
}

void BackendServer::RdmaHandler::OnError(ario::RdmaQueuePair* conn,
                                         ario::RdmaError error) {
  if (error == ario::RdmaError::kDisconnect) {
    LOG(INFO) << "Frontend disconnected.";
    // frontend disconnects
  } else {
    LOG(ERROR) << "Connection error.";
  }
  std::lock_guard<std::mutex> lock(outer_.mu_connections_);
  auto iter = outer_.map_connection_nodeid_.find(conn);
  if (iter != outer_.map_connection_nodeid_.end()) {
    outer_.node_connections_.erase(iter->second);
    outer_.map_connection_nodeid_.erase(iter);
  }
  outer_.all_connections_.erase(conn);
  conn->Shutdown();
}

void BackendServer::HandleFetchImageReply(ario::WorkRequestID wrid,
                                          ario::OwnedMemoryBlock buf,
                                          TimePoint got_image_time) {
  auto backend_prep_dequeue_ns = Clock::now().time_since_epoch().count();
  auto backend_got_image_ns = got_image_time.time_since_epoch().count();
  std::shared_ptr<Task> task;
  {
    std::lock_guard<std::mutex> lock(mu_tasks_pending_fetch_image_);
    auto iter = tasks_pending_fetch_image_.find(wrid);
    if (iter == tasks_pending_fetch_image_.end()) {
      LOG(ERROR) << "Cannot find Task pending FetchImage. "
                 << "wrid=" << wrid.DebugString();
      return;
    }
    task = iter->second;
    tasks_pending_fetch_image_.erase(iter);
  }
  auto global_id = task->query.global_id();

  CHECK(task->plan_id.has_value());
  auto plan_id = task->plan_id.value();
  std::shared_ptr<BatchPlanContext> plan;
  {
    std::lock_guard<std::mutex> lock(mu_pending_plans_);
    auto iter = pending_plans_.find(plan_id);
    if (iter == pending_plans_.end()) {
      LOG(ERROR) << "Cannot find pending plan. plan_id=" << plan_id.t
                 << ", global_id=" << task->query.global_id();
      return;
    }
    plan = iter->second;
  }

  auto fetch_image_elapse_ns =
      backend_got_image_ns - task->query.clock().backend_fetch_image_ns();
  LOG_IF(WARNING, fetch_image_elapse_ns > 5 * 1000 * 1000)
      << "Took way too long time to fetch image. "
      << fetch_image_elapse_ns / 1000 << "us. global_id=" << global_id;

  // TODO: refactor preprocessing
  constexpr int kSize = 224;
  auto view = buf.AsMessageView();
  auto* beg = reinterpret_cast<unsigned char*>(view.bytes());
  auto len = view.bytes_length() / sizeof(*beg);
  CHECK_EQ(len, kSize * kSize * 3);
  auto* cpu_device = DeviceManager::Singleton().GetCPUDevice();
  auto in_arr =
      plan->pinned_memory()->Slice(len * task->index_in_batchplan, len);
  cv::Mat img(kSize, kSize, CV_8UC3, beg);
  cv::Mat fimg(kSize, kSize, CV_32FC3, in_arr->Data<void>());
  img.convertTo(fimg, CV_32FC3);
  task->AppendInput(in_arr);
  // Skip the preprocessing stage
  // task->stage = Stage::kPreprocess;
  // task_queue_.push(std::move(task));
  auto backend_preprocessed_ns = Clock::now().time_since_epoch().count();

  task->stage = Stage::kForward;
  MarkBatchPlanQueryPreprocessed(task);
  auto backend_memcpy_ns = Clock::now().time_since_epoch().count();

  auto* clock = task->query.mutable_clock();
  clock->set_backend_got_image_ns(backend_got_image_ns);
  clock->set_backend_prep_dequeue_ns(backend_prep_dequeue_ns);
  clock->set_backend_preprocessed_ns(backend_preprocessed_ns);
  clock->set_backend_memcpy_ns(backend_memcpy_ns);
}

void BackendServer::LoadModelEnqueue(const BackendLoadModelCommand& request) {
  VLOG(1) << "LoadModelEnqueue: model_session="
          << ModelSessionToString(request.model_session());
  auto req = std::make_shared<BackendLoadModelCommand>();
  req->CopyFrom(request);
  model_table_requests_.push(std::move(req));
}

void BackendServer::LoadModel(const BackendLoadModelCommand& request) {
  auto model_sess_id = ModelSessionToString(request.model_session());
  VLOG(1) << "LoadModel: model_session=" << model_sess_id
          << ", gpu_idx=" << request.gpu_idx();
  CHECK_GT(gpus_.size(), request.gpu_idx());
  auto& gpu = *gpus_[request.gpu_idx()];

  // Start to update model table
  std::lock_guard<std::mutex> lock(gpu.model_table_mu);
  if (gpu.model_table.size() <= request.model_index()) {
    gpu.model_table.resize(request.model_index() + 1);
  }
  if (gpu.model_table[request.model_index()]) {
    CHECK_EQ(
        gpu.model_table[request.model_index()]->model()->model_session_id(),
        model_sess_id);
    LOG(INFO) << "Skip loading model session " << model_sess_id
              << " because already loaded.";
    return;
  }

  // Hack: limit max batch
  constexpr uint32_t kMaxBatch = 75;
  uint32_t max_batch = request.max_batch();
  if (max_batch > kMaxBatch) {
    LOG(WARNING) << "Limiting max_batch. actual=" << max_batch
                 << ", kMaxBatch=" << kMaxBatch
                 << ", model_session=" << model_sess_id;
    max_batch = kMaxBatch;
  }

  // Temporary adaptor to use existing ModelExecutor constructor.
  ModelInstanceConfig config;
  *config.add_model_session() = request.model_session();
  config.set_batch(1);
  config.set_max_batch(max_batch);

  auto profile_id = ModelSessionToProfileID(request.model_session());
  auto* profile = ModelDatabase::Singleton().GetModelProfile(
      gpu.gpu_name, gpu.gpu_uuid, profile_id);
  if (!profile) return;
  auto memory_usage = profile->GetMemoryUsage(config.max_batch());
  config.set_memory_usage(memory_usage);

  // Load new model instance
  auto model = std::make_shared<ModelExecutor>(
      gpu.cuda_idx, config, ModelIndex(request.model_index()), task_queue_);
  gpu.model_table[request.model_index()] = model;
  gpu.gpu_executor->AddModel(model);
  LOG(INFO) << "Load model instance " << model_sess_id
            << ", max_batch: " << model->model()->max_batch()
            << ", model_index: " << request.model_index()
            << ", gpu_idx: " << request.gpu_idx()
            << ", cuda_idx: " << gpu.cuda_idx;
}

bool BackendServer::EnqueueQuery(std::shared_ptr<Task> task) {
  std::shared_ptr<ConnContext> frontend_connctx;
  {
    std::lock_guard<std::mutex> lock(mu_connections_);
    auto frontend_id = NodeId(task->query.frontend_id());
    auto iter = node_connections_.find(frontend_id);
    if (iter == node_connections_.end()) {
      LOG(ERROR) << "Cannot find connection to Frontend " << frontend_id.t
                 << ". Ignore the incoming query. "
                 << "model_index=" << task->query.model_index()
                 << ", query_id=" << task->query.query_id()
                 << ", global_id=" << task->query.global_id();
      task->result.set_status(CtrlStatus::CTRL_FRONTEND_CONNECTION_NOT_FOUND);
      // TODO: SendReply
      return false;
    }
    frontend_connctx = iter->second;
  }
  task->SetConnection(&frontend_connctx->conn);

  // RDMA READ input image from Frontend
  auto backend_fetch_image_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now().time_since_epoch())
          .count();
  task->query.mutable_clock()->set_backend_fetch_image_ns(
      backend_fetch_image_ns);
  auto global_id = task->query.global_id();
  auto wrid = frontend_connctx->conn.AsyncRead(large_buffers_.Allocate(),
                                               task->rdma_read_offset,
                                               task->rdma_read_length);
  {
    std::lock_guard<std::mutex> lock(mu_tasks_pending_fetch_image_);
    tasks_pending_fetch_image_[wrid] = task;
  }
  return true;
}

void BackendServer::HandleEnqueueBatchPlan(BatchPlanProto&& req,
                                           RpcReply* reply) {
  auto backend_recv_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             Clock::now().time_since_epoch())
                             .count();

  // Add batchplan
  auto plan = std::make_shared<BatchPlanContext>(std::move(req));
  plan->stats().set_backend_id(node_id_);
  plan->stats().set_backend_recv_ns(backend_recv_ns);
  {
    std::lock_guard<std::mutex> lock(mu_pending_plans_);
    if (pending_plans_.count(plan->plan_id())) {
      LOG(ERROR) << "HandleEnqueueBatchPlan: already exists. plan_id="
                 << plan->proto().plan_id();
      return;
    }
    pending_plans_[plan->plan_id()] = plan;
  }

  // Acquire input array
  CHECK_GT(gpus_.size(), plan->proto().gpu_idx());
  auto& gpu = *gpus_[plan->proto().gpu_idx()];
  CHECK_GT(gpu.model_table.size(), plan->proto().model_index());
  auto model_executor = gpu.model_table[plan->proto().model_index()];
  CHECK(model_executor != nullptr);
  plan->SetInputArray(model_executor->AcquireInputArray());
  auto pinned = model_executor->AcquirePinnedMemory();
  plan->SetPinnedMemory(pinned);

  // Enqueue queries
  reply->set_status(CtrlStatus::CTRL_OK);
  for (int i = 0; i < plan->proto().queries_size(); ++i) {
    const auto& query = plan->proto().queries(i);
    auto task = std::make_shared<Task>(nullptr, model_executor);
    task->index_in_batchplan = i;
    task->SetQuery(query.query_without_input(), query.rdma_read_offset(),
                   query.rdma_read_length());
    task->query.set_model_index(plan->proto().model_index());
    task->SetPlanId(plan->plan_id());
    task->query.mutable_clock()->set_backend_recv_ns(backend_recv_ns);
    bool ok = EnqueueQuery(task);
    if (!ok) {
      plan->MarkQueryDropped(GlobalId(query.query_without_input().global_id()));
      reply->set_status(CtrlStatus(task->result.status()));
    }
  }
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
    auto gpu_idx = ready_plan->proto().gpu_idx();
    CHECK_GT(gpus_.size(), gpu_idx);
    gpus_[gpu_idx]->gpu_executor->AddBatchPlan(ready_plan);
  }
}

void BackendServer::Daemon() {
  while (running_) {
    auto next_time = Clock::now() + std::chrono::seconds(beacon_interval_sec_);
    KeepAlive();
    for (auto& gpu : gpus_) {
      std::vector<ModelExecutorPtr> model_table;
      {
        std::lock_guard<std::mutex> lock(gpu->model_table_mu);
        model_table = gpu->model_table;
      }
      for (const auto& m : model_table) {
        double rps = m->GetRequestRate();
        double drop_rate = m->GetDropRate();
        if (rps > 0.1) {
          int drop_percent = static_cast<int>(100. * drop_rate / rps);
          if (drop_percent >= 1) {
            LOG(WARNING) << m->model()->model_session_id()
                         << " request rate: " << rps
                         << ", drop rate: " << drop_rate << " (" << drop_percent
                         << "%)";
          } else {
            VLOG(1) << m->model()->model_session_id()
                    << " request rate: " << rps << ", drop rate: " << drop_rate
                    << " (" << drop_percent << "%)";
          }
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
  // Init node id
  std::uniform_int_distribution<uint32_t> dis(
      1, std::numeric_limits<uint32_t>::max());
  node_id_ = dis(rand_gen_);

  // Prepare request
  ControlMessage msg;
  auto& request = *msg.mutable_register_request();
  request.set_node_type(BACKEND_NODE);
  request.set_node_id(node_id_);
  request.set_port(rdma_port_);
  for (const auto& gpu : gpus_) {
    auto* g = request.add_gpus();
    g->set_gpu_idx(gpu->gpu_idx);
    g->set_gpu_device_name(gpu->gpu_name);
    g->set_gpu_uuid(gpu->gpu_uuid);
    g->set_gpu_available_memory(gpu->gpu_memory);
  }

  rdma_sender_.SendMessage(dispatcher_conn_, msg);
  auto reply = std::move(promise_register_reply_.get_future().get());
  if (reply.status() == CtrlStatus::CTRL_OK) {
    beacon_interval_sec_ = reply.beacon_interval_sec();
    VLOG(1) << "Register done.";
  } else {
    LOG(FATAL) << "Failed to register frontend to dispatcher: "
               << CtrlStatus_Name(reply.status());
  }
}

void BackendServer::Unregister() {
  ControlMessage msg;
  auto& request = *msg.mutable_unregister_request();
  request.set_node_type(BACKEND_NODE);
  request.set_node_id(node_id_);

  rdma_sender_.SendMessage(dispatcher_conn_, msg);
  auto reply = std::move(promise_unregister_reply_.get_future().get());
  CtrlStatus ret = reply.status();
  if (ret != CTRL_OK) {
    LOG(ERROR) << "Unregister error: " << CtrlStatus_Name(ret);
  }
}

void BackendServer::KeepAlive() {
  ControlMessage resp;
  auto* reply = resp.mutable_inform_alive();
  reply->set_node_type(NodeType::BACKEND_NODE);
  reply->set_node_id(node_id_);
  rdma_sender_.SendMessage(dispatcher_conn_, resp);
}

}  // namespace backend
}  // namespace nexus
