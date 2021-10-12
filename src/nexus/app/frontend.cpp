#include "nexus/app/frontend.h"

#include <gflags/gflags.h>

#include <boost/asio.hpp>
#include <limits>
#include <memory>
#include <mutex>
#include <string>

#include "nexus/common/config.h"
#include "nexus/common/model_def.h"
#include "nexus/proto/control.pb.h"

DECLARE_int32(load_balance);

namespace nexus {
namespace app {

Frontend::Frontend(ario::PollerType poller_type, std::string rdma_dev,
                   uint16_t rdma_tcp_server_port, std::string nexus_server_port,
                   std::string sch_addr)
    : ServerBase(nexus_server_port),
      executor_(poller_type),
      rdma_tcp_server_port_(rdma_tcp_server_port),
      rdma_handler_(*this),
      small_buffers_(kSmallBufferPoolBits, kSmallBufferBlockBits),
      large_buffers_(kLargeBufferPoolBits, kLargeBufferBlockBits),
      rdma_(rdma_dev, &executor_, &rdma_handler_, &small_buffers_),
      rdma_sender_(&small_buffers_),
      helper_executor_(ario::PollerType::kBlocking),
      rd_(),
      rand_gen_(rd_()) {
  rdma_.ExposeMemory(large_buffers_.data(), large_buffers_.pool_size());
  rdma_.ListenTcp(rdma_tcp_server_port);

  uint16_t sch_port;
  auto sch_colon_idx = sch_addr.find(':');
  if (sch_colon_idx == std::string::npos) {
    dispatcher_ip_ = sch_addr;
    sch_port = SCHEDULER_DEFAULT_PORT;
  } else {
    sch_port = std::stoi(sch_addr.substr(sch_colon_idx + 1));
    dispatcher_ip_ = sch_addr.substr(0, sch_colon_idx);
  }

  rdma_.ConnectTcp(dispatcher_ip_, sch_port);
  executor_threads_.emplace_back(&ario::EpollExecutor::RunEventLoop,
                                 &executor_);
  executor_threads_.emplace_back(&ario::EpollExecutor::RunEventLoop,
                                 &helper_executor_);
  dispatcher_conn_ = promise_dispatcher_conn_.get_future().get();

  // Init Node ID and register frontend to scheduler
  Register();
}

Frontend::~Frontend() {
  if (running_) {
    Stop();
  }
}

void Frontend::Run(QueryProcessor* qp, size_t nthreads) {
  // TODO: Unify workers and executor threads
  for (size_t i = 0; i < nthreads; ++i) {
    std::unique_ptr<Worker> worker(new Worker(qp, request_pool_));
    worker->Start();
    workers_.push_back(std::move(worker));
  }
  for (size_t i = 1; i < nthreads; ++i) {
    executor_threads_.emplace_back(&ario::EpollExecutor::RunEventLoop,
                                   &helper_executor_);
  }

  running_ = true;
  daemon_thread_ = std::thread(&Frontend::Daemon, this);
  LOG(INFO) << "Frontend server (id: " << node_id_ << ") is listening on "
            << address();
  io_context_.run();
}

void Frontend::Stop() {
  running_ = false;
  // Unregister frontend
  Unregister();
  // Stop all accept new connections
  ServerBase::Stop();
  helper_executor_.StopEventLoop();
  executor_.StopEventLoop();
  rdma_.Stop();
  // Stop all frontend connections
  for (auto conn : connection_pool_) {
    conn->Stop();
  }
  connection_pool_.clear();
  user_sessions_.clear();
  // Stop all backend connections
  backend_pool_.StopAll();
  // Stop workers
  for (auto& worker : workers_) {
    worker->Stop();
  }
  for (auto& worker : workers_) {
    worker->Join();
  }
  for (auto& t : executor_threads_) {
    t.join();
  }
  daemon_thread_.join();
  LOG(INFO) << "Frontend server stopped";
}

Frontend::RdmaHandler::RdmaHandler(Frontend& outer) : outer_(outer) {}

void Frontend::RdmaHandler::OnConnected(ario::RdmaQueuePair* conn) {
  if (!outer_.dispatcher_conn_) {
    outer_.promise_dispatcher_conn_.set_value(conn);
    return;
  }
  if (conn->peer_ip() == outer_.dispatcher_conn_->peer_ip() &&
      conn->peer_tcp_port() == outer_.model_worker_port_) {
    outer_.promise_model_worker_conn_.set_value(conn);
    return;
  }

  // from Backend
  auto key = conn->peer_ip() + ':' + std::to_string(conn->peer_tcp_port());
  BackendInfo backend_info;
  {
    std::lock_guard<std::mutex> lock(outer_.connecting_backends_mutex_);
    auto iter = outer_.connecting_backends_.find(key);
    if (iter == outer_.connecting_backends_.end()) {
      LOG(FATAL) << "Cannot find BackendInfo for " << key;
    }
    backend_info = std::move(iter->second);
    outer_.connecting_backends_.erase(iter);
  }
  auto backend =
      std::make_shared<BackendSession>(backend_info, conn, outer_.rdma_sender_);
  outer_.backend_pool_.AddBackend(backend);

  // Send TellNodeIdMessage
  ControlMessage msg;
  msg.mutable_tell_node_id()->set_node_id(outer_.node_id_);
  outer_.rdma_sender_.SendMessage(conn, msg);

  LOG(INFO) << "Connected to backend_id=" << backend_info.node_id() << " at "
            << key;
}

void Frontend::RdmaHandler::OnRemoteMemoryRegionReceived(
    ario::RdmaQueuePair* conn, uint64_t addr, size_t size) {
  // Do nothing
}

void Frontend::RdmaHandler::OnRdmaReadComplete(ario::RdmaQueuePair* conn,
                                               ario::WorkRequestID wrid,
                                               ario::OwnedMemoryBlock buf) {
  // Do nothing
}

void Frontend::RdmaHandler::OnRecv(ario::RdmaQueuePair* conn,
                                   ario::OwnedMemoryBlock buf) {
  // Use helper executor to handle messages. Prevent starving RDMA event loop.
  auto pbuf = std::make_shared<ario::OwnedMemoryBlock>(std::move(buf));
  outer_.helper_executor_.PostBigCallback(
      [this, conn, pbuf](ario::ErrorCode) {
        OnRecvInternal(conn, std::move(*pbuf));
      },
      ario::ErrorCode::kOk);
}

void Frontend::RdmaHandler::OnRecvInternal(ario::RdmaQueuePair* conn,
                                           ario::OwnedMemoryBlock buf) {
  auto view = buf.AsMessageView();
  ControlMessage msg;
  bool ok = msg.ParseFromArray(view.bytes(), view.bytes_length());
  if (!ok) {
    LOG(ERROR) << "ParseFromArray failed";
    return;
  }
  switch (msg.message_case()) {
    case ControlMessage::MessageCase::kRegisterReply: {
      // from Dispatcher
      outer_.promise_register_reply_.set_value(
          std::move(*msg.mutable_register_reply()));
      break;
    }
    case ControlMessage::MessageCase::kUnregisterReply: {
      // from Dispatcher
      outer_.promise_unregister_reply_.set_value(
          std::move(*msg.mutable_unregister_reply()));
      break;
    }
    case ControlMessage::MessageCase::kCheckAlive: {
      // from Dispatcher
      ControlMessage resp;
      auto* reply = resp.mutable_inform_alive();
      reply->set_node_type(FRONTEND_NODE);
      reply->set_node_id(outer_.node_id_);
      outer_.rdma_sender_.SendMessage(conn, resp);
      break;
    }
    case ControlMessage::MessageCase::kAddModelReply: {
      // from Dispatcher
      outer_.promise_add_model_reply_.set_value(
          std::move(*msg.mutable_add_model_reply()));
      break;
    }
    case ControlMessage::MessageCase::kUpdateBackendList: {
      // from Dispatcher
      outer_.UpdateBackendList(msg.update_backend_list());
      break;
    }
    case ControlMessage::MessageCase::kDispatchReply: {
      // from Dispatcher
      outer_.HandleDispatcherReply(msg.dispatch_reply());
      break;
    }
    case ControlMessage::ControlMessage::kQueryResult: {
      outer_.HandleQueryResult(msg.query_result());
      break;
    }
    default:
      LOG(FATAL) << "Unhandled message: " << msg.DebugString();
  }
}

void Frontend::RdmaHandler::OnSent(ario::RdmaQueuePair* conn,
                                   ario::OwnedMemoryBlock buf) {
  // Do nothing
}

void Frontend::RdmaHandler::OnError(ario::RdmaQueuePair* conn,
                                    ario::RdmaError error) {
  LOG(ERROR) << "TODO: Frontend::RdmaHandler::OnError";
}

void Frontend::HandleAccept() {
  auto conn = std::make_shared<UserSession>(std::move(socket_), this);
  connection_pool_.insert(conn);
  conn->Start();
}

void Frontend::HandleConnected(std::shared_ptr<Connection> conn) {}

void Frontend::HandleMessage(std::shared_ptr<Connection> conn,
                             std::shared_ptr<Message> message) {
  switch (message->type()) {
    case kUserRegister: {
      auto user_sess = std::dynamic_pointer_cast<UserSession>(conn);
      if (user_sess == nullptr) {
        LOG(ERROR) << "UserRequest message comes from non-user connection";
        break;
      }
      RequestProto request;
      ReplyProto reply;
      message->DecodeBody(&request);
      RegisterUser(user_sess, request, &reply);
      auto reply_msg =
          std::make_shared<Message>(kUserReply, reply.ByteSizeLong());
      reply_msg->EncodeBody(reply);
      user_sess->Write(reply_msg);
      break;
    }
    case kUserRequest: {
      auto user_sess = std::dynamic_pointer_cast<UserSession>(conn);
      if (user_sess == nullptr) {
        LOG(ERROR) << "UserRequest message comes from non-user connection";
        break;
      }
      auto req =
          std::make_shared<RequestContext>(user_sess, message, request_pool_);
      helper_executor_.PostBigCallback(
          [this, req](ario::ErrorCode) {
            req->PrepareImage(large_buffers_.Allocate());
            request_pool_.AddNewRequest(req);
          },
          ario::ErrorCode::kOk);
      break;
    }
    default: {
      LOG(ERROR) << "Wrong message type: " << message->type();
      // TODO: handle wrong type
      break;
    }
  }
}

std::shared_ptr<ModelHandler> Frontend::GetModel(ModelIndex model_index) const {
  if (model_pool_.size() > model_index.t) {
    return model_pool_[model_index.t];
  }
  return nullptr;
}

void Frontend::HandleQueryResult(const QueryResultProto& result) {
  auto m = GetModel(ModelIndex(result.model_index()));
  if (!m) {
    LOG(ERROR) << "Cannot find model handler for ModelIndex("
               << result.model_index() << ")";
    return;
  }
  m->HandleBackendReply(result);
}

void Frontend::HandleError(std::shared_ptr<Connection> conn,
                           boost::system::error_code ec) {
  if (auto backend_conn = std::dynamic_pointer_cast<BackendSession>(conn)) {
    if (ec == boost::asio::error::eof ||
        ec == boost::asio::error::connection_reset) {
      // backend disconnects
    } else {
      LOG(ERROR) << "Backend connection error (" << ec << "): " << ec.message();
    }
    backend_pool_.RemoveBackend(backend_conn);
  } else {  // user_connection
    if (ec == boost::asio::error::eof ||
        ec == boost::asio::error::connection_reset) {
      // user disconnects
    } else {
      LOG(ERROR) << "User connection error (" << ec << "): " << ec.message();
    }
    auto user_sess = std::dynamic_pointer_cast<UserSession>(conn);
    std::lock_guard<std::mutex> lock(user_mutex_);
    connection_pool_.erase(conn);
    uint32_t uid = user_sess->user_id();
    user_sessions_.erase(uid);
    VLOG(1) << "Remove user session " << uid;
    conn->Stop();
  }
}

std::shared_ptr<UserSession> Frontend::GetUserSession(uint32_t uid) {
  std::lock_guard<std::mutex> lock(user_mutex_);
  auto itr = user_sessions_.find(uid);
  if (itr == user_sessions_.end()) {
    return nullptr;
  }
  return itr->second;
}

std::shared_ptr<ModelHandler> Frontend::LoadModel(LoadModelRequest req) {
  ControlMessage request;
  request.mutable_add_model()->CopyFrom(req);

  // Use a lock to ensure there's only one connection being estabilished.
  std::unique_lock lock(model_worker_estabilish_mutex_);

  // Send AddModel to Dispatcher
  rdma_sender_.SendMessage(dispatcher_conn_, request);
  auto reply = std::move(promise_add_model_reply_.get_future().get());
  promise_add_model_reply_ = {};
  if (reply.status() != CTRL_OK) {
    LOG(ERROR) << "Load model error: " << CtrlStatus_Name(reply.status());
    return nullptr;
  }
  ModelIndex model_index(reply.model_index());

  // Connect to the ModelWorker.
  model_worker_port_ = reply.model_worker_port();
  LOG(INFO) << "Connecting to ModelWorker. port " << model_worker_port_;
  rdma_.ConnectTcp(dispatcher_ip_, model_worker_port_);
  auto* model_worker_conn = promise_model_worker_conn_.get_future().get();
  LOG(INFO) << "ModelWorker connected.";
  model_worker_port_ = 0;
  promise_model_worker_conn_ = {};

  lock.unlock();

  // Sanity check
  auto m = GetModel(model_index);
  CHECK(!m) << "Model already loaded. model_index: " << model_index.t
            << " loaded: " << m->model_session_id();

  // Add to model pool
  auto model_handler = std::make_shared<ModelHandler>(
      req.model_session(), model_index, backend_pool_, node_id_,
      model_worker_conn, rdma_sender_, &large_buffers_);
  if (model_pool_.size() <= model_index.t) {
    model_pool_.resize(model_index.t + 1);
  }
  model_pool_[model_index.t] = model_handler;

  return model_handler;
}

void Frontend::ComplexQuerySetup(const nexus::ComplexQuerySetupRequest& req) {
  LOG(FATAL) << "Frontend::ComplexQuerySetup not supported.";
}

void Frontend::ComplexQueryAddEdge(
    const nexus::ComplexQueryAddEdgeRequest& req) {
  LOG(FATAL) << "Frontend::ComplexQueryAddEdge not supported.";
}

void Frontend::Register() {
  // Init node id
  std::uniform_int_distribution<uint32_t> dis(
      1, std::numeric_limits<uint32_t>::max());
  node_id_ = dis(rand_gen_);

  // Prepare request
  ControlMessage msg;
  auto& request = *msg.mutable_register_request();
  request.set_node_type(FRONTEND_NODE);
  request.set_node_id(node_id_);
  request.set_port(rdma_tcp_server_port_);

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

void Frontend::Unregister() {
  ControlMessage msg;
  auto& request = *msg.mutable_unregister_request();
  request.set_node_type(FRONTEND_NODE);
  request.set_node_id(node_id_);

  rdma_sender_.SendMessage(dispatcher_conn_, msg);
  auto reply = std::move(promise_unregister_reply_.get_future().get());
  CtrlStatus ret = reply.status();
  if (ret != CTRL_OK) {
    LOG(ERROR) << "Failed to unregister frontend: " << CtrlStatus_Name(ret);
  }
}

void Frontend::UpdateBackendList(const BackendListUpdates& request) {
  // TODO: remove this rpc when we remove TellNodeIdMessage.
  for (const auto& backend_info : request.backends()) {
    auto key = backend_info.ip() + ':' + std::to_string(backend_info.port());
    {
      std::lock_guard<std::mutex> lock(connecting_backends_mutex_);
      connecting_backends_[key] = backend_info;
    }
    LOG(INFO) << "Connecting to backend_id=" << backend_info.node_id() << " at "
              << key;
    rdma_.ConnectTcp(backend_info.ip(), backend_info.port());
  }
}

void Frontend::HandleDispatcherReply(const DispatchReply& request) {
  auto m = GetModel(ModelIndex(request.model_index()));
  if (!m) {
    LOG(ERROR) << "HandleDispatcherReply cannot find ModelSession: ModelIndex("
               << request.model_index() << ")";
    return;
  }
  m->HandleDispatcherReply(request);
}

void Frontend::RegisterUser(std::shared_ptr<UserSession> user_sess,
                            const RequestProto& request, ReplyProto* reply) {
  uint32_t uid = request.user_id();
  user_sess->set_user_id(uid);
  std::lock_guard<std::mutex> lock(user_mutex_);
  auto itr = user_sessions_.find(uid);
  if (itr == user_sessions_.end()) {
    VLOG(1) << "New user session: " << uid;
    user_sessions_.emplace(uid, user_sess);
  } else if (itr->second != user_sess) {
    VLOG(1) << "Update user session: " << uid;
    user_sessions_[uid] = user_sess;
  }
  reply->set_user_id(uid);
  reply->set_status(CTRL_OK);
}

void Frontend::Daemon() {
  while (running_) {
    auto next_time = Clock::now() + std::chrono::seconds(beacon_interval_sec_);
    WorkloadStatsProto workload_stats;
    workload_stats.set_node_id(node_id_);
    for (const auto& m : model_pool_) {
      if (!m) {
        continue;
      }
      auto history = m->counter()->GetHistory();
      auto model_stats = workload_stats.add_model_stats();
      model_stats->set_model_session_id(m->model_session_id());
      for (auto nreq : history) {
        model_stats->add_num_requests(nreq);
      }
    }
    ReportWorkload(workload_stats);
    std::this_thread::sleep_until(next_time);
  }
}

void Frontend::ReportWorkload(const WorkloadStatsProto& request) {
  // Skip.
  return;
}

}  // namespace app
}  // namespace nexus
