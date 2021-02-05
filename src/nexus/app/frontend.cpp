#include "nexus/app/frontend.h"

#include <gflags/gflags.h>

#include <boost/asio.hpp>
#include <limits>
#include <mutex>
#include <string>

#include "nexus/common/config.h"
#include "nexus/common/model_def.h"
#include "nexus/proto/control.pb.h"

DECLARE_int32(load_balance);

namespace nexus {
namespace app {

Frontend::Frontend(std::string rdma_dev, uint16_t rdma_tcp_server_port,
                   std::string nexus_server_port, std::string sch_addr)
    : ServerBase(nexus_server_port),
      rdma_tcp_server_port_(rdma_tcp_server_port),
      rdma_handler_(*this),
      small_buffers_(kSmallBufferPoolBits, kSmallBufferBlockBits),
      large_buffers_(kLargeBufferPoolBits, kLargeBufferBlockBits),
      rdma_(rdma_dev, &rdma_handler_, &small_buffers_),
      rdma_sender_(&small_buffers_),
      rd_(),
      rand_gen_(rd_()) {
  rdma_.ExposeMemory(large_buffers_.data(), large_buffers_.pool_size());
  rdma_.ListenTcp(rdma_tcp_server_port);

  uint16_t sch_port;
  auto sch_colon_idx = sch_addr.find(':');
  if (sch_colon_idx == std::string::npos) {
    sch_port = SCHEDULER_DEFAULT_PORT;
  } else {
    sch_port = std::stoi(sch_addr.substr(sch_colon_idx + 1));
    sch_addr.resize(sch_colon_idx);
  }

  rdma_.ConnectTcp(sch_addr, sch_port);
  rdma_ev_thread_ = std::thread(&ario::RdmaManager::RunEventLoop, &rdma_);
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
  for (size_t i = 0; i < nthreads; ++i) {
    std::unique_ptr<Worker> worker(new Worker(qp, request_pool_));
    worker->Start();
    workers_.push_back(std::move(worker));
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
  rdma_.StopEventLoop();
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
  daemon_thread_.join();
  rdma_ev_thread_.join();
  LOG(INFO) << "Frontend server stopped";
}

Frontend::RdmaHandler::RdmaHandler(Frontend& outer) : outer_(outer) {}

void Frontend::RdmaHandler::OnConnected(ario::RdmaQueuePair* conn) {
  if (!outer_.dispatcher_conn_) {
    outer_.promise_dispatcher_conn_.set_value(conn);
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
                                               ario::OwnedMemoryBlock buf) {
  // Do nothing
}

void Frontend::RdmaHandler::OnRecv(ario::RdmaQueuePair* conn,
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
    case ControlMessage::MessageCase::kFetchImageRequest: {
      // from Backend
      // TODO: replace with READ
      ControlMessage resp;
      outer_.FetchImage(msg.fetch_image_request(),
                        resp.mutable_fetch_image_reply());
      outer_.rdma_sender_.SendMessage(conn, resp);
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
      request_pool_.AddNewRequest(
          std::make_shared<RequestContext>(user_sess, message, request_pool_));
      break;
    }
    default: {
      LOG(ERROR) << "Wrong message type: " << message->type();
      // TODO: handle wrong type
      break;
    }
  }
}

void Frontend::HandleQueryResult(const QueryResultProto& result) {
  std::string model_session_id = result.model_session_id();
  auto itr = model_pool_.find(model_session_id);
  if (itr == model_pool_.end()) {
    LOG(ERROR) << "Cannot find model handler for " << model_session_id;
    return;
  }
  itr->second->HandleBackendReply(result);
}

void Frontend::FetchImage(const FetchImageRequest& request,
                          FetchImageReply* reply) {
  auto iter = model_pool_.find(request.model_session_id());
  if (iter == model_pool_.end()) {
    LOG(ERROR) << "Cannot find model handler for "
               << request.model_session_id();
    return;
  }
  auto qid = request.query_id();
  VLOG(1) << "kFetchImageRequest: model_session=" << request.model_session_id()
          << ", query_id=" << qid;
  reply->set_global_id(request.global_id());
  bool ok = iter->second->FetchImage(QueryId(qid), reply->mutable_input());
  if (ok) {
    reply->set_status(CtrlStatus::CTRL_OK);
  } else {
    reply->set_status(CtrlStatus::CTRL_IMAGE_NOT_FOUND);
    LOG(ERROR) << "FetchImage not found. model_session_id="
               << request.model_session_id()
               << ", global_id=" << request.global_id();
  }
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
  return LoadModel(std::move(req), LoadBalancePolicy(FLAGS_load_balance));
}

std::shared_ptr<ModelHandler> Frontend::LoadModel(LoadModelRequest req,
                                                  LoadBalancePolicy lb_policy) {
  ControlMessage request;
  request.mutable_add_model()->CopyFrom(req);
  rdma_sender_.SendMessage(dispatcher_conn_, request);
  auto reply = std::move(promise_add_model_reply_.get_future().get());
  if (reply.status() != CTRL_OK) {
    LOG(ERROR) << "Load model error: " << CtrlStatus_Name(reply.status());
    return nullptr;
  }
  auto model_session_id = ModelSessionToString(req.model_session());
  auto model_handler =
      std::make_shared<ModelHandler>(model_session_id, backend_pool_, lb_policy,
                                     node_id_, dispatcher_conn_, rdma_sender_);
  // Only happens at Setup stage, so no concurrent modification to model_pool_
  model_pool_.emplace(model_handler->model_session_id(), model_handler);
  // UpdateBackendPoolAndModelRoute(reply.model_route());

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
  auto model_session_id = ModelSessionToString(request.model_session());
  auto iter = model_pool_.find(model_session_id);
  if (iter == model_pool_.end()) {
    LOG(ERROR) << "HandleDispatcherReply cannot find ModelSession: "
               << model_session_id;
    return;
  }
  iter->second->HandleDispatcherReply(request);
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
    for (auto const& iter : model_pool_) {
      auto model_session_id = iter.first;
      auto history = iter.second->counter()->GetHistory();
      auto model_stats = workload_stats.add_model_stats();
      model_stats->set_model_session_id(model_session_id);
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
