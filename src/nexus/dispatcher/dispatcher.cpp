#include "nexus/dispatcher/dispatcher.h"

#include <glog/logging.h>
#include <pthread.h>
#include <sys/socket.h>

#include <algorithm>
#include <boost/functional/hash.hpp>
#include <chrono>
#include <memory>
#include <sstream>
#include <string>

#include "ario/epoll.h"
#include "nexus/common/config.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/common/util.h"
#include "nexus/dispatcher/backend_delegate_impl.h"
#include "nexus/dispatcher/delayed_scheduler.h"
#include "nexus/dispatcher/frontend_delegate_impl.h"
#include "nexus/dispatcher/model_worker.h"
#include "nexus/dispatcher/session_context.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {

Dispatcher::Dispatcher(std::string rdma_dev, uint16_t port,
                       std::vector<int> pin_cpus)
    : rdma_dev_(std::move(rdma_dev)),
      tcp_server_port_(port),
      pin_cpus_(std::move(pin_cpus)),
      rdma_handler_(*this),
      small_buffers_(kSmallBufferPoolBits, kSmallBufferBlockBits),
      rdma_(rdma_dev_, &main_executor_, ario::PollerType::kEventLoop,
            &rdma_handler_, &small_buffers_),
      rdma_sender_(&small_buffers_),
      scheduler_(&main_executor_, &rank_thread_executor_) {
  // Don't Pin the main thread.
  // One CPU for the RankThread. The rest for ModelThreads.
  CHECK_GT(pin_cpus_.size(), 1) << "Need at least two cpus";
  for (size_t i = 1; i < pin_cpus_.size(); ++i) {
    auto m = std::make_unique<ModelWorker>(
        pin_cpus_[i], rdma_dev_, tcp_server_port_ + i, &global_id_issuer_);
    model_workers_.push_back(std::move(m));
  }
  LOG(INFO) << "Allocated " << model_workers_.size() << " ModelWorkers";
}

Dispatcher::~Dispatcher() {
  if (running_) {
    Stop();
  }
}

void Dispatcher::Run() {
  running_ = true;

  rdma_.ListenTcp(tcp_server_port_);
  rank_thread_ = std::thread([this] {
    std::string s;
    if (!pin_cpus_.empty()) {
      s = "Pinned on CPU " + std::to_string(pin_cpus_[0]);
      PinCpu(pin_cpus_[0]);
    } else {
      s = "Not CPU pinned.";
    }
    LOG(INFO) << "Starting RankThread. " << s;
    rank_thread_executor_.RunEventLoop();
  });
  for (auto& model_worker : model_workers_) {
    model_worker->Start();
  }

  // Use the main thread for the main executor
  LOG(INFO) << "Main thread goes into the event loop. Not CPU pinned.";
  main_executor_.RunEventLoop();
}

void Dispatcher::Stop() {
  LOG(INFO) << "Shutting down the dispatcher.";
  running_ = false;

  // Stop everything
  rdma_.Stop();
  for (auto& w : model_workers_) {
    w->Stop();
  }
  scheduler_.Stop();
  rank_thread_executor_.StopEventLoop();
  main_executor_.StopEventLoop();

  // Join all threads.
  LOG(INFO) << "Joining all threads";
  for (auto& w : model_workers_) {
    w->Join();
  }
  rank_thread_.join();
  LOG(INFO) << "Dispatcher stopped";
}

Dispatcher::RdmaHandler::RdmaHandler(Dispatcher& outer) : outer_(outer) {}

void Dispatcher::RdmaHandler::OnConnected(ario::RdmaQueuePair* conn) {
  // Do nothing
}

void Dispatcher::RdmaHandler::OnRemoteMemoryRegionReceived(
    ario::RdmaQueuePair* conn, uint64_t addr, size_t size) {
  // Do nothing.
}

void Dispatcher::RdmaHandler::OnRdmaReadComplete(ario::RdmaQueuePair* conn,
                                                 ario::WorkRequestID wrid,
                                                 ario::OwnedMemoryBlock buf) {
  // Do nothing.
}

void Dispatcher::RdmaHandler::OnRecv(ario::RdmaQueuePair* conn,
                                     ario::OwnedMemoryBlock buf) {
  auto dispatcher_recv_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now().time_since_epoch())
          .count();
  auto view = buf.AsMessageView();
  ControlMessage req;
  bool ok = req.ParseFromArray(view.bytes(), view.bytes_length());
  if (!ok) {
    LOG(ERROR) << "ParseFromArray failed";
    return;
  }
  switch (req.message_case()) {
    case ControlMessage::MessageCase::kRegisterRequest: {
      // Dispatcher <- Frontend/Backend
      ControlMessage resp;
      outer_.HandleRegister(conn, req.register_request(),
                            resp.mutable_register_reply());
      outer_.rdma_sender_.SendMessage(conn, resp);
      break;
    }
    case ControlMessage::MessageCase::kUnregisterRequest: {
      // Dispatcher <- Frontend/Backend
      ControlMessage resp;
      outer_.HandleUnregister(req.unregister_request(),
                              resp.mutable_unregister_reply());
      outer_.rdma_sender_.SendMessage(conn, resp);
      break;
    }
    case ControlMessage::MessageCase::kInformAlive: {
      // Dispatcher <- Frontend/Backend
      outer_.HandleInformAlive(req.inform_alive());
      break;
    }
    case ControlMessage::MessageCase::kAddModel: {
      // Dispatcher <- Frontend
      ControlMessage resp;
      outer_.HandleLoadModel(req.add_model(), resp.mutable_add_model_reply());
      outer_.rdma_sender_.SendMessage(conn, resp);
      break;
    }
    case ControlMessage::MessageCase::kLoadModelReply: {
      // Dispatcher <- Backend
      auto status = req.load_model_reply().status();
      if (status != CtrlStatus::CTRL_OK) {
        LOG(ERROR) << "LoadModelReply error: " << CtrlStatus_Name(status);
      }
      break;
    }
    case ControlMessage::MessageCase::kEnqueueQueryReply: {
      // Dispatcher <- Backend
      auto status = req.enqueue_query_reply().status();
      if (status != CtrlStatus::CTRL_OK) {
        LOG(ERROR) << "EnqueueQueryReply error: " << CtrlStatus_Name(status);
      }
      break;
    }
    case ControlMessage::MessageCase::kEnqueueBatchplanReply: {
      // Dispatcher <- Backend
      auto status = req.enqueue_batchplan_reply().status();
      if (status != CtrlStatus::CTRL_OK) {
        LOG(ERROR) << "EnqueueBatchplanReply error: "
                   << CtrlStatus_Name(status);
      }
      break;
    }
    case ControlMessage::MessageCase::kDispatch:
      [[fallthrough]];
    default:
      LOG(FATAL) << "Unhandled message: " << req.DebugString();
  }
}

void Dispatcher::RdmaHandler::OnSent(ario::RdmaQueuePair* conn,
                                     ario::OwnedMemoryBlock buf) {
  // Do nothing.
}

void Dispatcher::RdmaHandler::OnError(ario::RdmaQueuePair* conn,
                                      ario::RdmaError error) {
  LOG(ERROR) << "TODO: Dispatcher::RdmaHandler::OnError";
}

void Dispatcher::HandleRegister(ario::RdmaQueuePair* conn,
                                const RegisterRequest& request,
                                RegisterReply* reply) {
  LOG(INFO) << "Register server: " << request.DebugString();
  switch (request.node_type()) {
    case NodeType::FRONTEND_NODE: {
      auto frontend = std::make_shared<FrontendDelegateImpl>(
          request.node_id(), conn->peer_ip(), conn->peer_tcp_port(),
          beacon_interval_sec_, conn, rdma_sender_);
      auto frontend_id = NodeId(frontend->node_id());
      if (frontends_.find(frontend_id) != frontends_.end()) {
        reply->set_status(CtrlStatus::CTRL_FRONTEND_NODE_ID_CONFLICT);
        return;
      }
      frontends_[frontend_id] = frontend;

      // Add frontend for the scheduler.
      scheduler_.AddFrontend(frontend_id, frontend);

      // UpdateBackendList
      BackendListUpdates update;
      for (auto iter : backends_) {
        update.add_backends()->CopyFrom(iter.second->backend_info());
      }
      VLOG(1) << "Send UpdateBackendList: frontend_id=" << frontend->node_id();
      frontend->UpdateBackendList(std::move(update));
      VLOG(1) << "Finish sending UpdateBackendList: frontend_id="
              << frontend->node_id();

      reply->set_status(CtrlStatus::CTRL_OK);
      reply->set_beacon_interval_sec(BEACON_INTERVAL_SEC);
      VLOG(1) << "Finish registering frontend_id=" << frontend->node_id();
      break;
    }
    case NodeType::BACKEND_NODE: {
      auto backend = std::make_shared<BackendDelegateImpl>(
          request.node_id(), conn->peer_ip(), request.port(),
          request.gpu_device_name(), request.gpu_uuid(),
          request.gpu_available_memory(), beacon_interval_sec_, conn,
          rdma_sender_);
      auto backend_id = NodeId(backend->node_id());
      if (backends_.find(backend_id) != backends_.end()) {
        reply->set_status(CtrlStatus::CTRL_BACKEND_NODE_ID_CONFLICT);
        return;
      }
      backends_[backend_id] = backend;

      // Add backend for the scheduler.
      scheduler_.AddBackend(backend_id, backend);

      // Load Models
      for (auto iter : sessions_) {
        const auto& model_session = iter.second->model_session();
        auto profile_id = ModelSessionToProfileID(model_session);
        auto* profile = ModelDatabase::Singleton().GetModelProfile(
            backend->gpu_device(), backend->gpu_uuid(), profile_id);
        if (!profile) {
          reply->set_status(CtrlStatus::CTRL_INVALID_LOAD_MODEL_REQUEST);
          continue;
        }

        uint32_t max_batch =
            profile->GetMaxBatchWithFullBudget(model_session.latency_sla());

        // LoadModel RPC
        VLOG(1) << "SendLoadModelCommand: backend_id=" << backend->node_id()
                << ", model_session=" << iter.first;
        backend->SendLoadModelCommand(model_session, max_batch);
        VLOG(1) << "Finish SendLoadModelCommand: backend_id="
                << backend->node_id() << ", model_session=" << iter.first;
      }

      // UpdateBackendList
      BackendListUpdates update;
      update.add_backends()->CopyFrom(backend->backend_info());
      for (auto iter : frontends_) {
        VLOG(1) << "UpdateBackendList (adding backend_id=" << backend->node_id()
                << "): frontend_id=" << iter.second->node_id();
        iter.second->UpdateBackendList(std::move(update));
        VLOG(1) << "Finish UpdateBackendList (adding backend_id="
                << backend->node_id()
                << "): frontend_id=" << iter.second->node_id();
      }

      reply->set_status(CtrlStatus::CTRL_OK);
      reply->set_beacon_interval_sec(BEACON_INTERVAL_SEC);
      VLOG(1) << "Finish registering backend_id=" << backend->node_id();
      break;
    }
    default: {
      LOG(ERROR) << "Unknown node type: " << NodeType_Name(request.node_type());
      reply->set_status(CtrlStatus::CTRL_SERVER_NOT_REGISTERED);
    }
  }
}

void Dispatcher::HandleUnregister(const UnregisterRequest& request,
                                  RpcReply* reply) {
  // TODO
  LOG(ERROR) << "HandleUnregister not implemented. Request: "
             << request.DebugString();
  reply->set_status(CtrlStatus::CTRL_OK);
}

void Dispatcher::HandleLoadModel(const LoadModelRequest& request,
                                 LoadModelReply* reply) {
  auto model_info = ModelDatabase::Singleton().GetModelInfo(
      ModelSessionToModelID(request.model_session()));
  if (!model_info) {
    LOG(ERROR) << "HandleLoadModel: model not found. model="
               << ModelSessionToModelID(request.model_session());
    reply->set_status(CtrlStatus::MODEL_NOT_FOUND);
    return;
  }

  auto model_sess_id = ModelSessionToString(request.model_session());
  VLOG(1) << "HandleLoadModel: model_sess_id=" << model_sess_id;
  {
    auto it = sessions_.find(model_sess_id);
    if (it != sessions_.end()) {
      // Model already loaded. Just skip.
      reply->set_status(CtrlStatus::CTRL_OK);
      return;
    }
  }
  reply->set_status(CtrlStatus::CTRL_OK);

  // Add the model session
  auto sctx = std::make_shared<ModelSessionContext>(request.model_session());
  sessions_[model_sess_id] = sctx;

  // Add model session for the scheduler
  auto& model_worker = GetModelWorker(request.model_session());
  auto entrance = scheduler_.AddModelSession(model_worker.executor(),
                                             request.model_session());
  model_worker.AddModelSession(model_sess_id, entrance);
  reply->set_model_worker_port(model_worker.tcp_port());

  // Ask backends to load the model
  auto profile_id = ModelSessionToProfileID(request.model_session());
  for (auto backend_iter : backends_) {
    auto backend = backend_iter.second;
    auto* profile = ModelDatabase::Singleton().GetModelProfile(
        backend->gpu_device(), backend->gpu_uuid(), profile_id);
    if (!profile) {
      reply->set_status(CtrlStatus::CTRL_INVALID_LOAD_MODEL_REQUEST);
      continue;
    }
    uint32_t max_batch = profile->GetMaxBatchWithFullBudget(
        request.model_session().latency_sla());

    // LoadModel RPC
    VLOG(1) << "SendLoadModelCommand: backend_id=" << backend->node_id()
            << ", model_session=" << model_sess_id;
    backend->SendLoadModelCommand(request.model_session(), max_batch);
    VLOG(1) << "Finish SendLoadModelCommand: backend_id=" << backend->node_id()
            << ", model_session=" << model_sess_id;
  }
}

void Dispatcher::HandleInformAlive(const KeepAliveRequest& request) {
  auto node_id = NodeId(request.node_id());
  switch (request.node_type()) {
    case NodeType::FRONTEND_NODE: {
      auto it = frontends_.find(node_id);
      if (it == frontends_.end()) {
        LOG(ERROR) << "InformAlive: Frontend not registered. node_id="
                   << node_id.t;
      } else {
        it->second->Tick();
        return;
      }
      break;
    }
    case NodeType::BACKEND_NODE: {
      auto it = backends_.find(node_id);
      if (it == backends_.end()) {
        LOG(ERROR) << "InformAlive: Backend not registered. node_id="
                   << node_id.t;
      } else {
        it->second->Tick();
      }
      break;
    }
    default: {
      LOG(ERROR) << "InformAlive: Unknown node type: " << request.node_type()
                 << " node_id=" << request.node_id();
    }
  }
}

ModelWorker& Dispatcher::GetModelWorker(
    const ModelSession& model_session) const {
  size_t h = 0;
  boost::hash_combine(h, model_session.framework());
  boost::hash_combine(h, model_session.model_name());
  boost::hash_combine(h, model_session.latency_sla());
  return *model_workers_.at(h % model_workers_.size());
}

}  // namespace dispatcher
}  // namespace nexus
