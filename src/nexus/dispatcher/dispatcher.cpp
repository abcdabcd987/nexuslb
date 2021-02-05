#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "nexus/dispatcher/dispatcher.h"

#include <glog/logging.h>
#include <pthread.h>
#include <sys/socket.h>

#include <algorithm>
#include <chrono>
#include <sstream>

#include "nexus/common/config.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/accessor.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/dispatcher/delayed_scheduler.h"
#include "nexus/dispatcher/frontend_delegate.h"
#include "nexus/dispatcher/session_context.h"
#include "nexus/proto/control.pb.h"

namespace {
void PinCpu(pthread_t thread, int cpu) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  int rc = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
  LOG_IF(FATAL, rc != 0) << "Error calling pthread_setaffinity_np: " << rc;
}
}  // namespace

namespace nexus {
namespace dispatcher {

Dispatcher::Dispatcher(std::string rdma_dev, uint16_t port,
                       std::vector<int> pin_cpus)
    : rdma_dev_(std::move(rdma_dev)),
      tcp_server_port_(port),
      pin_cpus_(std::move(pin_cpus)),
      rdma_handler_(*this),
      small_buffers_(kSmallBufferPoolBits, kSmallBufferBlockBits),
      rdma_(rdma_dev_, &rdma_handler_, &small_buffers_),
      rdma_sender_(&small_buffers_),
      scheduler_(DispatcherAccessor(*this)) {
  CHECK(!pin_cpus_.empty()) << "Currently CPU pinning is not supported.";
}

Dispatcher::~Dispatcher() {
  if (running_) {
    Stop();
  }
}

void Dispatcher::Run() {
  running_ = true;

  rdma_.ListenTcp(tcp_server_port_);
  rdma_ev_thread_ = std::thread(&ario::RdmaManager::RunEventLoop, &rdma_);

  // Start a single threaded scheduler.
  scheduler_threads_.emplace_back(&decltype(scheduler_)::RunAsWorker,
                                  &scheduler_);

  // Nothing to do here
  for (;;) {
    std::this_thread::sleep_for(std::chrono::hours(24));
  }
}

void Dispatcher::Stop() {
  LOG(INFO) << "Shutting down the dispatcher.";
  running_ = false;

  // Stop everything
  rdma_.StopEventLoop();
  scheduler_.Stop();

  // Join all threads.
  for (auto& thread : scheduler_threads_) {
    thread.join();
  }
  rdma_ev_thread_.join();
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
      ControlMessage resp;
      outer_.HandleRegister(conn, req.register_request(),
                            resp.mutable_register_reply());
      outer_.rdma_sender_.SendMessage(conn, resp);
      break;
    }
    case ControlMessage::MessageCase::kUnregisterRequest: {
      ControlMessage resp;
      outer_.HandleUnregister(req.unregister_request(),
                              resp.mutable_unregister_reply());
      outer_.rdma_sender_.SendMessage(conn, resp);
      break;
    }
    case ControlMessage::MessageCase::kAddModel: {
      ControlMessage resp;
      outer_.HandleLoadModel(req.add_model(), resp.mutable_add_model_reply());
      outer_.rdma_sender_.SendMessage(conn, resp);
      break;
    }
    case ControlMessage::MessageCase::kInformAlive: {
      outer_.HandleInformAlive(req.inform_alive());
      break;
    }
    case ControlMessage::MessageCase::kDispatch: {
      ControlMessage resp;
      outer_.HandleDispatch(req.mutable_dispatch(),
                            resp.mutable_dispatch_reply(), dispatcher_recv_ns);
      outer_.rdma_sender_.SendMessage(conn, resp);
      break;
    }
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

void Dispatcher::HandleDispatch(DispatchRequest* request, DispatchReply* reply,
                                long dispatcher_recv_ns) {
  *reply->mutable_model_session() = request->model_session();
  reply->set_query_id(request->query_id());
  QueryProto query_without_input;
  query_without_input.Swap(request->mutable_query_without_input());
  query_without_input.mutable_clock()->set_dispatcher_recv_ns(
      dispatcher_recv_ns);

  // Update punch clock
  auto dispatcher_sched_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now().time_since_epoch())
          .count();
  query_without_input.mutable_clock()->set_dispatcher_sched_ns(
      dispatcher_sched_ns);

  // Assign GlobalId
  auto global_id = next_global_id_.fetch_add(1);
  query_without_input.set_global_id(global_id);

  // Enqueue query
  auto status = scheduler_.EnqueueQuery(std::move(query_without_input));
  reply->set_status(status);
}

void Dispatcher::HandleRegister(ario::RdmaQueuePair* conn,
                                const RegisterRequest& request,
                                RegisterReply* reply) {
  LOG(INFO) << "Register server: " << request.DebugString();
  switch (request.node_type()) {
    case NodeType::FRONTEND_NODE: {
      auto frontend = std::make_shared<FrontendDelegate>(
          request.node_id(), conn->peer_ip(), conn->peer_tcp_port(),
          beacon_interval_sec_, conn, rdma_sender_);
      {
        std::lock_guard<std::mutex> lock(mutex_);
        auto frontend_id = NodeId(frontend->node_id());
        if (frontends_.find(frontend_id) != frontends_.end()) {
          reply->set_status(CtrlStatus::CTRL_FRONTEND_NODE_ID_CONFLICT);
          return;
        }
        frontends_[frontend_id] = frontend;
      }

      // UpdateBackendList
      BackendListUpdates update;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto iter : backends_) {
          update.add_backends()->CopyFrom(iter.second->backend_info());
        }
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
      auto backend = std::make_shared<BackendDelegate>(
          request.node_id(), conn->peer_ip(), conn->peer_tcp_port(),
          request.gpu_device_name(), request.gpu_uuid(),
          request.gpu_available_memory(), beacon_interval_sec_, conn,
          rdma_sender_);
      auto backend_id = NodeId(backend->node_id());
      std::unordered_map<std::string, std::shared_ptr<ModelSessionContext>>
          sessions;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (backends_.find(backend_id) != backends_.end()) {
          reply->set_status(CtrlStatus::CTRL_BACKEND_NODE_ID_CONFLICT);
          return;
        }
        backends_[backend_id] = backend;
        sessions = sessions_;
      }

      // Add backend for the scheduler.
      scheduler_.AddBackend(backend_id);

      // Load Models
      for (auto iter : sessions) {
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
      decltype(frontends_) frontends;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        frontends = frontends_;
      }
      for (auto iter : frontends) {
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
    std::lock_guard<std::mutex> lock(mutex_);
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
  }

  // Add model session for the scheduler
  scheduler_.AddModelSession(request.model_session());

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
  std::lock_guard<std::mutex> lock(mutex_);
  auto node_id = NodeId(request.node_id());
  switch (request.node_type()) {
    case NodeType::FRONTEND_NODE: {
      auto it = frontends_.find(node_id);
      if (it == frontends_.end()) {
        LOG(ERROR) << "InformAlive: Server not registered. node_id="
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
        LOG(ERROR) << "InformAlive: Server not registered. node_id="
                   << node_id.t;
      } else {
        it->second->Tick();
      }
      break;
    }
    default: {
      LOG(ERROR) << "InformAlive: Unknown node type: " << request.node_type();
    }
  }
}

}  // namespace dispatcher
}  // namespace nexus
