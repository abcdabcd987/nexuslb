#include "nexus/dispatcher/model_worker.h"

#include <glog/logging.h>
#include <pthread.h>

#include <string>

#include "ario/error.h"
#include "nexus/common/config.h"
#include "nexus/common/model_def.h"
#include "nexus/common/util.h"

namespace nexus {
namespace dispatcher {

class ModelWorker::ModelWorkerRdmaHandler : public ario::RdmaEventHandler {
 public:
  explicit ModelWorkerRdmaHandler(ModelWorker& outer) : outer_(outer) {}

  void OnConnected(ario::RdmaQueuePair* conn) override {
    // Do nothing
  }

  void OnRemoteMemoryRegionReceived(ario::RdmaQueuePair* conn, uint64_t addr,
                                    size_t size) override {
    // Do nothing
  }

  void OnRdmaReadComplete(ario::RdmaQueuePair* conn, ario::WorkRequestID wrid,
                          ario::OwnedMemoryBlock buf) override {
    // Do nothing
  }

  void OnRecv(ario::RdmaQueuePair* conn, ario::OwnedMemoryBlock buf) override {
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
      case ControlMessage::MessageCase::kDispatch: {
        // Dispatcher <- Frontend
        ControlMessage resp;
        outer_.HandleDispatch(std::move(*req.mutable_dispatch()),
                              resp.mutable_dispatch_reply(),
                              dispatcher_recv_ns);
        outer_.rdma_sender_.SendMessage(conn, resp);
        break;
      }
      default:
        LOG(FATAL) << "Unhandled message: " << req.DebugString();
    }
  }

  void OnSent(ario::RdmaQueuePair* conn, ario::OwnedMemoryBlock buf) override {
    // Do nothing
  }

  void OnError(ario::RdmaQueuePair* conn, ario::RdmaError error) override {
    LOG(ERROR) << "TODO ModelWorkerRdmaHandler OnError: conn: "
               << conn->peer_ip() << ":" << conn->peer_tcp_port()
               << ", error: " << static_cast<int>(error);
  }

 private:
  ModelWorker& outer_;
};

ModelWorker::ModelWorker(ario::PollerType poller_type,
                         std::optional<int> pin_cpu, std::string rdma_dev,
                         uint16_t tcp_port, GlobalIdIssuer* global_id_issuer)
    : pin_cpu_(pin_cpu),
      rdma_dev_(std::move(rdma_dev)),
      tcp_port_(tcp_port),
      global_id_issuer_(*CHECK_NOTNULL(global_id_issuer)),
      executor_(poller_type),
      rdma_handler_(std::make_unique<ModelWorkerRdmaHandler>(*this)),
      small_buffers_(kSmallBufferPoolBits, kSmallBufferBlockBits),
      rdma_(rdma_dev_, &executor_, rdma_handler_.get(), &small_buffers_),
      rdma_sender_(&small_buffers_) {}

ModelWorker::~ModelWorker() {
  LOG_IF(FATAL, !stop_)
      << "ModelWorker destructored called without calling Stop() earlier.";
  LOG_IF(FATAL, ev_thread_.joinable())
      << "ModelWorker destructored called without calling Join() earlier.";
}

void ModelWorker::Start() {
  ev_thread_ = std::thread([this] {
    char buf[16];
    rdma_.ListenTcp(tcp_port_);
    std::string s;
    if (pin_cpu_.has_value()) {
      PinCpu(*pin_cpu_);
      s = "Pinned on CPU " + std::to_string(*pin_cpu_);
      snprintf(buf, sizeof(buf), "ModelT CPU%2d", *pin_cpu_);
    } else {
      s = "Not CPU pinned.";
      snprintf(buf, sizeof(buf), "ModelT");
    }
    pthread_setname_np(pthread_self(), buf);
    LOG(INFO) << "Starting ModelWorker. Listening on port " << tcp_port_ << ". "
              << s;
    executor_.RunEventLoop();
  });
}

void ModelWorker::Stop() {
  stop_ = true;
  rdma_.Stop();
}

void ModelWorker::Join() { ev_thread_.join(); }

void ModelWorker::AddModelSession(
    std::string model_session_id,
    MultiThreadRankScheduler::RequestEntrance entrance) {
  executor_.Post(
      [this, m = std::move(model_session_id), entrance](ario::ErrorCode) {
        model_session_entrance_table_.erase(m);
        model_session_entrance_table_.try_emplace(m, std::move(entrance));
      },
      ario::ErrorCode::kOk);
}

void ModelWorker::HandleDispatch(DispatchRequest&& request,
                                 DispatchReply* reply,
                                 long dispatcher_recv_ns) {
  *reply->mutable_model_session() = request.model_session();
  reply->set_query_id(request.query_id());
  auto* query_without_input = request.mutable_query_without_input();
  auto* clock = query_without_input->mutable_clock();
  clock->set_dispatcher_recv_ns(dispatcher_recv_ns);

  // Update punch clock
  auto dispatcher_sched_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now().time_since_epoch())
          .count();
  clock->set_dispatcher_sched_ns(dispatcher_sched_ns);

  // Assign GlobalId
  auto global_id = global_id_issuer_.Next();
  query_without_input->set_global_id(global_id.t);

  // Enqueue query
  // PERFORMANCE: model_session_id
  auto model_session_id = ModelSessionToString(request.model_session());
  auto& entrance = model_session_entrance_table_.at(model_session_id);
  auto status = entrance.EnqueueQuery(std::move(request));
  reply->set_status(status);
}

}  // namespace dispatcher
}  // namespace nexus
