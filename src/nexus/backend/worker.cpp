#include "nexus/backend/worker.h"

#include <glog/logging.h>
#include <pthread.h>

#include <chrono>

#include "nexus/backend/backend_server.h"
#include "nexus/backend/model_ins.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace backend {

Worker::Worker(int index, BackendServer* server, RdmaSender rdma_sender,
               BlockPriorityQueue<Task>& task_queue)
    : index_(index),
      server_(server),
      rdma_sender_(rdma_sender),
      task_queue_(task_queue),
      running_(false) {}

void Worker::Start(int core) {
  running_ = true;
  thread_ = std::thread(&Worker::Run, this);
  if (core >= 0) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(thread_.native_handle(), sizeof(cpu_set_t),
                                    &cpuset);
    if (rc != 0) {
      LOG(ERROR) << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
    LOG(INFO) << "Worker " << index_ << " is pinned on CPU " << core;
  }
}

void Worker::Stop() {
  running_ = false;
  if (thread_.joinable()) {
    thread_.join();
  }
}

void Worker::Run() {
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  LOG(INFO) << "Worker " << index_ << " starts";
  auto timeout = std::chrono::milliseconds(50);
  while (running_) {
    std::shared_ptr<Task> task = task_queue_.pop(timeout);
    if (task == nullptr) {
      continue;
    }
    Process(task);
  }
  LOG(INFO) << "Worker " << index_ << " stopped";
}

void Worker::Process(std::shared_ptr<Task> task) {
  switch (task->stage) {
    case kPreprocess: {
      task->model = server_->GetModel(task->query.model_session_id());
      if (task->model == nullptr) {
        std::stringstream ss;
        ss << "Model session is not loaded: " << task->query.model_session_id();
        task->result.set_status(MODEL_SESSION_NOT_LOADED);
        SendReply(std::move(task));
        break;
      }
      // Preprocess task
      if (task->model->Preprocess(task)) {
        if (task->plan_id.has_value()) {
          server_->MarkBatchPlanQueryPreprocessed(task);
        }
      } else {
        if (task->result.status() != CTRL_OK) {
          SendReply(std::move(task));
        } else {
          task->model->Preprocess(task, true);
        }
      }
      break;
    }
    case kPostprocess: {
      if (task->result.status() != CTRL_OK) {
        SendReply(std::move(task));
      } else {
        task->model->Postprocess(task);
        SendReply(std::move(task));
      }
      break;
    }
    default:
      LOG(ERROR) << "Wrong task stage: " << task->stage;
  }
}

void Worker::SendReply(std::shared_ptr<Task> task) {
  task->timer.Record("end");
  task->result.set_query_id(task->query.query_id());
  task->result.set_model_session_id(task->query.model_session_id());
  task->result.set_latency_us(task->timer.GetLatencyMicros("begin", "end"));
  task->result.set_queuing_us(task->timer.GetLatencyMicros("begin", "exec"));
  if (task->model != nullptr && task->model->backup()) {
    task->result.set_use_backup(true);
  } else {
    task->result.set_use_backup(false);
  }

  auto backend_reply_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              Clock::now().time_since_epoch())
                              .count();
  task->query.mutable_clock()->set_backend_reply_ns(backend_reply_ns);
  task->result.mutable_clock()->CopyFrom(task->query.clock());

  MessageType reply_type = kBackendReply;
  if (task->msg_type == kBackendRelay) {
    reply_type = kBackendRelayReply;
  }
  ControlMessage resp;
  // TODO: avoid copy.
  resp.mutable_query_result()->CopyFrom(task->result);
  rdma_sender_.SendMessage(task->connection, resp);
}

}  // namespace backend
}  // namespace nexus
