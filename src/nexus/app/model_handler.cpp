#include "nexus/app/model_handler.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <typeinfo>

#include "nexus/app/request_context.h"
#include "nexus/common/model_def.h"
#include "nexus/proto/control.pb.h"

DEFINE_int32(count_interval, 1, "Interval to count number of requests in sec");

namespace nexus {
namespace app {

QueryResult::QueryResult(uint64_t qid) : qid_(qid), ready_(false) {}

uint32_t QueryResult::status() const {
  CheckReady();
  return status_;
}

std::string QueryResult::error_message() const {
  CheckReady();
  return error_message_;
}

void QueryResult::ToProto(ReplyProto* reply) const {
  CheckReady();
  reply->set_status(status_);
  if (status_ != CTRL_OK) {
    reply->set_error_message(error_message_);
  } else {
    for (auto record : records_) {
      auto rec_p = reply->add_output();
      record.ToProto(rec_p);
    }
  }
}

const Record& QueryResult::operator[](uint32_t idx) const {
  CheckReady();
  return records_.at(idx);
}

uint32_t QueryResult::num_records() const {
  CheckReady();
  return records_.size();
}

void QueryResult::CheckReady() const {
  CHECK(ready_) << "Rpc reply for query " << qid_ << " is not ready yet";
}

void QueryResult::SetResult(const QueryResultProto& result) {
  status_ = result.status();
  if (status_ != CTRL_OK) {
    error_message_ = result.error_message();
  } else {
    for (auto record : result.output()) {
      records_.emplace_back(record);
    }
  }
  ready_ = true;
}

void QueryResult::SetError(uint32_t status, const std::string& error_msg) {
  status_ = status;
  error_message_ = error_msg;
  ready_ = true;
}

std::atomic<uint64_t> ModelHandler::global_query_id_(0);

ModelHandler::ModelHandler(ModelSession model_session, ModelIndex model_index,
                           BackendPool& pool, NodeId frontend_id,
                           ario::RdmaQueuePair* model_worker_conn,
                           RdmaSender rdma_sender,
                           ario::MemoryBlockAllocator* input_memory_allocator)
    : frontend_id_(frontend_id),
      model_session_(std::move(model_session)),
      model_session_id_(ModelSessionToString(model_session_)),
      model_index_(model_index),
      backend_pool_(pool),
      model_worker_conn_(model_worker_conn),
      rdma_sender_(rdma_sender),
      input_memory_allocator_(input_memory_allocator),
      total_throughput_(0.),
      rd_(),
      rand_gen_(rd_()) {
  counter_ =
      MetricRegistry::Singleton().CreateIntervalCounter(FLAGS_count_interval);
  LOG(INFO) << "Created ModelHandler " << model_session_id_ << " ModelIndex "
            << model_index.t;
}

ModelHandler::~ModelHandler() {
  MetricRegistry::Singleton().RemoveMetric(counter_);
}

std::shared_ptr<QueryResult> ModelHandler::Execute(
    std::shared_ptr<RequestContext> ctx, const ValueProto& input,
    std::vector<std::string> output_fields, uint32_t topk,
    std::vector<RectProto> windows) {
  uint64_t qid = global_query_id_.fetch_add(1, std::memory_order_relaxed);
  counter_->Increase(1);
  {
    std::lock_guard<std::mutex> lock(query_ctx_mu_);
    query_ctx_.emplace(qid, ctx);
  }

  // Build the query proto
  QueryProto query, query_without_input;
  query.set_query_id(qid);
  query.set_model_index(model_index_);
  query.set_frontend_id(frontend_id_.t);
  for (auto field : output_fields) {
    query.add_output_field(field);
  }
  if (topk > 0) {
    query.set_topk(topk);
  }
  for (auto rect : windows) {
    query.add_window()->CopyFrom(rect);
  }
  if (ctx->slack_ms() > 0) {
    query.set_slack_ms(int(floor(ctx->slack_ms())));
  }

  // Punch clock
  auto* clock = query.mutable_clock();
  auto frontend_recv_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              ctx->frontend_recv_time().time_since_epoch())
                              .count();
  clock->set_frontend_recv_ns(frontend_recv_ns);
  auto frontend_dispatch_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now().time_since_epoch())
          .count();
  clock->set_frontend_dispatch_ns(frontend_dispatch_ns);

  // Move `query`
  query_without_input.CopyFrom(query);
  query.mutable_input()->CopyFrom(input);
  ctx->SetBackendQueryProto(std::move(query),
                            input_memory_allocator_->Allocate());

  // Send to dispatcher
  ControlMessage msg;
  auto* request = msg.mutable_dispatch();
  request->set_model_index(model_index_.t);
  *request->mutable_query_without_input() = std::move(query_without_input);
  request->set_query_id(qid);
  request->set_rdma_read_offset(ctx->rdma_read_offset());
  request->set_rdma_read_length(ctx->rdma_read_length());
  rdma_sender_.SendMessage(model_worker_conn_, msg);

  auto reply = std::make_shared<QueryResult>(qid);
  return reply;
}

void ModelHandler::HandleBackendReply(const QueryResultProto& result) {
  std::shared_ptr<RequestContext> ctx;
  {
    std::lock_guard<std::mutex> lock(query_ctx_mu_);
    auto qid = QueryId(result.query_id());
    auto iter = query_ctx_.find(qid);
    if (iter == query_ctx_.end()) {
      // FIXME why this happens? lower from FATAL to ERROR temporarily
      LOG(ERROR) << model_session_id_ << " cannot find query context for query "
                 << qid.t;
      return;
    }
    ctx = iter->second;
    query_ctx_.erase(iter);
  }
  ctx->HandleQueryResult(result, model_session_);
}

void ModelHandler::HandleDispatcherReply(const DispatchReply& reply) {
  if (reply.status() == CtrlStatus::CTRL_OK) {
    // Do nothing. Dispatcher will send the query to the backend.
  } else {
    // Mark the query failed.
    if (reply.status() != CtrlStatus::CTRL_DISPATCHER_DROPPED_QUERY) {
      LOG(WARNING) << "Dispatcher returns failure: "
                   << CtrlStatus_Name(reply.status())
                   << " query_id_list.size(): " << reply.query_id_list_size()
                   << " model_session: " << model_session_id_;
    }
    QueryResultProto result;
    result.set_model_index(model_index_.t);
    result.set_status(reply.status());
    for (auto query_id : reply.query_id_list()) {
      result.set_query_id(query_id);
      HandleBackendReply(result);
    }
  }
}

std::vector<uint32_t> ModelHandler::BackendList() {
  std::vector<uint32_t> ret;
  std::lock_guard<std::mutex> lock(route_mu_);
  for (auto iter : backend_rates_) {
    ret.push_back(iter.first);
  }
  return ret;
}

std::shared_ptr<BackendSession> ModelHandler::GetBackendWeightedRoundRobin() {
  std::uniform_real_distribution<float> dis(0, total_throughput_);
  float select = dis(rand_gen_);
  uint i = 0;
  for (; i < backends_.size(); ++i) {
    uint32_t backend_id = backends_[i];
    float rate = backend_rates_.at(backend_id);
    select -= rate;
    if (select < 0) {
      auto backend_sess = backend_pool_.GetBackend(backend_id);
      if (backend_sess != nullptr) {
        return backend_sess;
      }
      break;
    }
  }
  ++i;
  for (uint j = 0; j < backends_.size(); ++j, ++i) {
    auto backend_sess = backend_pool_.GetBackend(backends_[i]);
    if (backend_sess != nullptr) {
      return backend_sess;
    }
  }
  return nullptr;
}

std::shared_ptr<BackendSession> ModelHandler::GetBackendDeficitRoundRobin() {
  for (size_t i = 0; i < 2 * backends_.size(); ++i) {
    size_t idx = (current_drr_index_ + i) % backends_.size();
    uint32_t backend_id = backends_[idx];
    if (backend_quanta_.at(backend_id) >= 1. - 1e-6) {
      auto backend = backend_pool_.GetBackend(backend_id);
      if (backend != nullptr) {
        backend_quanta_[backend_id] -= 1.;
        return backend;
      } else {
        // The backend has disappeared. Could this be fixed by
        // making `backend_pool_` share the same lock with `backend_`?
        current_drr_index_ = (current_drr_index_ + 1) % backends_.size();
      }
    } else {
      auto rate = backend_rates_[backend_id];
      backend_quanta_[backend_id] += rate * quantum_to_rate_ratio_;
      current_drr_index_ = (current_drr_index_ + 1) % backends_.size();
    }
  }

  return nullptr;
}

}  // namespace app
}  // namespace nexus
