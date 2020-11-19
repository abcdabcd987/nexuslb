#include "nexus/app/model_handler.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <limits>
#include <typeinfo>

#include "nexus/app/request_context.h"
#include "nexus/common/model_def.h"

DEFINE_int32(count_interval, 1, "Interval to count number of requests in sec");
DEFINE_int32(load_balance, 3,
             "Load balance policy (1: random, 2: choice of 2, "
             "3: deficit round robin, 4: centralized dispatcher)");

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

ModelHandler::ModelHandler(const std::string& model_session_id,
                           BackendPool& pool, LoadBalancePolicy lb_policy,
                           DispatcherRpcClient* dispatcher_rpc_client)
    : model_session_id_(model_session_id),
      backend_pool_(pool),
      lb_policy_(lb_policy),
      total_throughput_(0.),
      rand_gen_(rd_()),
      dispatcher_rpc_client_(dispatcher_rpc_client) {
  ParseModelSession(model_session_id, &model_session_);
  counter_ =
      MetricRegistry::Singleton().CreateIntervalCounter(FLAGS_count_interval);
  LOG(INFO) << model_session_id_ << " load balance policy: " << lb_policy_;
  if (lb_policy_ == LB_DeficitRR) {
    running_ = true;
  }
}

ModelHandler::~ModelHandler() {
  MetricRegistry::Singleton().RemoveMetric(counter_);
  running_ = false;
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
  query.set_model_session_id(model_session_id_);
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
  if (lb_policy_ == LB_Dispatcher) {
    query_without_input.CopyFrom(query);
  }
  query.mutable_input()->CopyFrom(input);
  ctx->SetBackendQueryProto(std::move(query));

  // Send query to backend if not using dispatcher.
  // Otherwise ask the dispatcher first.
  if (lb_policy_ != LB_Dispatcher) {
    auto backend = GetBackend();
    SendBackendQuery(ctx, qid, backend);
  } else {
    dispatcher_rpc_client_->AsyncQuery(model_session_, qid,
                                       std::move(query_without_input), this);
  }

  auto reply = std::make_shared<QueryResult>(qid);
  return reply;
}

void ModelHandler::SendBackendQuery(std::shared_ptr<RequestContext> ctx,
                                    uint64_t qid,
                                    std::shared_ptr<BackendSession> backend) {
  if (backend == nullptr) {
    ctx->HandleError(SERVICE_UNAVAILABLE, "Service unavailable");
    return;
  }

  ctx->RecordQuerySend(qid);
  const auto& query = ctx->backend_query_proto();
  auto msg = std::make_shared<Message>(kBackendRequest, query.ByteSizeLong());
  msg->EncodeBody(query);

  // Send to backend
  backend->Write(std::move(msg));
}

void ModelHandler::HandleBackendReply(const QueryResultProto& result) {
  std::lock_guard<std::mutex> lock(query_ctx_mu_);
  auto qid = QueryId(result.query_id());
  auto iter = query_ctx_.find(qid);
  if (iter == query_ctx_.end()) {
    // FIXME why this happens? lower from FATAL to ERROR temporarily
    LOG(ERROR) << model_session_id_ << " cannot find query context for query "
               << qid.t;
    return;
  }
  auto ctx = iter->second;
  ctx->HandleQueryResult(result);
  query_ctx_.erase(qid);
}

void ModelHandler::HandleDispatcherReply(const DispatchReply& reply) {
  auto qid = QueryId(reply.query_id());
  std::shared_ptr<RequestContext> ctx;
  {
    std::lock_guard<std::mutex> lock(query_ctx_mu_);
    auto iter = query_ctx_.find(qid);
    if (iter == query_ctx_.end()) {
      // Ignore dispatcher RPC replies that take too long time to return.
      return;
    }
    ctx = iter->second;
  }

  if (!ctx->MarkBackendQuerySent()) {
    // Avoid send to backend twice.
    return;
  }

  std::shared_ptr<BackendSession> backend;
  if (reply.status() == CtrlStatus::CTRL_OK) {
    auto backend_id = reply.backend().node_id();
    backend = backend_pool_.GetBackend(backend_id);
    if (!backend) {
      LOG(WARNING) << "Cannot find the backend returned by the dispatcher."
                   << " query_id: " << reply.query_id()
                   << " backend: " << reply.backend().ShortDebugString()
                   << " Fallback to deficit round robin.";
    }
  } else if (reply.status() == CtrlStatus::TIMEOUT) {
    const int n = 1000;
    LOG_EVERY_N(WARNING, n)
        << "LOG_EVERY " << n
        << "] Dispatcher timeout. query_id: " << reply.query_id()
        << " Fallback to deficit round robin.";
  } else {
    LOG(WARNING) << "Dispatcher returns failure: "
                 << CtrlStatus_Name(reply.status())
                 << " query_id: " << reply.query_id()
                 << " Fallback to deficit round robin.";
  }
  if (!backend) {
    std::lock_guard<std::mutex> lock(route_mu_);
    backend = GetBackendDeficitRoundRobin();
  }

  SendBackendQuery(ctx, qid, backend);
}

bool ModelHandler::FetchImage(QueryId query_id, ValueProto* output) {
  auto iter = query_ctx_.find(query_id);
  if (iter == query_ctx_.end()) {
    return false;
  }
  *output = iter->second->backend_query_proto().input();
  return true;
}

void ModelHandler::UpdateRoute(const ModelRouteProto& route) {
  std::lock_guard<std::mutex> lock(route_mu_);
  backends_.clear();
  backend_rates_.clear();
  total_throughput_ = 0.;

  double min_rate = std::numeric_limits<double>::max();
  for (auto itr : route.backend_rate()) {
    min_rate = std::min(min_rate, itr.throughput());
  }
  quantum_to_rate_ratio_ = 1. / min_rate;

  for (auto itr : route.backend_rate()) {
    uint32_t backend_id = itr.info().node_id();
    backends_.push_back(backend_id);
    const auto rate = itr.throughput();
    backend_rates_.emplace(backend_id, rate);
    total_throughput_ += rate;
    LOG(INFO) << "- backend " << backend_id << ": " << rate;
    if (backend_quanta_.count(backend_id) == 0) {
      backend_quanta_.emplace(backend_id, rate * quantum_to_rate_ratio_);
    }
  }
  LOG(INFO) << "Total throughput: " << total_throughput_;
  std::sort(backends_.begin(), backends_.end());
  current_drr_index_ %= backends_.size();
  for (auto iter = backend_quanta_.begin(); iter != backend_quanta_.end();) {
    if (backend_rates_.count(iter->first) == 0) {
      iter = backend_quanta_.erase(iter);
    } else {
      ++iter;
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

std::shared_ptr<BackendSession> ModelHandler::GetBackend() {
  std::lock_guard<std::mutex> lock(route_mu_);
  switch (lb_policy_) {
    case LB_WeightedRR: {
      return GetBackendWeightedRoundRobin();
    }
    case LB_DeficitRR: {
      auto backend = GetBackendDeficitRoundRobin();
      if (backend != nullptr) {
        return backend;
      }
      return GetBackendWeightedRoundRobin();
    }
    case LB_Query: {
      auto candidate1 = GetBackendWeightedRoundRobin();
      if (candidate1 == nullptr) {
        return nullptr;
      }
      auto candidate2 = GetBackendWeightedRoundRobin();
      if (candidate1 == candidate2) {
        return candidate1;
      }
      if (candidate1->GetUtilization() <= candidate2->GetUtilization()) {
        return candidate1;
      }
      return candidate2;
    }
    case LB_Dispatcher: {
      LOG(FATAL) << "Unreachable.";
    }
    default:
      return nullptr;
  }
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
