#include "nexus/dispatcher/rankmt/model_thread.h"

#include <glog/logging.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "nexus/common/functional.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/rankmt/rank_thread.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

ModelThread::ModelThread(
    RankmtConfig config, ario::EpollExecutor* executor,
    ModelSession model_session, ModelIndex model_index, RankThread* rank_thread,
    std::unordered_map<NodeId, std::shared_ptr<FrontendDelegate>> frontends,
    std::unordered_map<NodeId, std::shared_ptr<BackendDelegate>> backends)
    : config_(config),
      executor_(*CHECK_NOTNULL(executor)),
      rank_thread_(*CHECK_NOTNULL(rank_thread)),
      model_session_(std::move(model_session)),
      model_session_id_(ModelSessionToString(model_session_)),
      model_index_(model_index),
      stop_flag_(false),
      poller_(this),
      frontends_(std::move(frontends)),
      backends_(std::move(backends)),
      bse_(1.0, 0.0),
      rps_meter_(config.rpsmeter_window),
      rps_meter_timer_(*CHECK_NOTNULL(executor)),
      batch_policy_(unprocessed_queries_),
      target_batch_size_(0),
      drop_timer_(*CHECK_NOTNULL(executor)) {
  // TODO: GPU performance heterogeneity
  auto profile_id = ModelSessionToProfileID(model_session_);
  for (auto& backend : backends_) {
    for (auto gpu : backend.second->GetGpuDelegates()) {
      CHECK(!gpus_.count(gpu->gpu_id()));
      gpus_[gpu->gpu_id()] = gpu;
      const auto* profile = ModelDatabase::Singleton().GetModelProfile(
          gpu->gpu_device(), gpu->gpu_uuid(), profile_id);
      CHECK(profile != nullptr)
          << "Cannot find profile for " << profile_id << " on device \""
          << gpu->gpu_device() << "\" with uuid \"" << gpu->gpu_uuid() << "\"";
      profile_.MergeProfileBySlowest(*profile);
    }
  }
  profile_.ForceMonotonicity();

  batch_policy_.SetProfile(profile_);
  UpdateTargetBatchSize(std::nullopt);
  OnRpsMeterTimer();

  executor_.AddPoller(poller_);
}

ModelThread::~ModelThread() {
  LOG_IF(ERROR, !stop_flag_) << "ModelThread::Stop() not called!";
}

void ModelThread::Stop(std::mutex& mutex, size_t& cnt,
                       std::condition_variable& cv) {
  // TODO
  executor_.PostBigCallback(
      [this, &mutex, &cnt, &cv](ario::ErrorCode) {
        stop_flag_ = true;
        drop_timer_.CancelAll();
        rps_meter_timer_.CancelAll();
        {
          std::lock_guard lock(mutex);
          cnt += 1;
        }
        cv.notify_all();
      },
      ario::ErrorCode::kOk);
}

void ModelThread::PostAddBackend(NodeId backend_id,
                                 std::shared_ptr<BackendDelegate> delegate) {
  LOG(FATAL) << "Not supported: PostAddBackend";
}

void ModelThread::PostAddFrontend(NodeId frontend_id,
                                  std::shared_ptr<FrontendDelegate> delegate) {
  executor_.PostBigCallback(
      [this, frontend_id, delegate = std::move(delegate)](ario::ErrorCode) {
        frontends_[frontend_id] = delegate;
      },
      ario::ErrorCode::kOk);
}

void ModelThread::PostRemoveBackend(NodeId backend_id) {
  executor_.PostOk([this, backend_id](ario::ErrorCode) {
    CHECK(backends_.count(backend_id));
    auto backend = backends_[backend_id];
    for (auto gpu : backend->GetGpuDelegates()) {
      gpus_.erase(gpu->gpu_id());
    }
    backends_.erase(backend_id);
  });
}

void ModelThread::PostRemoveFrontend(NodeId frontend_id) {
  executor_.PostOk(
      [this, frontend_id](ario::ErrorCode) { frontends_.erase(frontend_id); });
}

void ModelThread::PostGrantedGpu(GrantedGpuMessage cmd) {
  std::lock_guard lock(rank_msg_mutex_);
  CHECK(!rank_msg_.granted_gpu.has_value());
  rank_msg_.granted_gpu = cmd;
}

CtrlStatus ModelThread::EnqueueQuery(DispatchRequest&& request) {
  CHECK_EQ(ario::EpollExecutor::ThisThreadExecutor(), &executor_);
  if (stop_flag_) return CtrlStatus::SERVICE_UNAVAILABLE;

  ModelIndex model_index(request.query_without_input().model_index());
  if (model_index.t != model_index_.t) {
    LOG(ERROR) << "Wrong ModelThread. global_id="
               << request.query_without_input().global_id()
               << ", requested model_index: " << model_index.t
               << ", this model_index: " << model_index_.t << " "
               << model_session_id_;
    return CtrlStatus::MODEL_SESSION_NOT_LOADED;
  }

  // Define deadline
  auto deadline = TimePoint(std::chrono::nanoseconds(
      request.query_without_input().clock().frontend_recv_ns()));
  deadline += std::chrono::milliseconds(model_session_.latency_sla());
  deadline -= config_.resp_latency;  // Backend -> Frontend: results
  constexpr auto kBackendExecutionDelay = std::chrono::microseconds(2000);
  deadline -= kBackendExecutionDelay;  // FIXME: investigate this

  auto qctx = std::make_shared<QueryContext>(std::move(request), deadline);

  // Add to pending queries
  rps_meter_.Hit();
  unprocessed_queries_.insert(qctx);

  // Update schedule
  UpdateCandidate();

  // Notify the RankThread
  rank_thread_.PostExecutionCandidate(model_index_, candidate_);

  return CtrlStatus::CTRL_OK;
}

void ModelThread::UpdateTargetBatchSize(const std::optional<AvgStd>& rps) {
  if (rps.has_value()) {
    double sec = model_session_.latency_sla() * 1e-3;
    std::chrono::duration<double> time_budget(sec);
    time_budget -= config_.ctrl_latency;
    time_budget -= config_.data_latency;
    time_budget -= config_.resp_latency;
    double time_budget_sec = time_budget.count();
    target_batch_size_ =
        bse_.Estimate(profile_, time_budget_sec, rps->avg, rps->std);
  } else {
    double time_budget_ms = model_session_.latency_sla() / 2.0;
    target_batch_size_ = profile_.GetMaxBatchWithFullBudget(time_budget_ms);
  }
}

void ModelThread::UpdateCandidate() {
  auto earliest_exec_at =
      Clock::now() + config_.data_latency + config_.ctrl_latency;
  auto rps = rps_meter_.Get();
  UpdateTargetBatchSize(rps);
  batch_policy_.Update(earliest_exec_at, target_batch_size_);

  TimePoint exec_at;
  const auto& inputs = batch_policy_.inputs();
  if (!inputs.empty()) {
    auto deadline = (*inputs.begin())->deadline;
    auto frontrun_elapse = EstimateExecElapse(profile_, inputs.size() + 1);
    auto frontrun_exec_at = deadline - frontrun_elapse;
    exec_at = std::max(earliest_exec_at, frontrun_exec_at);
    drop_timer_.SetTimeout(deadline);
    drop_timer_.AsyncWait([this](ario::ErrorCode err) {
      if (err != ario::ErrorCode::kOk) return;
      OnDropTimer();
    });
  } else {
    exec_at = TimePoint::max();
    drop_timer_.CancelAll();
  }

  candidate_ = ExecutionCandidate{exec_at};

  // Send dropped queries
  if (!batch_policy_.drops().empty()) {
    SendDroppedQueries(batch_policy_.PopDrops());
  }
}

void ModelThread::OnDropTimer() {
  if (stop_flag_) return;
  if (batch_policy_.inputs().empty()) {
    return;
  }

  UpdateCandidate();
  rank_thread_.PostExecutionCandidate(model_index_, candidate_);
}

void ModelThread::SendDroppedQueries(
    const std::vector<std::shared_ptr<QueryContext>>& drops) {
  std::unordered_map<NodeId, DispatchReply> replies;

  for (auto& qctx : drops) {
    const auto& proto = qctx->request.query_without_input();
    auto frontend_id = NodeId(proto.frontend_id());

    auto res = replies.try_emplace(frontend_id);
    auto& reply = res.first->second;
    if (res.second) {
      reply.set_model_index(model_index_.t);
      reply.set_status(CtrlStatus::CTRL_DISPATCHER_DROPPED_QUERY);
    }
    auto* q = reply.add_query_list();
    q->set_query_id(proto.query_id());
    q->mutable_clock()->CopyFrom(proto.clock());
  }

  for (auto& pair : replies) {
    auto frontend_id = NodeId(pair.first);
    auto iter = frontends_.find(frontend_id);
    if (iter == frontends_.end()) {
      LOG(ERROR) << "Cannot find frontend. frontend_id=" << frontend_id.t
                 << ", model_session=" << model_session_id_;
      continue;
    }
    auto& frontend = iter->second;
    frontend->MarkQueriesDroppedByDispatcher(std::move(pair.second));
  }
}

TimePoint ModelThread::DoGrantedGpuMessage(GrantedGpuMessage& cmd) {
  using namespace std::chrono;
  auto now = Clock::now();
  UpdateCandidate();
  CHECK(candidate_.exec_at >= cmd._debug_free_at)
      << "diff=" << (cmd._debug_free_at - candidate_.exec_at).count() * 1e-3
      << "us";
  auto inputs = batch_policy_.PopInputs();
  auto& delegate = gpus_.at(cmd.gpu_id);

  // Early return when batch_size=0
  if (inputs.empty()) {
    return now;
  }

  // Prepare the batchplan
  BatchPlanProto proto;
  proto.set_plan_id(cmd.plan_id.t);
  proto.set_gpu_idx(delegate->gpu_idx());
  proto.set_model_index(model_index_);
  proto.set_exec_time_ns(candidate_.exec_at.time_since_epoch().count());
  proto.set_deadline_ns((*inputs.begin())->deadline.time_since_epoch().count());
  auto exec_elapse = EstimateExecElapse(profile_, inputs.size());
  auto finish_at = candidate_.exec_at + exec_elapse;
  proto.set_expected_finish_time_ns(finish_at.time_since_epoch().count());
  for (auto& qctx : inputs) {
    auto* query = proto.add_queries();
    auto* query_without_input = query->mutable_query_without_input();
    query_without_input->Swap(qctx->request.mutable_query_without_input());
    query_without_input->set_model_index(model_index_.t);
    query->set_rdma_read_offset(qctx->request.rdma_read_offset());
    query->set_rdma_read_length(qctx->request.rdma_read_length());
    qctx->request.Clear();
  }
  VLOG(1) << "BatchPlan:  " << model_session_.model_name()
          << " id=" << cmd.plan_id.t << " gpu=" << delegate->backend_id() << "#"
          << delegate->gpu_idx() << "(" << cmd.gpu_id << ")"
          << " batch=" << proto.queries_size()
          << " target=" << target_batch_size_
          << " elapse=" << exec_elapse.count() / 1e6 << "ms";
  // Update punch clock
  auto dispatcher_dispatch_ns = Clock::now().time_since_epoch().count();
  for (auto& query : *proto.mutable_queries()) {
    query.mutable_query_without_input()
        ->mutable_clock()
        ->set_dispatcher_dispatch_ns(dispatcher_dispatch_ns);
  }
  // Send to backend
  delegate->EnqueueBatchPlan(std::move(proto));

  // Update candidate
  UpdateCandidate();
  return finish_at;
}

void ModelThread::Poll() {
  if (stop_flag_) return;
  MessagesFromRankThread rank_msg;
  {
    std::lock_guard lock(rank_msg_mutex_);
    rank_msg = std::move(rank_msg_);
    rank_msg_.granted_gpu.reset();
  }
  if (rank_msg.granted_gpu.has_value()) {
    auto& msg = rank_msg.granted_gpu.value();
    auto finish_at = DoGrantedGpuMessage(msg);

    // Update RankThread
    rank_command_queue_.enqueue(UpdateGpuCommand{msg.gpu_id, finish_at});
    rank_thread_.PostResumeCandidateUpdate(model_index_);
    rank_thread_.PostExecutionCandidate(model_index_, candidate_);
  }
}

void ModelThread::OnRpsMeterTimer() {
  auto now = Clock::now();
  rps_meter_.SealCounter(now);

  rps_meter_timer_.SetTimeout(now + config_.rpsmeter_rate);
  rps_meter_timer_.AsyncWait([this](ario::ErrorCode error) {
    if (error != ario::ErrorCode::kOk) return;
    OnRpsMeterTimer();
  });
}

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus
