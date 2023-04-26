#include "nexus/dispatcher/rankmt/model_thread.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "nexus/common/functional.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/batch_policy.h"
#include "nexus/dispatcher/query_context.h"
#include "nexus/dispatcher/rankmt/common.h"
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
      batch_policy_(config_.drop, config_.ctrl_latency, config_.data_latency,
                    unprocessed_queries_),
      target_batch_size_(0),
      target_queuing_delay_(0),
      last_exec_at_(TimePoint::min()),
      schedule_credit_(0),
      schedule_credit_reset_countdown_(0),
      drop_timer_(*CHECK_NOTNULL(executor)),
      invalidate_timer_(*CHECK_NOTNULL(executor)) {
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
  MaybeResetCredit();

  executor_.AddPoller(poller_);
}

ModelThread::~ModelThread() {
  LOG_IF(ERROR, !stop_flag_) << "ModelThread::Stop() not called!";
}

void ModelThread::Stop(std::mutex& mutex, size_t& cnt,
                       std::condition_variable& cv) {
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

  auto qctx = std::make_shared<QueryContext>(std::move(request), deadline);

  // Add to pending queries
  rps_meter_.Hit();
  unprocessed_queries_.insert(qctx);

  // Update schedule
  UpdateCandidate(TimePoint::min());

  // Notify the RankThread
  rank_thread_.PostExecutionCandidate(model_index_, candidate_);

  return CtrlStatus::CTRL_OK;
}

void ModelThread::MaybeResetCredit() {
  if (schedule_credit_reset_countdown_ > 0) {
    return;
  }
  schedule_credit_reset_countdown_ =
      config_.credit_reset_period * target_batch_size_;

  auto diff = CalcCreditDiff(1);
  auto new_credit = -diff * config_.credit_reset_value;
  if (VLOG_IS_ON(1) && model_index_ == 0) {
    VLOG(1) << "ResetCredit. model: " << model_session_.model_name()
            << " old_credit: " << schedule_credit_ / 1e6 << "ms"
            << " new_credit: " << new_credit / 1e6 << "ms";
    ;
  }
  schedule_credit_ = new_credit;
}

long ModelThread::CalcCreditDiff(uint32_t bs) const {
  // `l(tbs)/tbs` is the target throughput.
  // `l(tbs)/tbs * b` is the latency of `b` if running at target throughput.
  // `l(tbs)/tbs*b - l(b)` is the saved time.
  auto l_tbs =
      static_cast<long>(profile_.GetForwardLatency(target_batch_size_) * 1e3);
  auto l_bs = static_cast<long>(profile_.GetForwardLatency(bs) * 1e3);
  return bs * l_tbs / target_batch_size_ - l_bs;
}

void ModelThread::UpdateCredit(uint32_t bs) {
  auto diff = CalcCreditDiff(bs);
  schedule_credit_ += diff;
  if (VLOG_IS_ON(1) && model_index_ == 0) {
    VLOG(1) << "UpdateCredit model: " << model_session_.model_name()
            << " tbs: " << target_batch_size_ << " bs: " << bs
            << " diff: " << diff / 1e6
            << "ms new_credit: " << schedule_credit_ / 1e6 << "ms";
  }
}

bool ModelThread::IsBetaLambdaSchedulable(uint32_t bs) const {
  auto rps = rps_meter_.Get();
  if (!rps.has_value()) {
    return false;
  }
  float lambda = rps->avg;

  float x1 = bs != target_batch_size_ ? target_batch_size_ : bs + 1;
  float l1 = profile_.GetForwardLatency(x1) * 1e-6;
  float lb = profile_.GetForwardLatency(bs) * 1e-6;
  float alpha = (l1 - lb) / (x1 - bs);
  float beta = l1 - alpha * x1;

  float beta_lambda = beta * lambda;
  return bs >= beta_lambda;
}

void ModelThread::UpdateTargetBatchSize(const std::optional<AvgStd>& rps) {
  if (config_.drop == DropPolicy::kDropTimeout) {
    target_batch_size_ = 1;
    target_queuing_delay_ = std::chrono::nanoseconds(0);
    return;
  }
  if (rps.has_value()) {
    auto time_budget =
        std::chrono::nanoseconds(model_session_.latency_sla() * 1000000L);
    time_budget -= config_.ctrl_latency;
    time_budget -= config_.resp_latency;
    target_batch_size_ = bse_.Estimate(
        profile_, time_budget, config_.data_latency, rps->avg, rps->std);
    target_queuing_delay_ = std::chrono::nanoseconds(
        static_cast<long>(target_batch_size_ / rps->avg * 1e9));
  } else {
    double time_budget_ms = model_session_.latency_sla() / 2.0;
    target_batch_size_ = profile_.GetMaxBatchWithFullBudget(time_budget_ms);
    target_queuing_delay_ = std::chrono::nanoseconds(
        static_cast<long>(model_session_.latency_sla() / 2.0 * 1e6));
  }
}

void ModelThread::UpdateCandidate(TimePoint gpu_free_at) {
  auto sched_at = Clock::now();
  auto rps = rps_meter_.Get();
  UpdateTargetBatchSize(rps);
  batch_policy_.Update(sched_at, gpu_free_at, target_batch_size_);

  auto c = ExecutionCandidate::Invalid();
  auto bs = batch_policy_.batch_size();
  if (bs > 0) {
    constexpr auto kTimerJitter = std::chrono::microseconds(100);
    c.batch_size = bs;
    auto data_latency = config_.data_latency * bs;
    auto deadline = batch_policy_.deadline();
    auto exec_elapse = EstimateExecElapse(profile_, bs);
    auto frontrun_elapse = EstimateExecElapse(profile_, bs + 1);
    auto latest_exec_at = deadline - kTimerJitter - exec_elapse;
    auto frontrun_exec_at =
        deadline - kTimerJitter - frontrun_elapse - config_.data_latency;
    auto frontrun_scheduable_at =
        frontrun_exec_at - config_.ctrl_latency - data_latency;
    auto earliest_exec_at = sched_at + data_latency + config_.ctrl_latency;
    auto credit_diff = CalcCreditDiff(bs);

    switch (config_.schedulable) {
      case SchedulableCondition::kImmediately:
        c.exec_at = earliest_exec_at;
        c.schedulable_at = sched_at;
        c.priority = deadline - exec_elapse;
        break;
      case SchedulableCondition::kFrontrun:
        c.exec_at = frontrun_exec_at;
        c.schedulable_at = frontrun_scheduable_at;
        c.priority = frontrun_exec_at;
        break;
      case SchedulableCondition::kCredit: {
        bool b = schedule_credit_ + credit_diff >= 0;
        c.exec_at = b ? earliest_exec_at : frontrun_exec_at;
        c.schedulable_at = b ? sched_at : frontrun_scheduable_at;
        c.priority = frontrun_exec_at;
        break;
      }
      case SchedulableCondition::kBetaLambda: {
        bool b = IsBetaLambdaSchedulable(bs);
        c.exec_at = b ? earliest_exec_at : frontrun_exec_at;
        c.schedulable_at = b ? sched_at : frontrun_scheduable_at;
        c.priority = frontrun_exec_at;
        break;
      }
      default:
        LOG(FATAL) << "Unreachable. config_.schedulable="
                   << config_.schedulable;
    }
    c.exec_at = std::max(
        {earliest_exec_at, gpu_free_at, std::min(c.exec_at, latest_exec_at)});
    c.invalid_after = deadline - exec_elapse;

    drop_timer_.SetTimeout(deadline);
    drop_timer_.AsyncWait([this](ario::ErrorCode err) {
      if (err != ario::ErrorCode::kOk) return;
      OnDropTimer();
    });
    invalidate_timer_.SetTimeout(c.invalid_after - data_latency -
                                 config_.ctrl_latency);
    invalidate_timer_.AsyncWait([this](ario::ErrorCode err) {
      if (err != ario::ErrorCode::kOk) return;
      OnDropTimer();
    });
  } else {
    drop_timer_.CancelAll();
    invalidate_timer_.CancelAll();
  }
  candidate_ = c;

  // Send dropped queries
  if (batch_policy_.CountDrops() > 0) {
    SendDroppedQueries(batch_policy_.PopDrops());
  }
}

void ModelThread::OnDropTimer() {
  if (stop_flag_) return;
  if (batch_policy_.batch_size() == 0) {
    return;
  }

  UpdateCandidate(TimePoint::min());
  rank_thread_.PostExecutionCandidate(model_index_, candidate_);
}

void ModelThread::SendDroppedQueries(const SortedQueryList& drops) {
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
  UpdateCandidate(cmd.free_at);
  CHECK(candidate_.exec_at >= cmd.free_at)
      << "diff=" << (cmd.free_at - candidate_.exec_at).count() * 1e-3 << "us";
  auto inputs = batch_policy_.PopInputs();
  auto& delegate = gpus_.at(cmd.gpu_id);

  // Early return when batch_size=0
  if (inputs.empty()) {
    return now;
  }

  // Update credit
  UpdateCredit(inputs.size());
  schedule_credit_reset_countdown_ -= inputs.size();
  MaybeResetCredit();

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
  last_exec_at_ = candidate_.exec_at;
  UpdateCandidate(TimePoint::min());
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
