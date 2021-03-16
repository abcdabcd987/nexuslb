#include "nexus/dispatcher/rank_scheduler.h"

#include <glog/logging.h>

#include <algorithm>
#include <boost/asio/post.hpp>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ario/timer.h"
#include "nexus/common/metric.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/dispatcher.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {
namespace rank {

namespace {

constexpr size_t kRpsMeterHistoryLength = 32;
constexpr uint32_t kCtrlPlaneLatencyUs = 2000;
constexpr uint32_t kDataPlaneLatencyUs = 5000;

}  // namespace

InstanceContext::InstanceContext(ModelSession model_session, NodeId backend_id,
                                 const ModelProfile& profile)
    : model_session(std::move(model_session)),
      backend_id(backend_id),
      profile(profile) {
  max_batch =
      profile.GetMaxBatchWithFullBudget(this->model_session.latency_sla());
}

ModelSessionContext::ModelSessionContext(ModelSession model_session)
    : model_session(std::move(model_session)),
      rps_meter(this->model_session.latency_sla() * 1e-3,
                kRpsMeterHistoryLength, Clock::now()) {
  string_id = ModelSessionToString(this->model_session);
}

std::chrono::nanoseconds ModelSessionContext::EstimateExecElapse(
    uint32_t batch_size) {
  double micros = 0;
  micros += profile->GetPreprocessLatency();
  micros += profile->GetPostprocessLatency();
  micros += profile->GetForwardLatency(batch_size);
  return std::chrono::nanoseconds(static_cast<long>(micros * 1e3));
}

BackendContext::BackendContext(NodeId backend_id,
                               std::shared_ptr<BackendDelegate> delegate)
    : backend_id(backend_id),
      delegate(std::move(delegate)),
      next_available_time(std::chrono::nanoseconds(0)) {}

RankScheduler::Builder::Builder(ario::EpollExecutor& executor)
    : executor_(&executor) {}

std::unique_ptr<Scheduler> RankScheduler::Builder::Build(
    std::unique_ptr<DispatcherAccessor> dispatcher) {
  return std::make_unique<RankScheduler>(std::move(dispatcher), *executor_);
}

RankScheduler::RankScheduler(std::unique_ptr<DispatcherAccessor> dispatcher,
                             ario::EpollExecutor& executor)
    : Scheduler(std::move(dispatcher)), executor_(&executor), bse_(1.0, 0.0) {}

void RankScheduler::RunAsWorker() {
  // TODO
}

void RankScheduler::Stop() {
  // TODO
}

void RankScheduler::AddModelSession(ModelSession model_session) {
  // std::lock_guard<std::mutex> lock(mutex_);

  // Add model session
  auto mctx = std::make_shared<ModelSessionContext>(std::move(model_session));
  if (models_.count(mctx->string_id)) {
    LOG(ERROR) << "Model session already exists. model_session="
               << mctx->string_id;
    return;
  }
  models_[mctx->string_id] = mctx;

  // Add instances
  auto profile_id = ModelSessionToProfileID(mctx->model_session);
  for (auto& pair : backends_) {
    auto& bctx = pair.second;
    const auto* profile = ModelDatabase::Singleton().GetModelProfile(
        bctx->delegate->gpu_device(), bctx->delegate->gpu_uuid(), profile_id);
    if (!profile) {
      continue;
    }
    auto inst = std::make_shared<InstanceContext>(mctx->model_session,
                                                  bctx->backend_id, *profile);
    mctx->instances[bctx->backend_id] = inst;
    bctx->instances[mctx->string_id] = inst;

    // Workaround: use the first backend's profile as model session profile.
    // TODO: GPU performance heterogeneity.
    if (!mctx->profile) {
      mctx->profile = profile;
      if (!mctx->target_batch_size) {
        UpdateTargetBatchSize(mctx.get(), std::nullopt);
      }
    }
  }
}

void RankScheduler::AddBackend(NodeId backend_id) {
  // std::lock_guard<std::mutex> lock(mutex_);

  // Add backend
  auto delegate = dispatcher_->GetBackend(backend_id);
  if (!delegate) {
    LOG(ERROR) << "Cannot find backend delegate. backend_id=" << backend_id.t;
    return;
  }
  auto bctx = std::make_shared<BackendContext>(backend_id, delegate);
  if (backends_.count(bctx->backend_id)) {
    LOG(ERROR) << "Backend already exists. backend_id=" << backend_id.t;
    return;
  }
  backends_[backend_id] = std::move(bctx);

  // Add instances
  for (auto& pair : models_) {
    auto& mctx = pair.second;
    auto profile_id = ModelSessionToProfileID(mctx->model_session);
    const auto* profile = ModelDatabase::Singleton().GetModelProfile(
        bctx->delegate->gpu_device(), bctx->delegate->gpu_uuid(), profile_id);
    if (!profile) {
      continue;
    }
    auto inst = std::make_shared<InstanceContext>(mctx->model_session,
                                                  bctx->backend_id, *profile);
    mctx->instances[bctx->backend_id] = inst;
    bctx->instances[mctx->string_id] = inst;

    // Workaround: use the first backend's profile as model session profile.
    // TODO: GPU performance heterogeneity.
    if (!mctx->profile) {
      mctx->profile = profile;
      if (!mctx->target_batch_size) {
        UpdateTargetBatchSize(mctx.get(), std::nullopt);
      }
    }
  }
}

void RankScheduler::UpdateTargetBatchSize(ModelSessionContext* mctx,
                                          const std::optional<AvgStd>& rps) {
  if (!mctx->profile) {
    LOG(FATAL) << "ModelSession doesn't have a profile. model_session="
               << mctx->string_id;
  }
  if (rps.has_value()) {
    double time_budget_ms = mctx->model_session.latency_sla();
    time_budget_ms -= kCtrlPlaneLatencyUs;
    time_budget_ms -= kDataPlaneLatencyUs;
    mctx->target_batch_size = bse_.Estimate(
        *mctx->profile, time_budget_ms * 1e-3, rps->avg, rps->std);
  } else {
    double time_budget_ms = mctx->model_session.latency_sla() / 2.0;
    mctx->target_batch_size =
        mctx->profile->GetMaxBatchWithFullBudget(time_budget_ms);
  }
}

void RankScheduler::UpdateCandidatePool(TimePoint now,
                                        TimePoint earliest_exec_time,
                                        ModelSessionContext* mctx) {
  auto rps = mctx->rps_meter.Get(now);
  UpdateTargetBatchSize(mctx, rps);
  auto batch =
      batch_policy_.Getbatch(earliest_exec_time, mctx->target_batch_size,
                             mctx->queries, *mctx->profile);

  mctx->queries.clear();
  mctx->queries.insert(batch.inputs.begin(), batch.inputs.end());
  mctx->queries.insert(batch.remains.begin(), batch.remains.end());

  TimePoint latest_exec_time;
  if (!batch.inputs.empty()) {
    auto elapse = mctx->EstimateExecElapse(batch.inputs.size());
    latest_exec_time = batch.inputs[0]->deadline - elapse;
  } else {
    latest_exec_time = TimePoint::max();
  }
  std::shared_ptr<ExecutionCandidate> candidate(new ExecutionCandidate{
      mctx->string_id, latest_exec_time, std::move(batch.inputs),
      std::move(batch.drops), std::move(batch.remains)});
  candidate_pool_.Upsert(mctx->string_id, candidate);
}

void RankScheduler::UpdateActivePlans(TimePoint now,
                                      TimePoint earliest_exec_time,
                                      ModelSessionContext* mctx,
                                      size_t num_idle_backends) {
  size_t rank = candidate_pool_.Rank(mctx->string_id);
  if (rank < num_idle_backends) {
    const auto& candidate = candidate_pool_.GetByKey(mctx->string_id);
    if (!candidate->inputs.empty()) {
      RemoveActivePlan(mctx);
      SetupActivePlan(now, earliest_exec_time, mctx, candidate);
    }
  }
  if (candidate_pool_.Size() > num_idle_backends) {
    auto bottom_pair = candidate_pool_.GetByRank(num_idle_backends);
    auto bottom_mctx = models_.at(bottom_pair.key);
    RemoveActivePlan(bottom_mctx.get());
  }
}

std::shared_ptr<ExecutionCandidate> RankScheduler::PopCandidatePool(
    TimePoint now, TimePoint earliest_exec_time, size_t rank) {
  std::vector<std::shared_ptr<ExecutionCandidate>> candidates;
  for (size_t i = rank; rank < candidate_pool_.Size(); ++i) {
    auto pair = candidate_pool_.GetByRank(i);
    candidates.push_back(pair.value);
  }
  std::shared_ptr<ExecutionCandidate> bottom_candidate;
  for (auto& old_candidate : candidates) {
    const auto& model_session_id = old_candidate->model_session_id;
    auto& mctx = models_.at(model_session_id);
    if (old_candidate->latest_exec_time < earliest_exec_time) {
      UpdateCandidatePool(now, earliest_exec_time, mctx.get());
    }
    const auto& new_candidate = candidate_pool_.GetByKey(model_session_id);
    if (!bottom_candidate ||
        new_candidate->latest_exec_time < bottom_candidate->latest_exec_time) {
      bottom_candidate = new_candidate;
    }
  }
  return bottom_candidate;
}

void RankScheduler::SetupActivePlan(
    TimePoint now, TimePoint earliest_exec_time, ModelSessionContext* mctx,
    std::shared_ptr<ExecutionCandidate> candidate) {
  // Build plan
  auto plan = std::make_shared<ActivePlan>();
  plan->plan_id = NextPlanId();
  plan->deadline = candidate->inputs[0]->deadline;
  uint32_t batch_size = candidate->inputs.size();
  auto frontrun_elapse = mctx->EstimateExecElapse(batch_size + 1);
  auto frontrun_exec_time = plan->deadline - frontrun_elapse;
  auto send_time_calc = frontrun_exec_time -
                        std::chrono::microseconds(kCtrlPlaneLatencyUs) -
                        std::chrono::microseconds(kDataPlaneLatencyUs);
  plan->send_time = std::max(now, send_time_calc);
  auto exec_time_calc = plan->send_time +
                        std::chrono::microseconds(kCtrlPlaneLatencyUs) +
                        std::chrono::microseconds(kDataPlaneLatencyUs);
  plan->exec_time = std::max(earliest_exec_time, exec_time_calc);
  auto exec_elapse = mctx->EstimateExecElapse(batch_size);
  plan->finish_time = plan->exec_time + exec_elapse;
  plan->candidate = candidate;

  // Update bookkeeping
  mctx->active_plan = plan;
  plans_[plan->plan_id] = plan;

  // Setup timer
  plan->send_timer = std::make_unique<ario::Timer>(
      *executor_, plan->send_time,
      [this, plan_id = plan->plan_id] { OnPlanTimer(plan_id); });
}

void RankScheduler::RemoveActivePlan(ModelSessionContext* mctx) {
  auto plan = mctx->active_plan;
  if (plan) {
    plans_.erase(plan->plan_id);
    plan->send_timer->CancelAll();
    mctx->active_plan = nullptr;
  }
}

std::vector<std::shared_ptr<BackendContext>> RankScheduler::GetIdleBackends(
    TimePoint earliest_exec_time) {
  // TODO: Use Splay
  std::vector<std::shared_ptr<BackendContext>> ret;
  for (auto& pair : backends_) {
    if (pair.second->next_available_time <= earliest_exec_time) {
      ret.push_back(pair.second);
    }
  }
  return ret;
}

CtrlStatus RankScheduler::EnqueueQuery(DispatchRequest&& request) {
  std::shared_ptr<QueryContext> qctx;
  {
    const auto& q = request.query_without_input();
    ModelSession model_session;
    ParseModelSession(q.model_session_id(), &model_session);
    auto deadline =
        TimePoint(std::chrono::nanoseconds(q.clock().frontend_recv_ns()) +
                  std::chrono::milliseconds(model_session.latency_sla()));
    qctx = std::make_shared<QueryContext>(std::move(request), deadline);
  }
  const auto& query = qctx->request.query_without_input();
  auto now =
      TimePoint(std::chrono::nanoseconds(query.clock().dispatcher_recv_ns()));

  // Add to pending queries
  if (queries_.count(qctx->global_id)) {
    LOG(ERROR) << "Query already exists. global_id=" << qctx->global_id.t;
    return CtrlStatus::CTRL_GLOBAL_ID_CONFLICT;
  }
  const auto& model_session_id = query.model_session_id();
  auto miter = models_.find(model_session_id);
  if (miter == models_.end()) {
    LOG(ERROR) << "Cannot find model session. global_id=" << qctx->global_id.t
               << ", model_session=" << model_session_id;
    return CtrlStatus::MODEL_SESSION_NOT_LOADED;
  }
  queries_[qctx->global_id] = qctx;
  auto& mctx = miter->second;
  mctx->queries.insert(qctx);
  mctx->rps_meter.Hit(now);

  // Update schedule
  auto earliest_exec_time = now +
                            std::chrono::microseconds(kDataPlaneLatencyUs) +
                            std::chrono::microseconds(kCtrlPlaneLatencyUs);
  auto num_idle_backends = GetIdleBackends(earliest_exec_time).size();
  UpdateCandidatePool(now, earliest_exec_time, mctx.get());
  UpdateActivePlans(now, earliest_exec_time, mctx.get(), num_idle_backends);

  VLOG(1) << "EnqueueQuery success. global_id=" << qctx->global_id.t;
  return CtrlStatus::CTRL_OK;
}

PlanId RankScheduler::NextPlanId() { return PlanId(next_plan_id_.t++); }

void RankScheduler::OnBackendAvailableSoon(NodeId backend_id) {
  TimePoint now = Clock::now();
  auto& bctx = backends_.at(backend_id);
  auto schedule_time = bctx->next_available_time -
                       std::chrono::microseconds(kDataPlaneLatencyUs) -
                       std::chrono::microseconds(kCtrlPlaneLatencyUs);
  auto timer_delay = now - schedule_time;
  if (timer_delay > std::chrono::microseconds(100)) {
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(timer_delay)
                  .count();
    LOG(WARNING) << "OnBackendAvailableSoon: timer_delay: " << us << " us";
  }

  auto earliest_exec_time = bctx->next_available_time;
  auto num_idle_backends = GetIdleBackends(earliest_exec_time).size();
  CHECK_GT(num_idle_backends, 0);
  auto rank = num_idle_backends - 1;
  auto candidate = PopCandidatePool(now, earliest_exec_time, rank);
  if (candidate) {
    auto& mctx = models_.at(candidate->model_session_id);
    CHECK_EQ(mctx->active_plan, nullptr);
    UpdateActivePlans(now, earliest_exec_time, mctx.get(), num_idle_backends);
  }
}

void RankScheduler::OnPlanTimer(PlanId plan_id) {
  using namespace std::chrono;
  VLOG(1) << "OnPlanTimer: start. plan_id=" << plan_id.t;
  TimePoint now = Clock::now();
  std::shared_ptr<ActivePlan> plan;
  {
    auto iter = plans_.find(plan_id);
    if (iter == plans_.end()) {
      // Cancelled plan. Do nothing.
      VLOG(1) << "OnPlanTimer: return early. Plan cancelled. plan_id="
              << plan_id.t;
      return;
    }
    plan = iter->second;
    plans_.erase(iter);
  }
  auto timer_delay = now - plan->send_time;
  if (timer_delay > microseconds(100)) {
    auto us = duration_cast<microseconds>(timer_delay).count();
    LOG(WARNING) << "OnPlanTimer: huge timer delay: " << us << " us";
  }
  auto& mctx = models_.at(plan->candidate->model_session_id);

  // Assign backend
  auto idle_backends = GetIdleBackends(plan->exec_time);
  CHECK_GT(idle_backends.size(), 0);
  auto bctx = idle_backends[0];
  auto backend_id = bctx->backend_id;

  // Setup next schedule when this batch is almost done
  bctx->next_available_time = plan->finish_time;
  bctx->schedule_time = bctx->next_available_time -
                        microseconds(kDataPlaneLatencyUs) -
                        microseconds(kCtrlPlaneLatencyUs);
  bctx->schedule_timer.SetTimeout(bctx->schedule_time);
  bctx->schedule_timer.AsyncWait(
      [this, backend_id] { OnBackendAvailableSoon(backend_id); });

  // Remove pending queries.
  for (auto& qctx : plan->candidate->inputs) {
    mctx->queries.erase(qctx);
    queries_.erase(qctx->global_id);
  }
  for (auto& qctx : plan->candidate->drops) {
    mctx->queries.erase(qctx);
    queries_.erase(qctx->global_id);
  }

  // Update candidate pool
  auto earliest_exec_time = plan->exec_time;
  auto num_idle_backends = GetIdleBackends(earliest_exec_time).size();
  UpdateCandidatePool(now, earliest_exec_time, mctx.get());
  UpdateActivePlans(now, earliest_exec_time, mctx.get(), num_idle_backends);

  // Prepare to send to backend.
  auto delegate = dispatcher_->GetBackend(backend_id);
  // TODO: send without holding lock
  // lock.unlock();
  if (!delegate) {
    LOG(ERROR) << "Cannot find backend delegate. backend_id=" << backend_id.t;
    return;
  }
  BatchPlanProto proto;
  EnqueueQueryCommand query;
  proto.set_plan_id(plan->plan_id.t);
  proto.set_model_session_id(plan->candidate->model_session_id);
  proto.set_exec_time_ns(
      duration_cast<nanoseconds>(plan->exec_time.time_since_epoch()).count());
  proto.set_deadline_ns(
      duration_cast<nanoseconds>(plan->deadline.time_since_epoch()).count());
  proto.set_expected_finish_time_ns(
      duration_cast<nanoseconds>(plan->finish_time.time_since_epoch()).count());
  for (auto& qctx : plan->candidate->inputs) {
    query.mutable_query_without_input()->Swap(
        qctx->request.mutable_query_without_input());
    query.set_rdma_read_offset(qctx->request.rdma_read_offset());
    query.set_rdma_read_length(qctx->request.rdma_read_length());
    qctx->request.Clear();
    *proto.add_queries() = std::move(query);
  }
  // Update punch clock
  auto dispatcher_dispatch_ns =
      duration_cast<nanoseconds>(Clock::now().time_since_epoch()).count();
  for (auto& query : *proto.mutable_queries()) {
    query.mutable_query_without_input()
        ->mutable_clock()
        ->set_dispatcher_dispatch_ns(dispatcher_dispatch_ns);
  }
  // Send to backend
  VLOG(1) << "WorkFinalizePlan: send to backend.";
  delegate->EnqueueBatchPlan(std::move(proto));
  SendDroppedQueries(plan->candidate->drops);
  VLOG(1) << "WorkFinalizePlan: done.";
}

void RankScheduler::SendDroppedQueries(
    const std::vector<std::shared_ptr<QueryContext>>& drops) {
  DispatchReply dispatch_reply;
  for (auto& qctx : drops) {
    auto frontend_id =
        NodeId(qctx->request.query_without_input().frontend_id());
    auto frontend = dispatcher_->GetFrontend(frontend_id);
    const auto& model_session_id =
        qctx->request.query_without_input().model_session_id();
    if (!frontend) {
      LOG(ERROR) << "Cannot find frontend. frontend_id=" << frontend_id.t
                 << ", global_id=" << qctx->global_id.t
                 << ", model_session=" << model_session_id;
      continue;
    }
    dispatch_reply.Clear();
    ParseModelSession(model_session_id, dispatch_reply.mutable_model_session());
    dispatch_reply.set_query_id(qctx->request.query_without_input().query_id());
    dispatch_reply.set_status(CtrlStatus::CTRL_DISPATCHER_DROPPED_QUERY);
    frontend->MarkQueryDroppedByDispatcher(std::move(dispatch_reply));
  }
}

}  // namespace rank
}  // namespace dispatcher
}  // namespace nexus
