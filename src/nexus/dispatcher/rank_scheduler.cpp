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

#include "ario/error.h"
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

ActivePlan::ActivePlan(ario::EpollExecutor& executor) : send_timer(executor) {}

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
      batch_policy(queries),
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
  plans_.clear();
  queries_.clear();
  models_.clear();
  backends_.clear();
  LOG(INFO) << "RankScheduler::Stop";
}

void RankScheduler::AddModelSession(ModelSession model_session) {
  std::lock_guard<std::mutex> lock(mutex_);

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
      mctx->batch_policy.SetProfile(*profile);
      if (!mctx->target_batch_size) {
        UpdateTargetBatchSize(mctx.get(), std::nullopt);
      }
    }
  }
}

void RankScheduler::AddBackend(NodeId backend_id) {
  std::lock_guard<std::mutex> lock(mutex_);

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
  backend_availability_pool_.Upsert(backend_id, bctx->next_available_time);
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
      mctx->batch_policy.SetProfile(*profile);
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
  mctx->batch_policy.Update(earliest_exec_time, mctx->target_batch_size);

  TimePoint latest_exec_time;
  const auto& inputs = mctx->batch_policy.inputs();
  if (!inputs.empty()) {
    auto elapse = mctx->EstimateExecElapse(inputs.size());
    latest_exec_time = (*inputs.begin())->deadline - elapse;
  } else {
    latest_exec_time = TimePoint::max();
  }
  std::shared_ptr<ExecutionCandidate> candidate(
      new ExecutionCandidate{latest_exec_time, mctx});
  candidate_pool_.Upsert(mctx->string_id, candidate);
}

void RankScheduler::UpdateActivePlans(TimePoint now,
                                      TimePoint earliest_exec_time,
                                      ModelSessionContext* mctx,
                                      size_t num_idle_backends) {
  size_t rank = candidate_pool_.Rank(mctx->string_id);
  if (rank < num_idle_backends) {
    const auto& candidate = candidate_pool_.GetByKey(mctx->string_id);
    const auto& inputs = candidate->mctx->batch_policy.inputs();
    if (!inputs.empty()) {
      RemoveActivePlan(mctx);
      SetupActivePlan(now, earliest_exec_time, candidate);
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
    auto* mctx = old_candidate->mctx;
    if (old_candidate->latest_exec_time < earliest_exec_time) {
      UpdateCandidatePool(now, earliest_exec_time, mctx);
    }
    const auto& new_candidate = candidate_pool_.GetByKey(mctx->string_id);
    if (!bottom_candidate ||
        new_candidate->latest_exec_time < bottom_candidate->latest_exec_time) {
      bottom_candidate = new_candidate;
    }
  }
  return bottom_candidate;
}

void RankScheduler::SetupActivePlan(
    TimePoint now, TimePoint earliest_exec_time,
    std::shared_ptr<ExecutionCandidate> candidate) {
  auto* mctx = candidate->mctx;
  const auto& inputs = mctx->batch_policy.inputs();

  // Build plan
  auto plan = std::make_shared<ActivePlan>(*executor_);
  plan->plan_id = NextPlanId();
  plan->deadline = (*inputs.begin())->deadline;
  uint32_t batch_size = inputs.size();
  auto frontrun_elapse = mctx->EstimateExecElapse(batch_size + 1);
  auto frontrun_exec_time = plan->deadline - frontrun_elapse;
  auto send_time_calc = frontrun_exec_time -
                        std::chrono::microseconds(kCtrlPlaneLatencyUs) -
                        std::chrono::microseconds(kDataPlaneLatencyUs);
  plan->send_time = std::max(now, send_time_calc);
  plan->exec_time = plan->send_time +
                    std::chrono::microseconds(kCtrlPlaneLatencyUs) +
                    std::chrono::microseconds(kDataPlaneLatencyUs);
  CHECK_LE(earliest_exec_time.time_since_epoch().count(),
           plan->exec_time.time_since_epoch().count());
  auto exec_elapse = mctx->EstimateExecElapse(batch_size);
  plan->finish_time = plan->exec_time + exec_elapse;
  plan->candidate = candidate;
  CHECK(plan->finish_time <= plan->deadline)
      << "diff = " << (plan->finish_time - plan->deadline).count() / 1e3
      << "us";

  // Update bookkeeping
  mctx->active_plan = plan;
  plans_[plan->plan_id] = plan;

  // Setup timer
  plan->send_timer.SetTimeout(plan->send_time);
  plan->send_timer.AsyncWait(
      [this, plan_id = plan->plan_id](ario::ErrorCode error) {
        if (error == ario::ErrorCode::kCancelled) return;
        OnPlanTimer(plan_id);
      });
}

void RankScheduler::RemoveActivePlan(ModelSessionContext* mctx) {
  auto plan = mctx->active_plan;
  if (plan) {
    plans_.erase(plan->plan_id);
    plan->send_timer.CancelAll();
    mctx->active_plan = nullptr;
  }
}

CtrlStatus RankScheduler::EnqueueQuery(DispatchRequest&& request) {
  std::lock_guard<std::mutex> lock(mutex_);

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
  auto num_idle_backends =
      backend_availability_pool_.CountLessEqual(earliest_exec_time);
  UpdateCandidatePool(now, earliest_exec_time, mctx.get());
  UpdateActivePlans(now, earliest_exec_time, mctx.get(), num_idle_backends);
  if (!mctx->batch_policy.drops().empty()) {
    SendDroppedQueries(mctx->batch_policy.PopDrops());
  }

  VLOG(1) << "EnqueueQuery success. global_id=" << qctx->global_id.t;
  return CtrlStatus::CTRL_OK;
}

PlanId RankScheduler::NextPlanId() { return PlanId(next_plan_id_.t++); }

void RankScheduler::OnBackendAvailableSoon(NodeId backend_id) {
  std::lock_guard<std::mutex> lock(mutex_);
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
  auto num_idle_backends =
      backend_availability_pool_.CountLessEqual(earliest_exec_time);
  CHECK_GT(num_idle_backends, 0);
  auto rank = num_idle_backends - 1;
  auto candidate = PopCandidatePool(schedule_time, earliest_exec_time, rank);
  if (candidate) {
    auto* mctx = candidate->mctx;
    CHECK_EQ(mctx->active_plan, nullptr);
    UpdateActivePlans(schedule_time, earliest_exec_time, mctx,
                      num_idle_backends);
  }
}

void RankScheduler::OnPlanTimer(PlanId plan_id) {
  using namespace std::chrono;
  std::lock_guard<std::mutex> lock(mutex_);
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
  auto* mctx = plan->candidate->mctx;

  // Assign backend
  auto backend_id = backend_availability_pool_.GetByRank(0).key.get();
  auto& bctx = backends_.at(backend_id);
  CHECK_LT(bctx->next_available_time.time_since_epoch().count(),
           plan->exec_time.time_since_epoch().count());

  // Setup next schedule when this batch is almost done
  bctx->next_available_time = plan->finish_time;
  backend_availability_pool_.Upsert(backend_id, bctx->next_available_time);
  bctx->schedule_time = bctx->next_available_time -
                        microseconds(kDataPlaneLatencyUs) -
                        microseconds(kCtrlPlaneLatencyUs);
  bctx->schedule_timer.SetTimeout(bctx->schedule_time);
  bctx->schedule_timer.AsyncWait([this, backend_id](ario::ErrorCode error) {
    if (error == ario::ErrorCode::kCancelled) return;
    OnBackendAvailableSoon(backend_id);
  });

  // Remove pending queries.
  auto inputs = mctx->batch_policy.PopInputs();
  auto drops = mctx->batch_policy.PopDrops();
  for (auto& qctx : inputs) {
    queries_.erase(qctx->global_id);
  }
  for (auto& qctx : drops) {
    queries_.erase(qctx->global_id);
  }

  // Update candidate pool
  auto earliest_exec_time = plan->exec_time;
  auto num_idle_backends =
      backend_availability_pool_.CountLessEqual(earliest_exec_time);
  UpdateCandidatePool(plan->send_time, earliest_exec_time, mctx);
  UpdateActivePlans(plan->send_time, earliest_exec_time, mctx,
                    num_idle_backends);

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
  proto.set_model_session_id(mctx->string_id);
  proto.set_exec_time_ns(
      duration_cast<nanoseconds>(plan->exec_time.time_since_epoch()).count());
  proto.set_deadline_ns(
      duration_cast<nanoseconds>(plan->deadline.time_since_epoch()).count());
  proto.set_expected_finish_time_ns(
      duration_cast<nanoseconds>(plan->finish_time.time_since_epoch()).count());
  for (auto& qctx : inputs) {
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
  VLOG(1) << "OnPlanTimer: send to backend.";
  delegate->EnqueueBatchPlan(std::move(proto));
  SendDroppedQueries(drops);
  VLOG(1) << "OnPlanTimer: done.";
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
