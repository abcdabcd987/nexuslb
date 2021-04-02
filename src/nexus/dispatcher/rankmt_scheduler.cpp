#include "nexus/dispatcher/rankmt_scheduler.h"

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

#include "ario/epoll.h"
#include "ario/error.h"
#include "ario/timer.h"
#include "nexus/common/functional.h"
#include "nexus/common/metric.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/dispatcher.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

namespace {

constexpr size_t kRpsMeterHistoryLength = 32;
constexpr uint32_t kCtrlPlaneLatencyUs = 2000;
constexpr uint32_t kDataPlaneLatencyUs = 5000;

std::chrono::nanoseconds EstimateExecElapse(const ModelProfile& profile,
                                            uint32_t batch_size) {
  double micros = 0;
  micros += profile.GetPreprocessLatency();
  micros += profile.GetPostprocessLatency();
  micros += profile.GetForwardLatency(batch_size);
  return std::chrono::nanoseconds(static_cast<long>(micros * 1e3));
}

}  // namespace

ModelSessionContext::ModelSessionContext(ModelSession model_session,
                                         const ModelProfile& profile)
    : model_session(std::move(model_session)),
      batch_policy(queries),
      rps_meter(this->model_session.latency_sla() * 1e-3,
                kRpsMeterHistoryLength, Clock::now()),
      profile(profile) {
  string_id = ModelSessionToString(this->model_session);
  batch_policy.SetProfile(profile);
}

BackendContext::BackendContext(NodeId backend_id,
                               std::shared_ptr<BackendDelegate> delegate)
    : backend_id(backend_id),
      delegate(std::move(delegate)),
      next_available_time(std::chrono::nanoseconds(0)) {}

// ============ ModelThread ============

ModelThread::ModelThread(ario::EpollExecutor* executor,
                         ModelSession model_session,
                         const ModelProfile& profile, RankThread* rank_thread,
                         DispatcherAccessor* dispatcher)
    : executor_(*CHECK_NOTNULL(executor)),
      rank_thread_(*CHECK_NOTNULL(rank_thread)),
      dispatcher_(*CHECK_NOTNULL(dispatcher)),
      bse_(1.0, 0.0),
      mctx_(std::move(model_session), profile) {
  UpdateTargetBatchSize(std::nullopt);
}

ModelThread::~ModelThread() {
  // TODO
}

void ModelThread::Stop() {
  // TODO
  queries_.clear();
}

void ModelThread::PostCommand() {
  executor_.Post([this](ario::ErrorCode) { ExecuteCommand(); },
                 ario::ErrorCode::kOk);
}

CtrlStatus ModelThread::EnqueueQuery(DispatchRequest&& request) {
  CHECK_EQ(ario::EpollExecutor::ThisThreadExecutor(), &executor_);

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
  if (model_session_id != mctx_.string_id) {
    LOG(ERROR) << "Wrong ModelThread. global_id=" << qctx->global_id.t
               << ", requested model_session: " << model_session_id
               << ", ModelThread: " << mctx_.string_id;
    return CtrlStatus::MODEL_SESSION_NOT_LOADED;
  }
  queries_[qctx->global_id] = qctx;
  mctx_.queries.insert(qctx);
  mctx_.rps_meter.Hit(now);

  // Update schedule
  auto earliest_exec_time = now +
                            std::chrono::microseconds(kDataPlaneLatencyUs) +
                            std::chrono::microseconds(kCtrlPlaneLatencyUs);
  UpdateCandidate(earliest_exec_time);

  // Notify the RankThread
  rank_command_queue_.enqueue(
      UpdateCandidateCommand{mctx_.string_id, candidate_});
  rank_thread_.PostCommandFromModelThread(&mctx_.string_id);

  return CtrlStatus::CTRL_OK;
}

void ModelThread::UpdateTargetBatchSize(const std::optional<AvgStd>& rps) {
  if (rps.has_value()) {
    double time_budget_ms = mctx_.model_session.latency_sla();
    time_budget_ms -= kCtrlPlaneLatencyUs;
    time_budget_ms -= kDataPlaneLatencyUs;
    mctx_.target_batch_size =
        bse_.Estimate(mctx_.profile, time_budget_ms * 1e-3, rps->avg, rps->std);
  } else {
    double time_budget_ms = mctx_.model_session.latency_sla() / 2.0;
    mctx_.target_batch_size =
        mctx_.profile.GetMaxBatchWithFullBudget(time_budget_ms);
  }
}

void ModelThread::UpdateCandidate(TimePoint earliest_exec_time) {
  auto rps = mctx_.rps_meter.Get(earliest_exec_time);
  UpdateTargetBatchSize(rps);
  mctx_.batch_policy.Update(earliest_exec_time, mctx_.target_batch_size);

  TimePoint latest_exec_time;
  TimePoint deadline;
  const auto& inputs = mctx_.batch_policy.inputs();
  if (!inputs.empty()) {
    auto elapse = EstimateExecElapse(mctx_.profile, inputs.size());
    latest_exec_time = inputs[0]->deadline - elapse;
    deadline = inputs[0]->deadline;
  } else {
    latest_exec_time = TimePoint::max();
    deadline = TimePoint::max();
  }

  uint32_t batch_size = mctx_.batch_policy.inputs().size();
  candidate_ = ExecutionCandidate{earliest_exec_time, latest_exec_time,
                                  deadline, batch_size};

  // Send dropped queries
  if (!mctx_.batch_policy.drops().empty()) {
    SendDroppedQueries(mctx_.batch_policy.PopDrops());
  }
}

void ModelThread::SendDroppedQueries(
    const std::vector<std::shared_ptr<QueryContext>>& drops) {
  DispatchReply dispatch_reply;
  for (auto& qctx : drops) {
    auto frontend_id =
        NodeId(qctx->request.query_without_input().frontend_id());
    auto frontend = dispatcher_.GetFrontend(frontend_id);
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

    queries_.erase(qctx->global_id);
  }
}

void ModelThread::ExecuteCommand() {
  auto visitor = make_visitor(
      [this](GrantedBackendMessage& cmd) { DoGrantedBackendMessage(cmd); }
      // Force newline for clang-format
  );

  ModelCommand command;
  while (model_command_queue_.try_dequeue(command)) {
    std::visit(visitor, command);
  }
}

void ModelThread::DoGrantedBackendMessage(GrantedBackendMessage& cmd) {
  using namespace std::chrono;
  auto now = Clock::now();
  auto exec_time = now + microseconds(kDataPlaneLatencyUs) +
                   microseconds(kCtrlPlaneLatencyUs);
  UpdateCandidate(exec_time);

  // Remove pending queries.
  auto inputs = mctx_.batch_policy.PopInputs();
  for (auto& qctx : inputs) {
    queries_.erase(qctx->global_id);
  }

  // Prepare to send to backend.
  auto delegate = dispatcher_.GetBackend(cmd.backend_id);
  if (!delegate) {
    LOG(ERROR) << "Cannot find backend delegate. backend_id="
               << cmd.backend_id.t;
    return;
  }
  BatchPlanProto proto;
  EnqueueQueryCommand query;
  proto.set_plan_id(cmd.plan_id.t);
  proto.set_model_session_id(mctx_.string_id);
  proto.set_exec_time_ns(
      duration_cast<nanoseconds>(exec_time.time_since_epoch()).count());
  proto.set_deadline_ns(
      duration_cast<nanoseconds>(candidate_.deadline.time_since_epoch())
          .count());
  auto exec_elapse = EstimateExecElapse(mctx_.profile, candidate_.batch_size);
  auto finish_time = exec_time + exec_elapse;
  proto.set_expected_finish_time_ns(
      duration_cast<nanoseconds>(finish_time.time_since_epoch()).count());
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
  delegate->EnqueueBatchPlan(std::move(proto));

  // Update candidate
  UpdateCandidate(exec_time);

  // Notify the RankThread
  rank_command_queue_.enqueue(
      UpdateCandidateCommand{mctx_.string_id, candidate_});
  rank_thread_.PostCommandFromModelThread(&mctx_.string_id);
}

// ============ RankThread ============

RankThread::ActivePlan::ActivePlan(ario::EpollExecutor& executor)
    : send_timer(executor) {}

RankThread::RankThread(
    ario::EpollExecutor* executor,
    moodycamel::ReaderWriterQueue<RankCommand>* scheduler_command_queue)
    : executor_(*CHECK_NOTNULL(executor)),
      scheduler_command_queue_(*CHECK_NOTNULL(scheduler_command_queue)) {}

RankThread::~RankThread() {
  // TODO
}

void RankThread::Stop() {
  // TODO
  plans_.clear();
  backends_.clear();
}

PlanId RankThread::NextPlanId() { return PlanId(next_plan_id_.t++); }

void RankThread::PostCommandFromScheduler() {
  executor_.Post(
      [this](ario::ErrorCode) { ExecuteCommand(scheduler_command_queue_); },
      ario::ErrorCode::kOk);
}

void RankThread::PostCommandFromModelThread(
    const std::string* ptr_model_session_id) {
  executor_.Post(
      [this, ptr_model_session_id](ario::ErrorCode) {
        auto& mdata = model_threads_.at(*ptr_model_session_id);
        ExecuteCommand(mdata->rank_command_queue);
      },
      ario::ErrorCode::kOk);
}

void RankThread::ExecuteCommand(
    moodycamel::ReaderWriterQueue<RankCommand>& queue) {
  auto visitor = make_visitor(
      [this](AddModelThreadCommand& cmd) { DoAddModelThreadCommand(cmd); },
      [this](AddBackendCommand& cmd) { DoAddBackendCommand(cmd); },
      [this](UpdateCandidateCommand& cmd) { DoUpdateCandidateCommand(cmd); },
      [this](UpdateBackendCommand& cmd) { DoUpdateBackendCommand(cmd); }
      // Force newline for clang-format
  );

  RankCommand command;
  while (queue.try_dequeue(command)) {
    std::visit(visitor, command);
  }
}

void RankThread::DoAddModelThreadCommand(AddModelThreadCommand& cmd) {
  auto model_session_id = ModelSessionToString(cmd.model_session);
  if (model_threads_.count(model_session_id)) {
    LOG(ERROR) << "ModelThread already exists. model_sesion_id="
               << model_session_id;
    return;
  }
  auto& m = *CHECK_NOTNULL(cmd.model_thread);
  model_threads_[model_session_id] = std::unique_ptr<PerModelThreadData>(
      new PerModelThreadData{m, model_session_id, m.profile(),
                             *CHECK_NOTNULL(m.model_command_queue()),
                             *CHECK_NOTNULL(m.rank_command_queue()), nullptr});

  auto& mdata = *model_threads_[model_session_id];
  candidate_pool_.Upsert(model_session_id,
                         std::shared_ptr<CandidateInfo>(new CandidateInfo{
                             mdata, ExecutionCandidate::Invalid()}));
}

void RankThread::DoAddBackendCommand(AddBackendCommand& cmd) {
  auto bctx =
      std::make_shared<BackendContext>(cmd.backend_id, cmd.backend_delegate);
  if (backends_.count(bctx->backend_id)) {
    LOG(ERROR) << "Backend already exists. backend_id=" << cmd.backend_id;
    return;
  }
  backend_availability_pool_.Upsert(cmd.backend_id, bctx->next_available_time);
  backends_[cmd.backend_id] = std::move(bctx);
}

void RankThread::DoUpdateCandidateCommand(UpdateCandidateCommand& cmd) {
  auto& mdata = *model_threads_.at(cmd.model_session_id);

  auto cinfo =
      std::shared_ptr<CandidateInfo>(new CandidateInfo{mdata, cmd.candidate});
  candidate_pool_.Upsert(cmd.model_session_id, cinfo);

  UpdateActivePlans(cinfo->candidate.earliest_exec_time, mdata);
}

void RankThread::DoUpdateBackendCommand(UpdateBackendCommand& cmd) {
  auto& bctx = backends_.at(cmd.backend_id);
  UpdateBackend(bctx.get(), cmd.next_available_time);
}

void RankThread::UpdateActivePlans(TimePoint earliest_exec_time,
                                   PerModelThreadData& mdata) {
  auto num_idle_backends =
      backend_availability_pool_.CountLessEqual(earliest_exec_time);
  size_t rank = candidate_pool_.Rank(mdata.model_session_id);
  if (rank < num_idle_backends) {
    const auto& cinfo = candidate_pool_.GetByKey(mdata.model_session_id);
    if (cinfo->candidate.batch_size) {
      RemoveActivePlan(mdata);
      SetupActivePlan(earliest_exec_time, mdata, cinfo);
    }
  }
  if (candidate_pool_.Size() > num_idle_backends) {
    auto bottom_pair = candidate_pool_.GetByRank(num_idle_backends);
    auto& bottom_mdata = *model_threads_.at(bottom_pair.key);
    RemoveActivePlan(bottom_mdata);
  }
}

void RankThread::SetupActivePlan(TimePoint earliest_exec_time,
                                 PerModelThreadData& mdata,
                                 std::shared_ptr<CandidateInfo> cinfo) {
  // Build plan
  auto plan = std::make_shared<ActivePlan>(executor_);
  plan->plan_id = NextPlanId();
  plan->deadline = cinfo->candidate.deadline;
  uint32_t batch_size = cinfo->candidate.batch_size;
  auto frontrun_elapse = EstimateExecElapse(mdata.profile, batch_size + 1);
  auto frontrun_exec_time = plan->deadline - frontrun_elapse;
  plan->exec_time = std::max(earliest_exec_time, frontrun_exec_time);
  plan->send_time = plan->exec_time -
                    std::chrono::microseconds(kCtrlPlaneLatencyUs) -
                    std::chrono::microseconds(kDataPlaneLatencyUs);
  CHECK_LE(earliest_exec_time.time_since_epoch().count(),
           plan->exec_time.time_since_epoch().count());
  auto exec_elapse = EstimateExecElapse(mdata.profile, batch_size);
  plan->finish_time = plan->exec_time + exec_elapse;
  plan->cinfo = cinfo;
  CHECK(plan->finish_time <= plan->deadline)
      << "diff = " << (plan->finish_time - plan->deadline).count() / 1e3
      << "us";

  // Update bookkeeping
  mdata.active_plan = plan;
  plans_[plan->plan_id] = plan;

  // Setup timer
  plan->send_timer.SetTimeout(plan->send_time);
  plan->send_timer.AsyncWait(
      [this, plan_id = plan->plan_id](ario::ErrorCode error) {
        if (error == ario::ErrorCode::kCancelled) return;
        OnPlanTimer(plan_id);
      });
}

void RankThread::RemoveActivePlan(PerModelThreadData& mdata) {
  auto plan = mdata.active_plan;
  if (plan) {
    plans_.erase(plan->plan_id);
    plan->send_timer.CancelAll();
    mdata.active_plan = nullptr;
  }
}

void RankThread::OnBackendAvailableSoon(NodeId backend_id) {
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
  auto kv = candidate_pool_.GetByRank(num_idle_backends - 1);
  auto& cinfo = kv.value.get();
  if (cinfo->candidate.batch_size > 0) {
    auto& mdata = cinfo->mdata;
    CHECK_EQ(mdata.active_plan, nullptr);
    UpdateActivePlans(earliest_exec_time, mdata);
  }
}

void RankThread::OnPlanTimer(PlanId plan_id) {
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
  auto& mdata = plan->cinfo->mdata;

  // Assign backend
  auto backend_id = backend_availability_pool_.GetByRank(0).key.get();
  auto& bctx = backends_.at(backend_id);
  CHECK_LT(bctx->next_available_time.time_since_epoch().count(),
           plan->exec_time.time_since_epoch().count());

  // Setup next schedule when this batch is almost done
  UpdateBackend(bctx.get(), plan->finish_time);

  // Update candidate pool
  candidate_pool_.Upsert(mdata.model_session_id,
                         std::shared_ptr<CandidateInfo>(new CandidateInfo{
                             mdata, ExecutionCandidate::Invalid()}));
  UpdateActivePlans(plan->exec_time, mdata);

  // Let ModelThread send out the plan
  GrantedBackendMessage msg;
  msg.backend_id = backend_id;
  msg.plan_id = plan->plan_id;
  mdata.model_command_queue.enqueue(std::move(msg));
  mdata.model_thread.PostCommand();
}

void RankThread::UpdateBackend(BackendContext* bctx,
                               TimePoint next_available_time) {
  bctx->next_available_time = next_available_time;
  bctx->schedule_time = next_available_time -
                        std::chrono::microseconds(kDataPlaneLatencyUs) -
                        std::chrono::microseconds(kCtrlPlaneLatencyUs);
  bctx->schedule_timer.SetTimeout(bctx->schedule_time);
  bctx->schedule_timer.AsyncWait(
      [this, backend_id = bctx->backend_id](ario::ErrorCode error) {
        if (error == ario::ErrorCode::kCancelled) return;
        OnBackendAvailableSoon(backend_id);
      });

  backend_availability_pool_.Upsert(bctx->backend_id, next_available_time);
}

// ============ MultiThreadRankScheduler ============

MultiThreadRankScheduler::Builder::Builder(
    ario::EpollExecutor* scheduler_executor,
    ario::EpollExecutor* rank_thread_executor)
    : scheduler_executor_(CHECK_NOTNULL(scheduler_executor)),
      rank_thread_executor_(CHECK_NOTNULL(rank_thread_executor)) {}

std::unique_ptr<MultiThreadRankScheduler>
MultiThreadRankScheduler::Builder::Build(
    std::unique_ptr<DispatcherAccessor> dispatcher) {
  return std::make_unique<MultiThreadRankScheduler>(
      std::move(dispatcher), scheduler_executor_, rank_thread_executor_);
}

MultiThreadRankScheduler::MultiThreadRankScheduler(
    std::unique_ptr<DispatcherAccessor> dispatcher,
    ario::EpollExecutor* scheduler_executor,
    ario::EpollExecutor* rank_thread_executor)
    : dispatcher_(std::move(dispatcher)),
      executor_(*CHECK_NOTNULL(scheduler_executor)),
      rank_thread_(rank_thread_executor, &rank_thread_command_queue_) {}

MultiThreadRankScheduler::RequestEntrance::RequestEntrance(
    ModelThread* model_thread)
    : model_thread_(model_thread) {}

CtrlStatus MultiThreadRankScheduler::RequestEntrance::EnqueueQuery(
    DispatchRequest&& request) {
  return model_thread_->EnqueueQuery(std::move(request));
}

MultiThreadRankScheduler::~MultiThreadRankScheduler() {
  // TODO
}

void MultiThreadRankScheduler::Stop() {
  // TODO
  LOG(INFO) << "MultiThreadRankScheduler::Stop";
  for (auto& pair : model_threads_) {
    pair.second->Stop();
  }
  rank_thread_.Stop();
}

MultiThreadRankScheduler::RequestEntrance
MultiThreadRankScheduler::AddModelSession(
    ario::EpollExecutor* model_thread_executor, ModelSession model_session) {
  CHECK_NE(model_thread_executor, nullptr);
  std::lock_guard<std::mutex> lock(mutex_);

  if (!gpu_info_for_profile_.has_value()) {
    LOG(FATAL) << "Add backend before adding model sessions.";
  }

  auto model_session_id = ModelSessionToString(model_session);
  if (model_threads_.count(model_session_id)) {
    LOG(FATAL) << "Model session already exists. model_session="
               << model_session_id;
  }

  auto profile_id = ModelSessionToProfileID(model_session);
  const auto* profile = ModelDatabase::Singleton().GetModelProfile(
      gpu_info_for_profile_->gpu_device, gpu_info_for_profile_->gpu_uuid,
      profile_id);
  CHECK_NE(profile, nullptr)
      << "Cannot find profile for " << profile_id << " on device \""
      << gpu_info_for_profile_->gpu_device << "\" with uuid \""
      << gpu_info_for_profile_->gpu_uuid << "\"";

  model_threads_[model_session_id] =
      std::make_unique<ModelThread>(model_thread_executor, model_session,
                                    *profile, &rank_thread_, dispatcher_.get());
  auto* model_thread = model_threads_[model_session_id].get();
  rank_thread_command_queue_.enqueue(
      AddModelThreadCommand{model_session, model_thread});
  rank_thread_.PostCommandFromScheduler();
  return RequestEntrance(model_thread);
}

void MultiThreadRankScheduler::AddBackend(NodeId backend_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Add backend
  auto delegate = dispatcher_->GetBackend(backend_id);
  if (!delegate) {
    LOG(ERROR) << "Cannot find backend delegate. backend_id=" << backend_id.t;
    return;
  }

  // Workaround: use the first backend's profile as model session profile.
  if (!gpu_info_for_profile_.has_value()) {
    gpu_info_for_profile_ = {delegate->gpu_device(), delegate->gpu_uuid()};
  }

  // Ask RankThread to add the backend
  rank_thread_command_queue_.enqueue(AddBackendCommand{backend_id, delegate});
  rank_thread_.PostCommandFromScheduler();
}

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus
