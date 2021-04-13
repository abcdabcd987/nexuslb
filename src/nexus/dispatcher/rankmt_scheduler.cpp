#include "nexus/dispatcher/rankmt_scheduler.h"

#include <glog/logging.h>

#include <algorithm>
#include <boost/asio/post.hpp>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <condition_variable>
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
#include "nexus/dispatcher/batch_policy.h"
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

// ============ ModelThread ============

ModelThread::ModelThread(ario::EpollExecutor* executor,
                         ModelSession model_session,
                         const ModelProfile& profile, RankThread* rank_thread,
                         DispatcherAccessor* dispatcher)
    : executor_(*CHECK_NOTNULL(executor)),
      rank_thread_(*CHECK_NOTNULL(rank_thread)),
      dispatcher_(*CHECK_NOTNULL(dispatcher)),
      model_session_(std::move(model_session)),
      model_session_id_(ModelSessionToString(model_session_)),
      profile_(profile),
      stop_flag_(false),
      bse_(1.0, 0.0),
      rps_meter_(model_session_.latency_sla() * 1e-3, kRpsMeterHistoryLength,
                 Clock::now()),
      batch_policy_(unprocessed_queries_),
      target_batch_size_(0),
      drop_timer_(*CHECK_NOTNULL(executor)) {
  batch_policy_.SetProfile(profile_);
  UpdateTargetBatchSize(std::nullopt);
}

ModelThread::~ModelThread() {
  LOG_IF(ERROR, !stop_flag_) << "ModelThread::Stop() not called!";
}

void ModelThread::Stop(std::mutex& mutex, size_t& cnt,
                       std::condition_variable& cv) {
  // TODO
  executor_.Post(
      [this, &mutex, &cnt, &cv](ario::ErrorCode) {
        stop_flag_ = true;
        drop_timer_.CancelAll();
        {
          std::lock_guard lock(mutex);
          cnt += 1;
        }
        cv.notify_all();
      },
      ario::ErrorCode::kOk);
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
  const auto& model_session_id = query.model_session_id();
  if (model_session_id != model_session_id_) {
    LOG(ERROR) << "Wrong ModelThread. global_id=" << qctx->global_id.t
               << ", requested model_session: " << model_session_id
               << ", ModelThread: " << model_session_id_;
    return CtrlStatus::MODEL_SESSION_NOT_LOADED;
  }
  rps_meter_.Hit(now);
  unprocessed_queries_.insert(qctx);

  // Update schedule
  auto earliest_exec_time = now +
                            std::chrono::microseconds(kDataPlaneLatencyUs) +
                            std::chrono::microseconds(kCtrlPlaneLatencyUs);
  UpdateCandidate(earliest_exec_time);

  // Notify the RankThread
  rank_command_queue_.enqueue(UpdateCandidateCommand{candidate_});
  rank_thread_.PostCommandFromModelThread(&model_session_id_);

  return CtrlStatus::CTRL_OK;
}

void ModelThread::UpdateTargetBatchSize(const std::optional<AvgStd>& rps) {
  if (rps.has_value()) {
    double time_budget_ms = model_session_.latency_sla();
    time_budget_ms -= kCtrlPlaneLatencyUs;
    time_budget_ms -= kDataPlaneLatencyUs;
    target_batch_size_ =
        bse_.Estimate(profile_, time_budget_ms * 1e-3, rps->avg, rps->std);
  } else {
    double time_budget_ms = model_session_.latency_sla() / 2.0;
    target_batch_size_ = profile_.GetMaxBatchWithFullBudget(time_budget_ms);
  }
}

void ModelThread::UpdateCandidate(TimePoint earliest_exec_time) {
  auto rps = rps_meter_.Get(earliest_exec_time);
  UpdateTargetBatchSize(rps);
  batch_policy_.Update(earliest_exec_time, target_batch_size_);

  TimePoint latest_exec_time;
  TimePoint deadline;
  const auto& inputs = batch_policy_.inputs();
  if (!inputs.empty()) {
    auto elapse = EstimateExecElapse(profile_, inputs.size());
    latest_exec_time = (*inputs.begin())->deadline - elapse;
    deadline = (*inputs.begin())->deadline;
    drop_timer_.SetTimeout(deadline);
    drop_timer_.AsyncWait([this](ario::ErrorCode) { OnDropTimer(); });
  } else {
    latest_exec_time = TimePoint::max();
    deadline = TimePoint::max();
    drop_timer_.CancelAll();
  }

  uint32_t batch_size = batch_policy_.inputs().size();
  candidate_ = ExecutionCandidate{earliest_exec_time, latest_exec_time,
                                  deadline, batch_size};

  // Send dropped queries
  if (!batch_policy_.drops().empty()) {
    SendDroppedQueries(batch_policy_.PopDrops());
  }
}

void ModelThread::OnDropTimer() {
  auto now = Clock::now();
  auto earliest_exec_time = now +
                            std::chrono::microseconds(kDataPlaneLatencyUs) +
                            std::chrono::microseconds(kCtrlPlaneLatencyUs);
  UpdateCandidate(earliest_exec_time);
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
  }
}

void ModelThread::ExecuteCommand() {
  if (stop_flag_) {
    return;
  }
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
  auto inputs = batch_policy_.PopInputs();

  // Early return when batch_size=0
  if (inputs.empty()) {
    rank_command_queue_.enqueue(UpdateBackendCommand{cmd.backend_id, now});
    rank_command_queue_.enqueue(UpdateCandidateCommand{candidate_});
    rank_thread_.PostCommandFromModelThread(&model_session_id_);
    return;
  }

  // Inform RankThread about the backend's correct next_available_time
  auto exec_elapse = EstimateExecElapse(profile_, candidate_.batch_size);
  auto finish_time = exec_time + exec_elapse;
  rank_command_queue_.enqueue(
      UpdateBackendCommand{cmd.backend_id, finish_time});
  rank_thread_.PostCommandFromModelThread(&model_session_id_);

  // Prepare the batchplan
  BatchPlanProto proto;
  EnqueueQueryCommand query;
  proto.set_plan_id(cmd.plan_id.t);
  proto.set_model_session_id(model_session_id_);
  proto.set_exec_time_ns(
      duration_cast<nanoseconds>(exec_time.time_since_epoch()).count());
  proto.set_deadline_ns(
      duration_cast<nanoseconds>(candidate_.deadline.time_since_epoch())
          .count());
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
  auto delegate = dispatcher_.GetBackend(cmd.backend_id);
  if (!delegate) {
    LOG(ERROR) << "Cannot find backend delegate. backend_id="
               << cmd.backend_id.t;
    return;
  }
  delegate->EnqueueBatchPlan(std::move(proto));

  // Update candidate
  UpdateCandidate(exec_time);
  // Notify the RankThread about the new candidate
  rank_command_queue_.enqueue(UpdateCandidateCommand{candidate_});
  rank_thread_.PostCommandFromModelThread(&model_session_id_);
}

// ============ RankThread ============

RankThread::ActivePlan::ActivePlan(ario::EpollExecutor& executor)
    : send_timer(executor) {}

RankThread::BackendContext::BackendContext(
    NodeId backend_id, std::shared_ptr<BackendDelegate> delegate)
    : backend_id(backend_id),
      delegate(std::move(delegate)),
      next_available_time(std::chrono::nanoseconds(0)) {}

RankThread::RankThread(ario::EpollExecutor* executor)
    : executor_(*CHECK_NOTNULL(executor)), stop_flag_(false) {}

RankThread::~RankThread() {
  // TODO
  LOG_IF(ERROR, !stop_flag_) << "RankThread::Stop() not called!";
}

void RankThread::Stop(std::mutex& mutex, size_t& cnt,
                      std::condition_variable& cv) {
  // TODO
  executor_.Post(
      [this, &mutex, &cnt, &cv](ario::ErrorCode) {
        stop_flag_ = true;
        plans_.clear();
        backends_.clear();
        {
          std::lock_guard lock(mutex);
          ++cnt;
        }
        cv.notify_all();
      },
      ario::ErrorCode::kOk);
}

PlanId RankThread::NextPlanId() { return PlanId(next_plan_id_.t++); }

void RankThread::PostCommandFromModelThread(
    const std::string* ptr_model_session_id) {
  executor_.Post([this, ptr_model_session_id](
                     ario::ErrorCode) { ExecuteCommand(ptr_model_session_id); },
                 ario::ErrorCode::kOk);
}

void RankThread::ExecuteCommand(const std::string* ptr_model_session_id) {
  if (stop_flag_) {
    return;
  }
  auto& mdata = model_threads_.at(*ptr_model_session_id);
  auto visitor = make_visitor(
      [this, ptr_model_session_id](UpdateCandidateCommand& cmd) {
        DoUpdateCandidateCommand(cmd, ptr_model_session_id);
      },
      [this](UpdateBackendCommand& cmd) { DoUpdateBackendCommand(cmd); }
      // Force newline for clang-format
  );

  RankCommand command;
  while (mdata->rank_command_queue.try_dequeue(command)) {
    std::visit(visitor, command);
  }
}

void RankThread::DoUpdateCandidateCommand(
    UpdateCandidateCommand& cmd, const std::string* ptr_model_session_id) {
  auto& mdata = *model_threads_.at(*ptr_model_session_id);

  auto cinfo =
      std::shared_ptr<CandidateInfo>(new CandidateInfo{mdata, cmd.candidate});
  candidate_pool_.Upsert(*ptr_model_session_id, cinfo);

  UpdateActivePlans(cinfo->candidate.earliest_exec_time, mdata);
}

void RankThread::DoUpdateBackendCommand(UpdateBackendCommand& cmd) {
  auto& bctx = backends_.at(cmd.backend_id);
  UpdateBackend(bctx.get(), cmd.next_available_time);
}

void RankThread::PostAddModelThread(ModelSession model_session,
                                    ModelThread* model_thread) {
  executor_.Post(
      [this, model_session = std::move(model_session),
       model_thread](ario::ErrorCode) {
        DoAddModelThread(std::move(model_session), model_thread);
      },
      ario::ErrorCode::kOk);
}

void RankThread::PostAddBackend(
    NodeId backend_id, std::shared_ptr<BackendDelegate> backend_delegate) {
  executor_.Post(
      [this, backend_id, backend_delegate](ario::ErrorCode) {
        DoAddBackend(backend_id, backend_delegate);
      },
      ario::ErrorCode::kOk);
}

void RankThread::DoAddModelThread(ModelSession model_session,
                                  ModelThread* model_thread) {
  auto model_session_id = ModelSessionToString(model_session);
  if (model_threads_.count(model_session_id)) {
    LOG(ERROR) << "ModelThread already exists. model_sesion_id="
               << model_session_id;
    return;
  }
  auto& m = *CHECK_NOTNULL(model_thread);
  model_threads_[model_session_id] = std::unique_ptr<PerModelThreadData>(
      new PerModelThreadData{m, model_session_id, m.profile(),
                             *CHECK_NOTNULL(m.model_command_queue()),
                             *CHECK_NOTNULL(m.rank_command_queue()), nullptr});

  auto& mdata = *model_threads_[model_session_id];
  candidate_pool_.Upsert(model_session_id,
                         std::shared_ptr<CandidateInfo>(new CandidateInfo{
                             mdata, ExecutionCandidate::Invalid()}));
}

void RankThread::DoAddBackend(
    NodeId backend_id, std::shared_ptr<BackendDelegate> backend_delegate) {
  auto bctx = std::make_shared<BackendContext>(backend_id, backend_delegate);
  if (backends_.count(bctx->backend_id)) {
    LOG(ERROR) << "Backend already exists. backend_id=" << backend_id;
    return;
  }
  backend_availability_pool_.Upsert(backend_id, bctx->next_available_time);
  backends_[backend_id] = std::move(bctx);
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
  uint32_t batch_size = cinfo->candidate.batch_size;
  if (!batch_size) {
    mdata.active_plan = nullptr;
    return;
  }

  // Build plan
  auto plan = std::make_shared<ActivePlan>(executor_);
  plan->plan_id = NextPlanId();
  auto deadline = cinfo->candidate.deadline;
  auto frontrun_elapse = EstimateExecElapse(mdata.profile, batch_size + 1);
  auto frontrun_exec_time = deadline - frontrun_elapse;
  plan->exec_time = std::max(earliest_exec_time, frontrun_exec_time);
  auto send_time = plan->exec_time -
                   std::chrono::microseconds(kCtrlPlaneLatencyUs) -
                   std::chrono::microseconds(kDataPlaneLatencyUs);
  CHECK_LE(earliest_exec_time.time_since_epoch().count(),
           plan->exec_time.time_since_epoch().count());
  auto exec_elapse = EstimateExecElapse(mdata.profile, batch_size);
  auto finish_time = plan->exec_time + exec_elapse;
  plan->mdata = &cinfo->mdata;
  CHECK(finish_time <= deadline)
      << "diff = " << (finish_time - deadline).count() / 1e3 << "us";

  // Update bookkeeping
  mdata.active_plan = plan;
  plans_[plan->plan_id] = plan;

  // Setup timer
  plan->send_timer.SetTimeout(send_time);
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
  auto schedule_time = bctx->schedule_timer.timeout();
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
  auto timer_delay = now - plan->send_timer.timeout();
  if (timer_delay > microseconds(100)) {
    auto us = duration_cast<microseconds>(timer_delay).count();
    LOG(WARNING) << "OnPlanTimer: huge timer delay: " << us << " us";
  }
  auto& mdata = *plan->mdata;

  // Assign backend
  CHECK_GT(backend_availability_pool_.Size(), 0);
  auto backend_id = backend_availability_pool_.GetByRank(0).key.get();
  auto& bctx = backends_.at(backend_id);
  CHECK_LT(bctx->next_available_time.time_since_epoch().count(),
           plan->exec_time.time_since_epoch().count());

  // Mark backend unavailable until ModelThread gives UpdateBackendCommand
  UpdateBackend(bctx.get(), TimePoint::max());

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
  auto schedule_time = next_available_time -
                       std::chrono::microseconds(kDataPlaneLatencyUs) -
                       std::chrono::microseconds(kCtrlPlaneLatencyUs);
  bctx->schedule_timer.SetTimeout(schedule_time);
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
      rank_thread_(rank_thread_executor) {}

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
  std::mutex mutex;
  size_t cnt = 0;
  std::condition_variable cv;
  for (auto& pair : model_threads_) {
    pair.second->Stop(mutex, cnt, cv);
  }
  rank_thread_.Stop(mutex, cnt, cv);
  {
    size_t target = model_threads_.size() + 1;
    std::unique_lock lock(mutex);
    cv.wait(lock, [target, &cnt] { return cnt == target; });
  }
  model_threads_.clear();
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
  rank_thread_.PostAddModelThread(model_session, model_thread);
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
  rank_thread_.PostAddBackend(backend_id, delegate);
}

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus
