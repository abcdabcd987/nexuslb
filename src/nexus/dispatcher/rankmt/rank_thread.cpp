#include "nexus/dispatcher/rankmt/rank_thread.h"

#include <glog/logging.h>

#include <chrono>
#include <memory>
#include <mutex>

#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/rankmt/model_thread.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

RankThread::ActivePlan::ActivePlan(ario::EpollExecutor& executor)
    : send_timer(executor) {}

RankThread::BackendContext::BackendContext(
    NodeId backend_id, std::shared_ptr<BackendDelegate> delegate)
    : backend_id(backend_id),
      delegate(std::move(delegate)),
      next_available_time(std::chrono::nanoseconds(0)) {}

RankThread::RankThread(ario::EpollExecutor* executor)
    : executor_(*CHECK_NOTNULL(executor)), stop_flag_(false), poller_(this) {
  constexpr size_t kMaxModels = 512;

  // Prevent reallocation for thread safety.
  model_threads_.reserve(kMaxModels);

  executor_.AddPoller(poller_);
}

RankThread::~RankThread() {
  // TODO
  LOG_IF(ERROR, !stop_flag_) << "RankThread::Stop() not called!";
}

void RankThread::Stop(std::mutex& mutex, size_t& cnt,
                      std::condition_variable& cv) {
  // TODO
  executor_.PostBigCallback(
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

void RankThread::ExecuteCommand(PerModelThreadData& mdata) {
  if (stop_flag_) {
    return;
  }
  auto visitor = make_visitor(
      [this](UpdateBackendCommand& cmd) { DoUpdateBackendCommand(cmd); }
      // Force newline for clang-format
  );

  RankCommand command;
  while (mdata.rank_command_queue.try_dequeue(command)) {
    std::visit(visitor, command);
  }
}

void RankThread::PostExecutionCandidate(ModelIndex model_index,
                                        ExecutionCandidate candidate) {
  auto& mdata = model_threads_.at(model_index.t);
  CHECK(mdata);

  std::lock_guard lock(mdata->model_msg_mutex);
  mdata->model_msg.new_candidate = candidate;
}

void RankThread::PostResumeCandidateUpdate(ModelIndex model_index) {
  auto& mdata = model_threads_.at(model_index.t);
  CHECK(mdata);

  std::lock_guard lock(mdata->model_msg_mutex);
  CHECK(!mdata->model_msg.resume_candidate_update);
  mdata->model_msg.resume_candidate_update = true;
}

void RankThread::DoUpdateCandidate(PerModelThreadData& mdata) {
  std::optional<ExecutionCandidate> candidate;
  {
    std::lock_guard lock(mdata.model_msg_mutex);
    if (mdata.model_msg.new_candidate.has_value()) {
      candidate = std::move(mdata.model_msg.new_candidate);
      mdata.model_msg.new_candidate.reset();
    }
    if (mdata.model_msg.resume_candidate_update) {
      CHECK(mdata.rejecting_candidates);
      mdata.rejecting_candidates = false;
      mdata.model_msg.resume_candidate_update = false;
    }
  }
  if (!candidate.has_value() || mdata.rejecting_candidates) {
    return;
  }

  auto cinfo = std::shared_ptr<CandidateInfo>(
      new CandidateInfo{mdata, candidate.value()});
  candidate_pool_.Upsert(mdata.model_index, cinfo);
  CHECK_EQ(candidate_pool_.Size(), model_threads_.size());

  SetupActivePlan(mdata);
}

void RankThread::DoUpdateBackendCommand(UpdateBackendCommand& cmd) {
  auto& bctx = backends_.at(cmd.backend_id);
  UpdateBackend(bctx.get(), cmd.next_available_time);
}

void RankThread::PostAddModelThread(ModelIndex model_index,
                                    ModelThread* model_thread) {
  executor_.PostBigCallback(
      [this, model_index, model_thread](ario::ErrorCode) {
        if (model_threads_.size() > model_index.t &&
            model_threads_[model_index.t]) {
          LOG(ERROR)
              << "ModelThread already exists. model_index=" << model_index.t
              << " model_sesion_id="
              << model_threads_[model_index.t]->model_thread.model_session_id();
          return;
        }
        auto& m = *CHECK_NOTNULL(model_thread);

        // Ensure no realloaction for thread safety.
        CHECK_LT(model_index.t, model_threads_.capacity());
        if (model_threads_.size() <= model_index.t) {
          model_threads_.resize(model_index.t + 1);
        }

        model_threads_[model_index.t] =
            std::unique_ptr<PerModelThreadData>(new PerModelThreadData{
                m, model_index, m.profile(),
                *CHECK_NOTNULL(m.rank_command_queue()), nullptr, false});

        auto& mdata = *model_threads_[model_index.t];
        candidate_pool_.Upsert(model_index,
                               std::shared_ptr<CandidateInfo>(new CandidateInfo{
                                   mdata, ExecutionCandidate::Invalid()}));
      },
      ario::ErrorCode::kOk);
}

void RankThread::PostAddBackend(NodeId backend_id,
                                std::shared_ptr<BackendDelegate> delegate) {
  executor_.PostBigCallback(
      [this, backend_id, delegate = std::move(delegate)](ario::ErrorCode) {
        auto bctx = std::make_shared<BackendContext>(backend_id, delegate);
        if (backends_.count(bctx->backend_id)) {
          LOG(ERROR) << "Backend already exists. backend_id=" << backend_id;
          return;
        }
        backend_availability_pool_.Upsert(backend_id,
                                          bctx->next_available_time);
        backends_[backend_id] = std::move(bctx);
      },
      ario::ErrorCode::kOk);
}

void RankThread::PostRemoveBackend(NodeId backend_id) {
  executor_.PostOk([this, backend_id](ario::ErrorCode) {
    backend_availability_pool_.Remove(backend_id);
    backends_.erase(backend_id);
  });
}

void RankThread::SetupActivePlan(PerModelThreadData& mdata) {
  auto& cinfo = candidate_pool_.GetByKey(mdata.model_index);
  uint32_t batch_size = cinfo->candidate.batch_size;
  if (!batch_size) {
    mdata.active_plan = nullptr;
    return;
  }

  // Build plan
  constexpr auto kInterThreadLatency = std::chrono::microseconds(1000);
  auto plan = std::make_shared<ActivePlan>(executor_);
  plan->plan_id = NextPlanId();
  auto deadline = cinfo->candidate.deadline;
  auto frontrun_elapse = EstimateExecElapse(mdata.profile, batch_size + 1);
  auto frontrun_exec_time = deadline - frontrun_elapse - kInterThreadLatency;
  auto earliest_exec_time = cinfo->candidate.earliest_exec_time;
  plan->exec_time = std::max(earliest_exec_time, frontrun_exec_time);
  CHECK_LE(earliest_exec_time.time_since_epoch().count(),
           plan->exec_time.time_since_epoch().count());
  auto exec_elapse = EstimateExecElapse(mdata.profile, batch_size);
  auto finish_time = plan->exec_time + exec_elapse;
  plan->mdata = &cinfo->mdata;
  CHECK(finish_time <= deadline)
      << "diff = " << (finish_time - deadline).count() / 1e3 << "us"
      << " earliest_exec_time-candidate.latest_exec_time = "
      << (earliest_exec_time - cinfo->candidate.latest_exec_time).count() / 1e3
      << "us";

  // Remove old plan. Timer will be cancelled by the destructor.
  if (mdata.active_plan) {
    plans_.erase(mdata.active_plan->plan_id);
    CHECK_EQ(mdata.active_plan.use_count(), 1);
  }

  // Update bookkeeping
  mdata.active_plan = plan;
  plans_[plan->plan_id] = plan;

  // Setup timer
  auto send_time = plan->exec_time - kCtrlPlaneLatency - kDataPlaneLatency;
  auto now = Clock::now();
  plan->send_timer.SetTimeout(std::max(send_time, now));
  plan->send_timer.AsyncWait(
      [this, plan_id = plan->plan_id](ario::ErrorCode error) {
        if (error == ario::ErrorCode::kCancelled) return;
        OnPlanTimer(plan_id);
      });
}

void RankThread::OnPlanTimer(PlanId plan_id) {
  using namespace std::chrono;
  TimePoint now = Clock::now();
  std::shared_ptr<ActivePlan> plan;
  {
    auto iter = plans_.find(plan_id);
    if (iter == plans_.end()) {
      // Cancelled plan. Do nothing.
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
  CHECK_EQ(mdata.active_plan, plan);
  mdata.active_plan = nullptr;

  // Try to assign backend if possible
  CHECK_EQ(backend_availability_pool_.Size(), backends_.size());
  if (backend_availability_pool_.Size() == 0) {
    return;
  }
  auto backend_id = backend_availability_pool_.GetByRank(0).key.get();
  auto& bctx = backends_.at(backend_id);
  if (bctx->next_available_time > plan->exec_time) {
    return;
  }

  // Let ModelThread send out the plan
  GrantedBackendMessage msg;
  msg.backend_id = backend_id;
  msg.plan_id = plan->plan_id;
  msg.next_available_time = bctx->next_available_time;
  mdata.model_thread.PostGrantedBackend(msg);
  VLOG(1) << "GrantBackend " << mdata.model_thread.model_session().model_name()
          << " id=" << plan->plan_id.t << " backend=" << backend_id;

  // Mark backend unavailable.
  // Also set the candidate of this model to be invalid.
  // ModelThread will give us updates on the backend and new candidates.
  UpdateBackend(bctx.get(), TimePoint::max());
  candidate_pool_.Upsert(mdata.model_index,
                         std::shared_ptr<CandidateInfo>(new CandidateInfo{
                             mdata, ExecutionCandidate::Invalid()}));

  // Reject candidate updates until ModelThread picks up the granted backend.
  // Because after the backend is granted and before ModelThread picks it up,
  // all candidates sent by ModelThread are invalid.
  mdata.rejecting_candidates = true;
}

void RankThread::UpdateBackend(BackendContext* bctx,
                               TimePoint next_available_time) {
  bctx->next_available_time = next_available_time;
  backend_availability_pool_.Upsert(bctx->backend_id, next_available_time);
}

void RankThread::Poll() {
  for (auto& mdata : model_threads_) {
    if (!mdata) {
      continue;
    }
    ExecuteCommand(*mdata);
    DoUpdateCandidate(*mdata);
  }
}

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus
