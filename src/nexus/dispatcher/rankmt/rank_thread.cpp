#include "nexus/dispatcher/rankmt/rank_thread.h"

#include <glog/logging.h>

#include <chrono>
#include <memory>
#include <mutex>

#include "nexus/common/model_def.h"
#include "nexus/dispatcher/rankmt/model_thread.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

RankThread::ActivePlan::ActivePlan(ario::EpollExecutor& executor)
    : send_timer(executor) {}

RankThread::BackendContext::BackendContext(
    ario::EpollExecutor* executor, NodeId backend_id,
    std::shared_ptr<BackendDelegate> delegate)
    : backend_id(backend_id),
      delegate(std::move(delegate)),
      next_available_time(std::chrono::nanoseconds(0)),
      schedule_timer(*CHECK_NOTNULL(executor)) {}

RankThread::RankThread(ario::EpollExecutor* executor)
    : executor_(*CHECK_NOTNULL(executor)), stop_flag_(false) {}

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

void RankThread::PostCommandFromModelThread(
    const std::string* ptr_model_session_id) {
  executor_.PostOk([this, ptr_model_session_id](ario::ErrorCode) {
    ExecuteCommand(ptr_model_session_id);
  });
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
  executor_.PostBigCallback(
      [this, model_session = std::move(model_session),
       model_thread](ario::ErrorCode) {
        auto model_session_id = ModelSessionToString(model_session);
        if (model_threads_.count(model_session_id)) {
          LOG(ERROR) << "ModelThread already exists. model_sesion_id="
                     << model_session_id;
          return;
        }
        auto& m = *CHECK_NOTNULL(model_thread);
        model_threads_[model_session_id] =
            std::unique_ptr<PerModelThreadData>(new PerModelThreadData{
                m, model_session_id, m.profile(),
                *CHECK_NOTNULL(m.model_command_queue()),
                *CHECK_NOTNULL(m.rank_command_queue()), nullptr});

        auto& mdata = *model_threads_[model_session_id];
        candidate_pool_.Upsert(model_session_id,
                               std::shared_ptr<CandidateInfo>(new CandidateInfo{
                                   mdata, ExecutionCandidate::Invalid()}));
      },
      ario::ErrorCode::kOk);
}

void RankThread::PostAddBackend(NodeId backend_id,
                                std::shared_ptr<BackendDelegate> delegate) {
  executor_.PostBigCallback(
      [this, backend_id, delegate = std::move(delegate)](ario::ErrorCode) {
        auto bctx =
            std::make_shared<BackendContext>(&executor_, backend_id, delegate);
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

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus
