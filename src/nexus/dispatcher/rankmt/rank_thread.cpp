#include "nexus/dispatcher/rankmt/rank_thread.h"

#include <glog/logging.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>

#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/rankmt/model_thread.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

RankThread::GpuContext::GpuContext(GpuId gpu_id, GpuDelegate* delegate)
    : gpu_id(gpu_id),
      delegate(delegate),
      free_at(std::chrono::nanoseconds(0)) {}

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
        gpus_.clear();
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
  auto visitor =
      make_visitor([this](UpdateGpuCommand& cmd) { DoUpdateGpuCommand(cmd); }
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

  mdata.candidate = std::move(candidate);
  SetupActivePlan(mdata);
}

void RankThread::DoUpdateGpuCommand(UpdateGpuCommand& cmd) {
  auto& gctx = gpus_.at(cmd.gpu_id);
  UpdateGpu(gctx.get(), cmd.free_at);
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
                *CHECK_NOTNULL(m.rank_command_queue()), ario::Timer(executor_),
                false, std::nullopt});
      },
      ario::ErrorCode::kOk);
}

void RankThread::PostAddBackend(NodeId backend_id,
                                std::shared_ptr<BackendDelegate> backend) {
  executor_.PostBigCallback(
      [this, backend_id, backend = std::move(backend)](ario::ErrorCode) {
        // Add backend
        if (backends_.count(backend_id)) {
          LOG(ERROR) << "Backend already exists. backend_id=" << backend_id;
          return;
        }
        backends_[backend_id] = backend;

        // Add GPUs
        for (auto gpu : backend->GetGpuDelegates()) {
          auto gpu_id = gpu->gpu_id();
          CHECK(!gpus_.count(gpu_id));
          auto gctx = std::make_shared<GpuContext>(gpu_id, gpu);
          gpus_[gpu_id] = gctx;
          gpu_availability_pool_.Upsert(gpu_id, gctx->free_at);
        }
      },
      ario::ErrorCode::kOk);
}

void RankThread::PostRemoveBackend(NodeId backend_id) {
  executor_.PostOk([this, backend_id](ario::ErrorCode) {
    CHECK(backends_.count(backend_id));
    auto backend = backends_[backend_id];

    // Remove GPUs
    for (auto gpu : backend->GetGpuDelegates()) {
      auto gpu_id = gpu->gpu_id();
      gpu_availability_pool_.Remove(gpu_id);
      gpus_.erase(gpu_id);
    }

    // Remove backend
    backends_.erase(backend_id);
  });
}

void RankThread::SetupActivePlan(PerModelThreadData& mdata) {
  constexpr auto kInterThreadLatency = std::chrono::microseconds(100);
  CHECK(mdata.candidate.has_value());
  const auto& candidate = mdata.candidate.value();

  auto now = Clock::now();
  auto send_at = candidate.exec_at - kCtrlPlaneLatency - kDataPlaneLatency -
                 kInterThreadLatency;
  mdata.send_timer.SetTimeout(std::max(send_at, now));
  mdata.send_timer.AsyncWait([this, pmdata = &mdata](ario::ErrorCode error) {
    if (error == ario::ErrorCode::kCancelled) return;
    OnPlanTimer(*pmdata);
  });
}

void RankThread::OnPlanTimer(PerModelThreadData& mdata) {
  using namespace std::chrono;
  if (stop_flag_) return;
  TimePoint now = Clock::now();
  auto timer_delay = now - mdata.send_timer.timeout();
  if (timer_delay > microseconds(100)) {
    auto us = duration_cast<microseconds>(timer_delay).count();
    LOG(WARNING) << "OnPlanTimer: huge timer delay: " << us << " us";
  }

  // Try to assign backend if possible
  CHECK_EQ(gpu_availability_pool_.Size(), gpus_.size());
  if (gpu_availability_pool_.Size() == 0) {
    return;
  }
  auto gpu_id = gpu_availability_pool_.GetByRank(0).key.get();
  auto& gctx = gpus_.at(gpu_id);
  if (gctx->free_at > mdata.candidate->exec_at) {
    return;
  }

  // Let ModelThread send out the plan
  GrantedGpuMessage msg;
  msg.gpu_id = gpu_id;
  msg.plan_id = NextPlanId();
  msg._debug_free_at = gctx->free_at;
  mdata.model_thread.PostGrantedGpu(msg);
  VLOG(1) << "GrantBackend " << mdata.model_thread.model_session().model_name()
          << " id=" << msg.plan_id.t << " gpu=" << gpu_id;

  // Mark backend unavailable.
  // Also set the candidate of this model to be invalid.
  // ModelThread will give us updates on the backend and new candidates.
  UpdateGpu(gctx.get(), TimePoint::max());
  mdata.candidate = std::nullopt;

  // Reject candidate updates until ModelThread picks up the granted backend.
  // Because after the backend is granted and before ModelThread picks it up,
  // all candidates sent by ModelThread are invalid.
  mdata.rejecting_candidates = true;
}

void RankThread::UpdateGpu(GpuContext* gctx, TimePoint free_at) {
  gctx->free_at = free_at;
  gpu_availability_pool_.Upsert(gctx->gpu_id, free_at);
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
