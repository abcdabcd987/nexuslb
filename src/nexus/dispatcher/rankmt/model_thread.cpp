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
    ario::EpollExecutor* executor, ModelSession model_session,
    ModelIndex model_index, const ModelProfile& profile,
    RankThread* rank_thread,
    std::unordered_map<NodeId, std::shared_ptr<FrontendDelegate>> frontends,
    std::unordered_map<NodeId, std::shared_ptr<BackendDelegate>> backends)
    : executor_(*CHECK_NOTNULL(executor)),
      rank_thread_(*CHECK_NOTNULL(rank_thread)),
      model_session_(std::move(model_session)),
      model_session_id_(ModelSessionToString(model_session_)),
      model_index_(model_index),
      profile_(profile),
      stop_flag_(false),
      frontends_(std::move(frontends)),
      backends_(std::move(backends)),
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
  executor_.PostBigCallback(
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

void ModelThread::PostAddBackend(NodeId backend_id,
                                 std::shared_ptr<BackendDelegate> delegate) {
  executor_.PostBigCallback(
      [this, backend_id, delegate = std::move(delegate)](ario::ErrorCode) {
        backends_[backend_id] = delegate;
      },
      ario::ErrorCode::kOk);
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
  executor_.PostOk(
      [this, backend_id](ario::ErrorCode) { backends_.erase(backend_id); });
}

void ModelThread::PostRemoveFrontend(NodeId frontend_id) {
  executor_.PostOk(
      [this, frontend_id](ario::ErrorCode) { frontends_.erase(frontend_id); });
}

void ModelThread::PostCommand() {
  executor_.PostOk([this](ario::ErrorCode) { ExecuteCommand(); });
}

CtrlStatus ModelThread::EnqueueQuery(DispatchRequest&& request) {
  CHECK_EQ(ario::EpollExecutor::ThisThreadExecutor(), &executor_);

  ModelIndex model_index(request.query_without_input().model_index());
  if (model_index.t != model_index_.t) {
    LOG(ERROR) << "Wrong ModelThread. global_id="
               << request.query_without_input().global_id()
               << ", requested model_index: " << model_index.t
               << ", this model_index: " << model_index_.t << " "
               << model_session_id_;
    return CtrlStatus::MODEL_SESSION_NOT_LOADED;
  }

  auto deadline =
      TimePoint(std::chrono::nanoseconds(
                    request.query_without_input().clock().frontend_recv_ns()) +
                std::chrono::milliseconds(model_session_.latency_sla()) -
                std::chrono::microseconds(kDataPlaneLatencyUs));
  auto qctx = std::make_shared<QueryContext>(std::move(request), deadline);
  const auto& query = qctx->request.query_without_input();
  auto now = Clock::now();

  // Add to pending queries
  rps_meter_.Hit(now);
  unprocessed_queries_.insert(qctx);

  // Update schedule
  auto earliest_exec_time = now +
                            std::chrono::microseconds(kDataPlaneLatencyUs) +
                            std::chrono::microseconds(kCtrlPlaneLatencyUs);
  UpdateCandidate(earliest_exec_time);

  // Notify the RankThread
  rank_command_queue_.enqueue(UpdateCandidateCommand{candidate_});
  rank_thread_.PostCommandFromModelThread(model_index);

  return CtrlStatus::CTRL_OK;
}

void ModelThread::UpdateTargetBatchSize(const std::optional<AvgStd>& rps) {
  if (rps.has_value()) {
    double time_budget = model_session_.latency_sla() * 1e-3;
    time_budget -= kCtrlPlaneLatencyUs * 1e-6;
    time_budget -= kDataPlaneLatencyUs * 1e-6;
    target_batch_size_ =
        bse_.Estimate(profile_, time_budget, rps->avg, rps->std);
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
    reply.add_query_id_list(proto.query_id());
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
    rank_command_queue_.enqueue(
        UpdateBackendCommand{cmd.backend_id, TimePoint::min()});
    rank_command_queue_.enqueue(UpdateCandidateCommand{candidate_});
    rank_thread_.PostCommandFromModelThread(model_index_);
    return;
  }

  // Inform RankThread about the backend's correct next_available_time
  auto exec_elapse = EstimateExecElapse(profile_, candidate_.batch_size);
  auto finish_time = exec_time + exec_elapse;
  rank_command_queue_.enqueue(
      UpdateBackendCommand{cmd.backend_id, finish_time});
  rank_thread_.PostCommandFromModelThread(model_index_);

  // Prepare the batchplan
  BatchPlanProto proto;
  EnqueueQueryCommand query;
  proto.set_plan_id(cmd.plan_id.t);
  proto.set_model_index(model_index_);
  proto.set_exec_time_ns(
      duration_cast<nanoseconds>(exec_time.time_since_epoch()).count());
  proto.set_deadline_ns(
      duration_cast<nanoseconds>(candidate_.deadline.time_since_epoch())
          .count());
  proto.set_expected_finish_time_ns(
      duration_cast<nanoseconds>(finish_time.time_since_epoch()).count());
  for (auto& qctx : inputs) {
    auto* query_without_input = query.mutable_query_without_input();
    query_without_input->Swap(qctx->request.mutable_query_without_input());
    query_without_input->clear_model_index();
    query.set_rdma_read_offset(qctx->request.rdma_read_offset());
    query.set_rdma_read_length(qctx->request.rdma_read_length());
    qctx->request.Clear();
    *proto.add_queries() = std::move(query);
  }
  VLOG(1) << "Send BatchPlan: " << model_session_id_
          << " batch_size=" << proto.queries_size()
          << " elapse=" << exec_elapse.count() / 1e6 << "ms"
          << " target_batch_size=" << target_batch_size_;
  // Update punch clock
  auto dispatcher_dispatch_ns =
      duration_cast<nanoseconds>(Clock::now().time_since_epoch()).count();
  for (auto& query : *proto.mutable_queries()) {
    query.mutable_query_without_input()
        ->mutable_clock()
        ->set_dispatcher_dispatch_ns(dispatcher_dispatch_ns);
  }
  // Send to backend
  auto iter = backends_.find(cmd.backend_id);
  if (iter != backends_.end()) {
    auto& delegate = iter->second;
    delegate->EnqueueBatchPlan(std::move(proto));
  } else {
    LOG(ERROR) << "Cannot find backend delegate. backend_id="
               << cmd.backend_id.t;
    return;
  }

  // Update candidate
  UpdateCandidate(exec_time);
  // Notify the RankThread about the new candidate
  rank_command_queue_.enqueue(UpdateCandidateCommand{candidate_});
  rank_thread_.PostCommandFromModelThread(model_index_);
}

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus
