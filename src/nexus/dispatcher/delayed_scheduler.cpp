#include "nexus/dispatcher/delayed_scheduler.h"

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

#include "nexus/common/metric.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/dispatcher.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {
namespace delayed {

namespace {

constexpr uint32_t kCountIntervalSec = 1;
constexpr uint32_t kAvgIntervalSec = 5;
constexpr uint32_t kCtrlPlaneLatencyUs = 2000;
constexpr uint32_t kDataPlaneLatencyUs = 5000;

}  // namespace

QueryContext::QueryContext(DispatchRequest request, TimePoint deadline)
    : request(std::move(request)),
      global_id(this->request.query_without_input().global_id()),
      deadline(deadline) {}

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
      req_rate(kCountIntervalSec, kAvgIntervalSec) {
  string_id = ModelSessionToString(this->model_session);
  req_counter =
      MetricRegistry::Singleton().CreateIntervalCounter(kCountIntervalSec);
}

double ModelSessionContext::GetRequestRate() const {
  for (auto nreq : req_counter->GetHistory()) {
    if (req_rate.rate() < 0 && nreq == 0) {
      continue;
    }
    req_rate.AddSample(nreq);
  }
  return req_rate.rate();
}

BackendContext::BackendContext(NodeId backend_id,
                               std::shared_ptr<BackendDelegate> delegate)
    : backend_id(backend_id),
      delegate(std::move(delegate)),
      next_available_time(std::chrono::nanoseconds(0)) {}

std::unique_ptr<Scheduler> DelayedScheduler::Builder::Build(
    std::unique_ptr<DispatcherAccessor> dispatcher) {
  return std::make_unique<DelayedScheduler>(std::move(dispatcher));
}

DelayedScheduler::DelayedScheduler(
    std::unique_ptr<DispatcherAccessor> dispatcher)
    : Scheduler(std::move(dispatcher)),
      bse_(1.0, 0.0),
      io_context_work_guard_(io_context_.get_executor()) {}

void DelayedScheduler::RunAsWorker() { io_context_.run(); }

void DelayedScheduler::Stop() { io_context_work_guard_.reset(); }

void DelayedScheduler::AddModelSession(ModelSession model_session) {
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
    }
  }
}

void DelayedScheduler::AddBackend(NodeId backend_id) {
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
    }
  }
}

CtrlStatus DelayedScheduler::EnqueueQuery(DispatchRequest&& request) {
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

  // Add to pending queries
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queries_.count(qctx->global_id)) {
      LOG(ERROR) << "Query already exists. global_id=" << qctx->global_id.t;
      return CtrlStatus::CTRL_GLOBAL_ID_CONFLICT;
    }
    const auto& model_session_id =
        qctx->request.query_without_input().model_session_id();
    auto miter = models_.find(model_session_id);
    if (miter == models_.end()) {
      LOG(ERROR) << "Cannot find model session. global_id=" << qctx->global_id.t
                 << ", model_session=" << model_session_id;
      return CtrlStatus::MODEL_SESSION_NOT_LOADED;
    }
    queries_[qctx->global_id] = qctx;
    auto& mctx = miter->second;
    mctx->queries.insert(qctx);
  }

  // Trigger full schedule on the worker thread.
  boost::asio::post(io_context_, [this] { WorkFullSchedule(); });
  VLOG(1) << "EnqueueQuery success. global_id=" << qctx->global_id.t;
  return CtrlStatus::CTRL_OK;
}

PlanId DelayedScheduler::NextPlanId() { return PlanId(next_plan_id_.t++); }

void DelayedScheduler::WorkFullSchedule() {
  std::unique_lock<std::mutex> lock(mutex_);
  VLOG(1) << "WorkFullSchedule: start";

  // Drop timeout queries. Replies will be sent later.
  auto dropped = DropTimeoutQueries();
  if (!dropped.empty()) {
    VLOG(1) << "WorkFullSchedule: will drop " << dropped.size() << " queries.";
  }

  // Copies all remaining queries
  std::unordered_map<std::string, std::set<GlobalId>> remains;
  for (auto& pair : models_) {
    remains.try_emplace(pair.first);
    auto& global_ids = remains[pair.first];
    for (auto& qctx : pair.second->queries) {
      global_ids.insert(qctx->global_id);
    }
  }

  // Sort backends by next_available_time
  std::vector<std::shared_ptr<BackendContext>> backends;
  for (auto& pair : backends_) {
    backends.push_back(pair.second);
  }
  std::sort(backends.begin(), backends.end(),
            [](const auto& lhs, const auto& rhs) {
              return lhs->next_available_time < rhs->next_available_time;
            });

  // Schedule each backend
  for (auto& bctx : backends) {
    // Try all model sessions to schedule on this backend
    std::optional<BatchPlan> best_plan;
    for (auto& pair : remains) {
      auto& remain_queries = pair.second;
      if (remain_queries.empty()) {
        continue;
      }
      auto candidate = TryScheduleModelSessionOnBackend(
          bctx, models_[pair.first], remain_queries);

      // Update the best plan.
      // Pick the one that has the earliest exec_time. The intuition is to give
      // model sessions that have plenty of time left some time to accumulate
      // more requests, thus avoiding running them with a small batch size.
      if (!candidate) {
        continue;
      }
      if (!best_plan || candidate->exec_time < best_plan->exec_time) {
        best_plan = std::move(candidate);
      }
    }

    // Use the best plan as the backend's next plan.
    if (!best_plan) {
      // Deprecate previously scheduled plan.
      // The timer will be cancelled by the destructor.
      bctx->next_plan = std::nullopt;
      VLOG(1) << "WorkFullSchedule: Backend " << bctx->backend_id.t
              << " assigned no BatchPlan.";
    } else {
      bctx->next_plan = std::move(best_plan);
      auto& plan = *bctx->next_plan;
      plan.plan_id = NextPlanId();
      VLOG(1) << "WorkFullSchedule: Backend " << bctx->backend_id.t
              << " assigned a BatchPlan. plan_id=" << plan.plan_id
              << ", model_session=" << plan.model_session_id
              << ", batch_size=" << plan.actual_batch_size
              << ", send_time=" << plan.send_time.time_since_epoch().count()
              << ", exec_time=" << plan.exec_time.time_since_epoch().count()
              << ", finish_time="
              << plan.finish_time.time_since_epoch().count();

      // Remove scheduled queries so that the next backend won't see them.
      auto& remain_queries = remains[plan.model_session_id];
      for (auto global_id : plan.global_ids) {
        remain_queries.erase(global_id);
      }

      // Setup timer to finalize and send out batch plans.
      plan.send_timer =
          std::make_unique<boost::asio::basic_waitable_timer<Clock>>(
              io_context_, plan.send_time);
      auto backend_id = plan.backend_id;
      auto plan_id = plan.plan_id;
      plan.send_timer->async_wait(
          [backend_id, plan_id, this](const boost::system::error_code& ec) {
            if (ec) return;
            WorkFinalizePlan(backend_id, plan_id);
          });
    }
  }

  // Send replies about dropped queries
  lock.unlock();
  DispatchReply dispatch_reply;
  for (auto& qctx : dropped) {
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
  VLOG(1) << "WorkFullSchedule: done";
}

std::vector<std::shared_ptr<QueryContext>>
DelayedScheduler::DropTimeoutQueries() {
  auto now = Clock::now();
  std::vector<std::shared_ptr<QueryContext>> dropped;
  for (auto& pair : models_) {
    auto& mctx = pair.second;
    auto& queries = mctx->queries;
    for (auto iter = queries.begin(); iter != queries.end();) {
      auto& qctx = *iter;
      if (qctx->deadline > now) {
        break;
      }
      dropped.push_back(qctx);
      queries_.erase(qctx->global_id);
      iter = queries.erase(iter);
    }
  }
  return dropped;
}

std::optional<BatchPlan> DelayedScheduler::TryScheduleModelSessionOnBackend(
    std::shared_ptr<const BackendContext> bctx,
    std::shared_ptr<const ModelSessionContext> mctx,
    const std::set<GlobalId>& query_ids) {
  using namespace std::chrono;
  auto now = Clock::now();
  double rps = mctx->GetRequestRate();
  double time_budget = mctx->model_session.latency_sla() * 1e-3;
  if (!mctx->profile) {
    LOG(ERROR) << "ModelSession doesn't have a profile. model_session="
               << mctx->string_id;
    return std::nullopt;
  }
  uint32_t reserved_batch_size =
      bse_.Estimate(*mctx->profile, time_budget, rps, 0.0);
  auto proc_elapse = nanoseconds(0);
  // TODO: WithNStd
  proc_elapse += nanoseconds(
      static_cast<long>(mctx->profile->GetPreprocessLatency() * 1e3));
  proc_elapse += nanoseconds(
      static_cast<long>(mctx->profile->GetPostprocessLatency() * 1e3));
  auto plan_recv_time = now + microseconds(kCtrlPlaneLatencyUs) +
                        microseconds(kDataPlaneLatencyUs);
  auto eexec_time = std::max(bctx->next_available_time, plan_recv_time);

  // Sliding window policy
  std::vector<std::shared_ptr<QueryContext>> inputs;
  auto qiter = query_ids.begin();
  size_t qremain = query_ids.size();
  uint32_t bs = std::min(static_cast<uint32_t>(qremain), reserved_batch_size);
  while (inputs.size() < bs && qiter != query_ids.end()) {
    --qremain;
    auto& qctx = queries_.at(*qiter);
    auto deadline = inputs.empty() ? qctx->deadline : inputs[0]->deadline;
    auto fwd_elapse = nanoseconds(
        static_cast<long>(mctx->profile->GetForwardLatency(bs) * 1e3));
    auto finish_time = eexec_time + fwd_elapse + proc_elapse;
    if (deadline > finish_time) {
      inputs.push_back(qctx);
    } else {
      // Not actually dropping the query. Skipping instead.
      // Just in case another backend can pick it up.
      bs = std::min(static_cast<uint32_t>(inputs.size() + qremain),
                    reserved_batch_size);
    }
    ++qiter;
  }

  // See if there is any free lunch.
  while (qiter != query_ids.end()) {
    --qremain;
    auto& qctx = queries_.at(*qiter);
    auto deadline = inputs.empty() ? qctx->deadline : inputs[0]->deadline;
    bs = inputs.size() + 1;
    auto fwd_elapse = nanoseconds(
        static_cast<long>(mctx->profile->GetForwardLatency(bs) * 1e3));
    auto finish_time = eexec_time + fwd_elapse + proc_elapse;
    if (deadline > finish_time) {
      inputs.push_back(qctx);
    } else {
      break;
    }
    ++qiter;
  }

  // If there is no inputs for the new batch, consider other model sessions.
  if (inputs.empty()) {
    return std::nullopt;
  }

  // Build the new BatchPlan.
  //
  // Heuristic: Calculate exec_time using batch size + 1.
  //
  //                                                             deadline
  //     ABCDEF      X   [exec_time = deadline - f(batch_size)   ]
  //     ABCDEF   [exec_time = deadline - f(batch_size + 1)      ]
  //
  // Effectively, this heuristic makes the batch start earlier.
  //
  // Suppose there is no additional requests coming in between the gap. It won't
  // hurt to move the batch earlier. In fact, it'll make the batch finish
  // earlier.
  //
  // Suppose there is a request X in between. With the heuristic, X will be left
  // for the future batch. Without the heuristic, notice that the batch won't
  // fit X in unless it drops the head of queue. Hence, it won't gain anything
  // for adding X. What's worse, dropping the head of queue prolongs the
  // deadline, which might further prolong the exec_time and potentially
  // dropping more heads of queue.
  //
  // Therefore, this heuristic is very safe and effective.
  // The only caveat is that the analysis above assumes X won't move the batch's
  // deadline ahead. Shall the X arrived at the dispatcher out of order and has
  // a deadline earlier than A's, the analysis doesn't hold. However, it is
  // worth pointing out that such long a delay is less common, almost unlikely.
  auto fwd1_elapse = nanoseconds(static_cast<long>(
      mctx->profile->GetForwardLatency(inputs.size() + 1) * 1e3));
  auto exec1_elapse = fwd1_elapse + proc_elapse;
  auto earliest_deadline = inputs[0]->deadline;
  auto exec_time = std::max(eexec_time, earliest_deadline - exec1_elapse);
  auto fwd_elapse = nanoseconds(
      static_cast<long>(mctx->profile->GetForwardLatency(inputs.size()) * 1e3));
  auto exec_elapse = fwd_elapse + proc_elapse;
  auto finish_time = exec_time + fwd_elapse + proc_elapse;
  auto send_time = exec_time - microseconds(kCtrlPlaneLatencyUs) -
                   microseconds(kDataPlaneLatencyUs);
  LOG_IF(ERROR, send_time < now)
      << "send_time < now. send_time=" << send_time.time_since_epoch().count()
      << ", now=" << now.time_since_epoch().count();

  BatchPlan plan;
  plan.plan_id.t = 0;
  plan.backend_id = bctx->backend_id;
  plan.model_session_id = mctx->string_id;
  plan.send_time = send_time;
  plan.exec_time = exec_time;
  plan.finish_time = finish_time;
  plan.earliest_deadline = earliest_deadline;
  plan.actual_batch_size = inputs.size();
  plan.reserved_batch_size = reserved_batch_size;
  plan.exec_elapse = exec_elapse;
  for (const auto& qctx : inputs) {
    plan.global_ids.push_back(qctx->global_id);
  }
  return plan;
}

void DelayedScheduler::WorkFinalizePlan(NodeId backend_id, PlanId plan_id) {
  using namespace std::chrono;
  VLOG(1) << "WorkFinalizePlan: start. backend_id=" << backend_id.t
          << ", plan_id=" << plan_id.t;
  std::unique_lock<std::mutex> lock(mutex_);
  if (!backends_.count(backend_id)) {
    LOG(ERROR) << "WorkFinalizePlan: Cannot find backend. plan_id=" << plan_id.t
               << ", backend_id=" << backend_id.t;
    return;
  }
  auto& bctx = backends_.at(backend_id);
  if (!bctx->next_plan || bctx->next_plan->plan_id != plan_id) {
    // Skip if this is a deprecated plan.
    return;
  }
  auto plan = std::move(*bctx->next_plan);
  if (!models_.count(plan.model_session_id)) {
    LOG(ERROR) << "WorkFinalizePlan: Cannot find model session. plan_id="
               << plan_id.t << ", backend_id=" << backend_id.t
               << ", model_session_id=" << plan.model_session_id;
    return;
  }
  auto& mctx = models_.at(plan.model_session_id);
  auto now = Clock::now();
  if (duration_cast<microseconds>(now - plan.send_time) > microseconds(100)) {
    LOG(WARNING) << "WorkFinalizePlan: Huge timer offset. plan.send_time="
                 << plan.send_time.time_since_epoch().count()
                 << ", now=" << now.time_since_epoch().count();
  }

  // Update backend context
  CHECK(bctx->next_available_time <= plan.exec_time);
  bctx->next_available_time = plan.finish_time;
  bctx->next_plan.reset();

  // Remove pending queries.
  std::vector<std::shared_ptr<QueryContext>> queries;
  queries.reserve(plan.global_ids.size());
  for (auto global_id : plan.global_ids) {
    auto iter = queries_.find(global_id);
    CHECK(iter != queries_.end());
    queries.push_back(iter->second);
    mctx->queries.erase(iter->second);
    queries_.erase(iter);
  }

  // Prepare to send to backend.
  auto delegate = dispatcher_->GetBackend(backend_id);
  lock.unlock();
  if (!delegate) {
    LOG(ERROR) << "Cannot find backend delegate. backend_id=" << backend_id.t;
    return;
  }
  BatchPlanProto proto;
  EnqueueQueryCommand query;
  proto.set_plan_id(plan.plan_id.t);
  proto.set_model_session_id(plan.model_session_id);
  proto.set_exec_time_ns(
      duration_cast<nanoseconds>(plan.exec_time.time_since_epoch()).count());
  proto.set_deadline_ns(
      duration_cast<nanoseconds>(plan.earliest_deadline.time_since_epoch())
          .count());
  proto.set_expected_finish_time_ns(
      duration_cast<nanoseconds>(plan.finish_time.time_since_epoch()).count());
  for (auto& qctx : queries) {
    query.mutable_query_without_input()->Swap(
        qctx->request.mutable_query_without_input());
    query.set_rdma_read_offset(qctx->request.rdma_read_offset());
    query.set_rdma_read_length(qctx->request.rdma_read_length());
    qctx->request.Clear();
    *proto.add_queries() = std::move(query);
  }
  // Update punch clock
  auto dispatcher_dispatch_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now().time_since_epoch())
          .count();
  for (auto& query : *proto.mutable_queries()) {
    query.mutable_query_without_input()
        ->mutable_clock()
        ->set_dispatcher_dispatch_ns(dispatcher_dispatch_ns);
  }
  // Send to backend
  VLOG(1) << "WorkFinalizePlan: send to backend.";
  delegate->EnqueueBatchPlan(std::move(proto));
  VLOG(1) << "WorkFinalizePlan: done.";
}

}  // namespace delayed
}  // namespace dispatcher
}  // namespace nexus
