#include "nexus/dispatcher/round_robin_scheduler.h"

#include <glog/logging.h>

#include <algorithm>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
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
namespace rr {

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

ModelSessionContext::ModelSessionContext(ModelSession model_session)
    : model_session(std::move(model_session)) {
  string_id = ModelSessionToString(this->model_session);
}

BackendContext::BackendContext(NodeId backend_id,
                               std::shared_ptr<BackendDelegate> delegate,
                               boost::asio::io_context* io_context)
    : backend_id(backend_id),
      delegate(std::move(delegate)),
      send_time(std::chrono::nanoseconds(0)),
      send_timer(*io_context) {}

RoundRobinScheduler::Builder::Builder(YAML::Node static_config)
    : static_config_(std::move(static_config)) {}

std::unique_ptr<Scheduler> RoundRobinScheduler::Builder::Build(
    std::unique_ptr<DispatcherAccessor> dispatcher) {
  return std::make_unique<RoundRobinScheduler>(std::move(dispatcher),
                                               std::move(static_config_));
}

RoundRobinScheduler::RoundRobinScheduler(
    std::unique_ptr<DispatcherAccessor> dispatcher, YAML::Node static_config)
    : Scheduler(std::move(dispatcher)),
      static_config_(std::move(static_config)),
      io_context_work_guard_(io_context_.get_executor()) {
  CHECK(static_config_.IsMap())
      << "RoundRobinScheduler expects the static config to be a map in the "
         "following format: `model_name: num_backends`. For example, "
         "`{resnet_0: 10, googlenet: 3}`";
}

void RoundRobinScheduler::RunAsWorker() { io_context_.run(); }

void RoundRobinScheduler::Stop() { io_context_work_guard_.reset(); }

void RoundRobinScheduler::AddModelSession(ModelSession model_session) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Add model session
  auto mctx = std::make_shared<ModelSessionContext>(std::move(model_session));
  if (models_.count(mctx->string_id)) {
    LOG(ERROR) << "Model session already exists. model_session="
               << mctx->string_id;
    return;
  }
  models_[mctx->string_id] = mctx;

  // Read the static config
  const auto& name = mctx->model_session.model_name();
  auto model_config = static_config_[name];
  if (!model_config) {
    LOG(FATAL) << "Could not find static config for model \"" << name << "\"";
  }
  CHECK(model_config.IsScalar())
      << "Static config for model \"" << name
      << "\" should be the number of backends. Got: " << model_config;
  auto num_backends = model_config.as<size_t>();
  CHECK_NE(num_backends, 0)
      << "Number of backends should be greater than 0. Model: " << name;

  // Add instances
  auto profile_id = ModelSessionToProfileID(mctx->model_session);
  size_t added_backends = 0;
  auto now = Clock::now();
  for (auto& pair : backends_) {
    auto& bctx = pair.second;
    if (bctx->model) {
      // Skip if the backend has been assigned to another model session.
      continue;
    }

    const auto* profile = ModelDatabase::Singleton().GetModelProfile(
        bctx->delegate->gpu_device(), bctx->delegate->gpu_uuid(), profile_id);
    CHECK_NE(profile, nullptr);
    // Workaround: use the first backend's profile as model session profile.
    // TODO: GPU performance heterogeneity.
    if (!mctx->profile) {
      mctx->profile = profile;
      // l(b) * (1+1/n) < SLO
      double budget =
          mctx->model_session.latency_sla() / (1 + 1. / num_backends);
      mctx->max_batch = profile->GetMaxBatchWithFullBudget(budget);
      LOG(INFO) << "Adding model session " << mctx->string_id
                << ", budget: " << budget
                << " ms, max_batch: " << mctx->max_batch;
    }

    bctx->model = mctx.get();
    mctx->backends[bctx->backend_id] = bctx.get();

    // Setup the staggered execution
    auto offset_ns = static_cast<int64_t>(mctx->model_session.latency_sla() *
                                          1e6 * added_backends / num_backends);
    bctx->send_time = now - std::chrono::nanoseconds(offset_ns);
    SetupBackendTimer(bctx.get());

    ++added_backends;
    if (added_backends == num_backends) {
      break;
    }
  }

  // Check number of backends
  CHECK_EQ(added_backends, num_backends)
      << "Not enough backends for model \"" << name << "\"";
}

void RoundRobinScheduler::AddBackend(NodeId backend_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!models_.empty()) {
    LOG(FATAL) << "RoundRobinScheduler requires all backends to be added "
                  "before any model session is loaded.";
  }

  // Add backend
  auto delegate = dispatcher_->GetBackend(backend_id);
  if (!delegate) {
    LOG(ERROR) << "Cannot find backend delegate. backend_id=" << backend_id.t;
    return;
  }
  if (backends_.count(backend_id)) {
    LOG(ERROR) << "Backend already exists. backend_id=" << backend_id.t;
    return;
  }
  auto bctx =
      std::make_shared<BackendContext>(backend_id, delegate, &io_context_);
  backends_[backend_id] = std::move(bctx);
}

CtrlStatus RoundRobinScheduler::EnqueueQuery(DispatchRequest&& request) {
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

  return CtrlStatus::CTRL_OK;
}

PlanId RoundRobinScheduler::NextPlanId() { return PlanId(next_plan_id_.t++); }

void RoundRobinScheduler::SetupBackendTimer(BackendContext* bctx) {
  bctx->send_timer.expires_at(bctx->send_time);
  bctx->send_timer.async_wait([backend_id = bctx->backend_id,
                               this](const boost::system::error_code& ec) {
    if (ec) return;
    GatherAndSendPlan(backend_id);
  });
}

std::tuple<RoundRobinScheduler::QueryList, RoundRobinScheduler::QueryList,
           std::chrono::nanoseconds>
RoundRobinScheduler::GatherBatch(ModelSessionContext* mctx,
                                 TimePoint exec_time) {
  using namespace std::chrono;
  std::vector<std::shared_ptr<QueryContext>> dropped, inputs;

  auto proc_elapse = nanoseconds(0);
  // TODO: WithNStd
  proc_elapse += nanoseconds(
      static_cast<long>(mctx->profile->GetPreprocessLatency() * 1e3));
  proc_elapse += nanoseconds(
      static_cast<long>(mctx->profile->GetPostprocessLatency() * 1e3));

  // Sliding window
  auto& Q = mctx->queries;
  auto qiter = Q.begin();
  uint32_t bs = std::min(static_cast<uint32_t>(Q.size()), mctx->max_batch);
  size_t cnt_remain = Q.size();
  while (inputs.size() < bs && qiter != Q.end()) {
    --cnt_remain;
    auto& qctx = *qiter;
    if (qctx->deadline < exec_time) {
      // Drop only when it's too late.
      dropped.push_back(qctx);
      qiter = Q.erase(qiter);
      continue;
    }

    auto deadline = inputs.empty() ? qctx->deadline : inputs[0]->deadline;
    auto fwd_elapse = nanoseconds(
        static_cast<long>(mctx->profile->GetForwardLatency(bs) * 1e3));
    auto finish_time = exec_time + fwd_elapse + proc_elapse;
    if (deadline > finish_time) {
      inputs.push_back(qctx);
      qiter = Q.erase(qiter);
    } else {
      // Don't drop yet.
      // Just in case the request can be satisfy later.
      bs = std::min(static_cast<uint32_t>(inputs.size() + cnt_remain),
                    mctx->max_batch);
      ++qiter;
    }
  }

  nanoseconds exec_elapse;
  if (!inputs.empty()) {
    auto fwd_elapse = nanoseconds(static_cast<long>(
        mctx->profile->GetForwardLatency(inputs.size()) * 1e3));
    exec_elapse = fwd_elapse + proc_elapse;
  } else {
    exec_elapse = nanoseconds(0);
  }

  return std::tie(dropped, inputs, exec_elapse);
}

void RoundRobinScheduler::ReplyDroppedQueries(
    const std::vector<std::shared_ptr<QueryContext>>& dropped) {
  // Send replies about dropped queries
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
}

void RoundRobinScheduler::GatherAndSendPlan(NodeId backend_id) {
  using namespace std::chrono;
  auto now = Clock::now();
  std::unique_lock<std::mutex> lock(mutex_);
  if (!backends_.count(backend_id)) {
    LOG(ERROR) << "GatherAndSendPlan: Cannot find backend. backend_id="
               << backend_id.t;
    return;
  }
  auto* bctx = backends_.at(backend_id).get();
  auto* mctx = bctx->model;
  if (duration_cast<microseconds>(now - bctx->send_time) > microseconds(100)) {
    LOG(WARNING) << "GatherAndSendPlan: Huge timer offset. bctx.send_time="
                 << bctx->send_time.time_since_epoch().count()
                 << ", now=" << now.time_since_epoch().count()
                 << ", diff=" << ((now - bctx->send_time).count() / 1e3)
                 << "us";
  }

  // Gather dropped requests and the batch inputs
  auto exec_time = now + microseconds(kCtrlPlaneLatencyUs) +
                   microseconds(kDataPlaneLatencyUs);
  std::vector<std::shared_ptr<QueryContext>> dropped, inputs;
  nanoseconds exec_elapse;
  std::tie(dropped, inputs, exec_elapse) = GatherBatch(mctx, exec_time);
  if (!dropped.empty()) {
    VLOG(1) << "Drop " << dropped.size() << " queries.";

    // Tell frontends that the requests are dropped.
    ReplyDroppedQueries(dropped);
  }

  // Setup next timer
  bctx->send_time += milliseconds(mctx->model_session.latency_sla());
  SetupBackendTimer(bctx);

  // Skip if nothing to run.
  if (inputs.empty()) {
    return;
  }

  // Create a batch plan.
  lock.unlock();
  EnqueueQueryCommand query;
  BatchPlanProto proto;
  proto.set_plan_id(NextPlanId());
  proto.set_model_session_id(mctx->string_id);
  proto.set_exec_time_ns(
      duration_cast<nanoseconds>(exec_time.time_since_epoch()).count());
  proto.set_deadline_ns(
      duration_cast<nanoseconds>(inputs[0]->deadline.time_since_epoch())
          .count());
  proto.set_expected_finish_time_ns(
      duration_cast<nanoseconds>((exec_time + exec_elapse).time_since_epoch())
          .count());
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
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now().time_since_epoch())
          .count();
  for (auto& query : *proto.mutable_queries()) {
    query.mutable_query_without_input()
        ->mutable_clock()
        ->set_dispatcher_dispatch_ns(dispatcher_dispatch_ns);
  }
  // Send to backend
  VLOG(1) << "GatherAndSendPlan: send to backend.";
  bctx->delegate->EnqueueBatchPlan(std::move(proto));
  VLOG(1) << "GatherAndSendPlan: done.";
}

}  // namespace rr
}  // namespace dispatcher
}  // namespace nexus
