#include "nexus/dispatcher/delayed_scheduler.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <mutex>

#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"

namespace nexus {
namespace dispatcher {
namespace delayed {

namespace {

bool OrderQueryContextByDeadlineASC(const std::unique_ptr<QueryContext>& lhs,
                                    const std::unique_ptr<QueryContext>& rhs) {
  return lhs->deadline() > rhs->deadline();
}

}  // namespace

QueryContext::QueryContext(QueryProto query_without_input, TimePoint deadline)
    : query_without_input_(std::move(query_without_input)),
      deadline_(deadline) {}

InstanceContext::InstanceContext(ModelSession model_session, NodeId backend_id,
                                 const ModelProfile& profile)
    : model_session_(std::move(model_session)),
      backend_id_(backend_id),
      profile_(profile) {
  max_batch_ = profile_.GetMaxBatchWithFullBudget(model_session_.latency_sla());
}

ModelSessionContext::ModelSessionContext(ModelSession model_session)
    : model_session_(std::move(model_session)) {
  string_id_ = ModelSessionToString(model_session_);
}

std::shared_ptr<InstanceContext> ModelSessionContext::GetInstanceContext(
    NodeId backend_id) const {
  return instances_.at(backend_id);
}

void ModelSessionContext::AddInstanceContext(
    NodeId backend_id, std::shared_ptr<InstanceContext> inst) {
  instances_[backend_id] = inst;
}

void ModelSessionContext::EnqueueQuery(std::unique_ptr<QueryContext> qctx) {
  pending_queries_.emplace_back(std::move(qctx));
  std::push_heap(pending_queries_.begin(), pending_queries_.end(),
                 OrderQueryContextByDeadlineASC);
}

BackendContext::BackendContext(NodeId backend_id)
    : backend_id_(backend_id),
      next_available_time_(std::chrono::nanoseconds(0)) {}

void DelayedScheduler::AddModelSession(ModelSession model_session) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto mctx = std::make_unique<ModelSessionContext>(std::move(model_session));
  if (models_.count(mctx->string_id())) {
    LOG(ERROR) << "Model session already exists. model_session="
               << mctx->string_id();
    return;
  }
  models_[mctx->string_id()] = std::move(mctx);
}

void DelayedScheduler::AddBackend(NodeId backend_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto bctx = std::make_unique<BackendContext>(backend_id);
  if (backends_.count(bctx->backend_id())) {
    LOG(ERROR) << "Backend already exists. backend_id=" << backend_id.t;
    return;
  }
  backends_[backend_id] = std::move(bctx);
}

void DelayedScheduler::EnqueueQuery(QueryProto query_without_input) {
  ModelSession model_session;
  ParseModelSession(query_without_input.model_session_id(), &model_session);
  auto deadline = TimePoint(
      std::chrono::nanoseconds(query_without_input.clock().frontend_recv_ns()) +
      std::chrono::microseconds(model_session.latency_sla()));
  auto qctx =
      std::make_unique<QueryContext>(std::move(query_without_input), deadline);

  std::lock_guard<std::mutex> lock(mutex_);
  auto& mctx = models_.at(qctx->proto().model_session_id());
  mctx->EnqueueQuery(std::move(qctx));
}

}  // namespace delayed
}  // namespace dispatcher
}  // namespace nexus
