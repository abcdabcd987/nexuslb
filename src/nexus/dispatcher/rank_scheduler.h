#ifndef NEXUS_DISPATCHER_RANK_SCHEDULER_H_
#define NEXUS_DISPATCHER_RANK_SCHEDULER_H_

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "ario/epoll.h"
#include "ario/timer.h"
#include "nexus/common/metric.h"
#include "nexus/common/model_db.h"
#include "nexus/common/rps_meter.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/common/value_ranked_splay_map.h"
#include "nexus/dispatcher/accessor.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/dispatcher/batch_policy.h"
#include "nexus/dispatcher/batch_size_estimator.h"
#include "nexus/dispatcher/query_context.h"
#include "nexus/dispatcher/scheduler.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {
namespace rank {

class ModelSessionContext;

struct ExecutionCandidate {
  TimePoint latest_exec_time;
  ModelSessionContext* mctx;

  struct OrderByLatestExecTimeASC {
    bool operator()(const std::shared_ptr<ExecutionCandidate>& lhs,
                    const std::shared_ptr<ExecutionCandidate>& rhs) const {
      return lhs->latest_exec_time < rhs->latest_exec_time;
    }
  };
};

struct ActivePlan {
  explicit ActivePlan(ario::EpollExecutor& executor);

  PlanId plan_id;
  TimePoint send_time;
  TimePoint exec_time;
  TimePoint finish_time;
  TimePoint deadline;
  std::shared_ptr<ExecutionCandidate> candidate;

  ario::Timer send_timer;
};

struct InstanceContext {
  InstanceContext(ModelSession model_session, NodeId backend_id,
                  const ModelProfile& profile);

  ModelSession model_session;
  NodeId backend_id;
  const ModelProfile& profile;
  uint32_t max_batch;
};

struct ModelSessionContext {
  explicit ModelSessionContext(ModelSession model_session);
  std::chrono::nanoseconds EstimateExecElapse(uint32_t batch_size);

  ModelSession model_session;
  std::string string_id;
  std::unordered_map<NodeId, std::shared_ptr<InstanceContext>> instances;
  SortedQueryList queries;
  IncrementalBatchPolicy batch_policy;
  RpsMeter rps_meter;
  std::shared_ptr<ActivePlan> active_plan;

  // TODO: GPU performance heterogeneity
  const ModelProfile* profile = nullptr;
  uint32_t target_batch_size = 0;
};

struct BackendContext {
  BackendContext(NodeId backend_id, std::shared_ptr<BackendDelegate> delegate);

  NodeId backend_id;
  std::shared_ptr<BackendDelegate> delegate;
  std::unordered_map<std::string, std::shared_ptr<InstanceContext>> instances;
  TimePoint schedule_time;
  TimePoint next_available_time;

  ario::Timer schedule_timer;
};

class RankScheduler : public Scheduler {
 public:
  class Builder : public Scheduler::Builder {
   public:
    explicit Builder(ario::EpollExecutor& executor);
    std::unique_ptr<Scheduler> Build(
        std::unique_ptr<DispatcherAccessor> dispatcher) override;

   private:
    ario::EpollExecutor* executor_;
  };

  RankScheduler(std::unique_ptr<DispatcherAccessor> dispatcher,
                ario::EpollExecutor& executor);
  void RunAsWorker();
  void Stop();
  void AddModelSession(ModelSession model_session);
  void AddBackend(NodeId backend_id);
  CtrlStatus EnqueueQuery(DispatchRequest&& request);

 private:
  PlanId NextPlanId();
  void UpdateTargetBatchSize(ModelSessionContext* mctx,
                             const std::optional<AvgStd>& rps);
  void UpdateCandidatePool(TimePoint now, TimePoint earliest_exec_time,
                           ModelSessionContext* mctx);
  void UpdateActivePlans(TimePoint now, TimePoint earliest_exec_time,
                         ModelSessionContext* mctx, size_t num_idle_backends);
  std::shared_ptr<ExecutionCandidate> PopCandidatePool(
      TimePoint now, TimePoint earliest_exec_time, size_t rank);
  void SetupActivePlan(TimePoint now, TimePoint earliest_exec_time,
                       std::shared_ptr<ExecutionCandidate> candidate);
  void RemoveActivePlan(ModelSessionContext* mctx);
  std::vector<std::shared_ptr<BackendContext>> GetIdleBackends(
      TimePoint earliest_exec_time);
  void SendDroppedQueries(
      const std::vector<std::shared_ptr<QueryContext>>& drops);
  void OnBackendAvailableSoon(NodeId backend_id);
  void OnPlanTimer(PlanId plan_id);

  ario::EpollExecutor* executor_;

  BatchSizeEstimator bse_;
  ValueRankedSplayMap<std::string, std::shared_ptr<ExecutionCandidate>,
                      ExecutionCandidate::OrderByLatestExecTimeASC>
      candidate_pool_;

  std::mutex mutex_;
  PlanId next_plan_id_{1};
  std::unordered_map<std::string, std::shared_ptr<ModelSessionContext>> models_;
  std::unordered_map<NodeId, std::shared_ptr<BackendContext>> backends_;
  std::unordered_map<GlobalId, std::shared_ptr<QueryContext>> queries_;
  std::unordered_map<PlanId, std::shared_ptr<ActivePlan>> plans_;
};

}  // namespace rank

using RankScheduler = rank::RankScheduler;

}  // namespace dispatcher
}  // namespace nexus

#endif
