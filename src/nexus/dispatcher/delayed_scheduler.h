#ifndef NEXUS_DISPATCHER_DELAYED_SCHEDULER_H_
#define NEXUS_DISPATCHER_DELAYED_SCHEDULER_H_

#include <boost/asio.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "nexus/common/metric.h"
#include "nexus/common/model_db.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/accessor.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/dispatcher/batch_size_estimator.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class DispatcherAccessor;

namespace delayed {

struct QueryContext {
  QueryContext(QueryProto query_without_input, TimePoint deadline);

  QueryProto proto;
  GlobalId global_id;
  TimePoint deadline;
};

struct OrderQueryContextByDeadlineASC {
  bool operator()(const std::shared_ptr<QueryContext>& lhs,
                  const std::shared_ptr<QueryContext>& rhs) const {
    return lhs->deadline < rhs->deadline ||
           (lhs->deadline == rhs->deadline &&
            lhs->global_id.t < rhs->global_id.t);
  }
};

using SortedQueryList =
    std::set<std::shared_ptr<QueryContext>, OrderQueryContextByDeadlineASC>;

struct BatchPlan {
  PlanId plan_id;
  NodeId backend_id;
  std::string model_session_id;

  TimePoint send_time;
  TimePoint exec_time;
  TimePoint finish_time;
  TimePoint earliest_deadline;

  uint32_t actual_batch_size;
  uint32_t reserved_batch_size;
  std::chrono::nanoseconds exec_elapse;

  // List of queries in this batch. Ordered by deadline ASC.
  // len(queries) == actual_batch_size.
  // earliest_deadline == pending_queries[query_ids[0]].deadline
  std::vector<GlobalId> global_ids;
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
  double GetRequestRate() const;

  ModelSession model_session;
  std::string string_id;
  std::unordered_map<NodeId, std::shared_ptr<InstanceContext>> instances;
  SortedQueryList queries;

  // TODO: replace the metric library used.
  std::shared_ptr<IntervalCounter> req_counter;
  mutable EWMA req_rate;

  // TODO: GPU performance heterogeneity
  const ModelProfile* profile = nullptr;
};

struct BackendContext {
  BackendContext(NodeId backend_id, std::shared_ptr<BackendDelegate> delegate);

  NodeId backend_id;
  std::shared_ptr<BackendDelegate> delegate;
  std::unordered_map<std::string, std::shared_ptr<InstanceContext>> instances;
  TimePoint next_available_time;
  std::optional<BatchPlan> next_plan;
};

class DelayedScheduler {
 public:
  explicit DelayedScheduler(DispatcherAccessor dispatcher);
  void RunAsWorker();
  void Stop();
  void AddModelSession(ModelSession model_session) /* EXCLUDES(mutex_) */;
  void AddBackend(NodeId backend_id) /* EXCLUDES(mutex_) */;
  void EnqueueQuery(QueryProto query_without_input) /* EXCLUDES(mutex_) */;

 private:
  PlanId NextPlanId() /* REQUIRES(mutex_) */;
  void WorkFullSchedule() /* EXCLUDES(mutex_) */;
  void DropTimeoutQueries() /* REQUIRES(mutex_) */;
  std::optional<BatchPlan> TryScheduleModelSessionOnBackend(
      std::shared_ptr<const BackendContext> bctx,
      std::shared_ptr<const ModelSessionContext> mctx,
      const std::set<GlobalId>& query_ids);

  DispatcherAccessor dispatcher_;
  BatchSizeEstimator bse_;

  boost::asio::io_context io_context_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      io_context_work_guard_;

  std::mutex mutex_;
  PlanId next_plan_id_{1} /* GUARDED_BY(mutex_) */;
  std::unordered_map<std::string, std::shared_ptr<ModelSessionContext>>
      models_ /* GUARDED_BY(mutex_) */;
  std::unordered_map<NodeId, std::shared_ptr<BackendContext>>
      backends_ /* GUARDED_BY(mutex_) */;
  std::unordered_map<GlobalId, std::shared_ptr<QueryContext>>
      queries_ /* GUARDED_BY(mutex_) */;
};

}  // namespace delayed

using DelayedScheduler = delayed::DelayedScheduler;

}  // namespace dispatcher
}  // namespace nexus

#endif
