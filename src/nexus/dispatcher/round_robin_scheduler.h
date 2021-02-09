#ifndef NEXUS_DISPATCHER_STATIC_SCHEDULER_H_
#define NEXUS_DISPATCHER_STATIC_SCHEDULER_H_

#include <yaml-cpp/yaml.h>

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
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class DispatcherAccessor;

namespace rr {

struct QueryContext {
  QueryContext(DispatchRequest request, TimePoint deadline);

  DispatchRequest request;
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

class BackendContext;

struct ModelSessionContext {
  explicit ModelSessionContext(ModelSession model_session);

  ModelSession model_session;
  std::string string_id;
  std::unordered_map<NodeId, BackendContext*> backends;
  SortedQueryList queries;
  const ModelProfile* profile = nullptr;
  uint32_t max_batch = 0;
};

struct BackendContext {
  BackendContext(NodeId backend_id, std::shared_ptr<BackendDelegate> delegate,
                 boost::asio::io_context* io_context);

  NodeId backend_id;
  std::shared_ptr<BackendDelegate> delegate;
  ModelSessionContext* model = nullptr;
  TimePoint send_time;
  boost::asio::basic_waitable_timer<Clock> send_timer;
};

class RoundRobinScheduler {
 public:
  RoundRobinScheduler(DispatcherAccessor dispatcher, YAML::Node static_config);
  void RunAsWorker();
  void Stop();
  void AddModelSession(ModelSession model_session) /* EXCLUDES(mutex_) */;
  void AddBackend(NodeId backend_id) /* EXCLUDES(mutex_) */;
  CtrlStatus EnqueueQuery(DispatchRequest&& request) /* EXCLUDES(mutex_) */;

 private:
  using QueryList = std::vector<std::shared_ptr<QueryContext>>;
  PlanId NextPlanId() /* REQUIRES(mutex_) */;
  void SetupBackendTimer(BackendContext* bctx);
  std::tuple<QueryList, QueryList, std::chrono::nanoseconds> GatherBatch(
      ModelSessionContext* mctx, TimePoint exec_time) /* REQUIRES(mutex_) */;
  void ReplyDroppedQueries(const std::vector<std::shared_ptr<QueryContext>>&
                               dropped) /* REQUIRES(mutex_) */;
  void GatherAndSendPlan(NodeId backend_id) /* EXCLUDES(mutex_) */;

  DispatcherAccessor dispatcher_;
  YAML::Node static_config_;

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

}  // namespace rr

using RoundRobinScheduler = rr::RoundRobinScheduler;

}  // namespace dispatcher
}  // namespace nexus

#endif
