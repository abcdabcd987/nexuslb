#ifndef NEXUS_DISPATCHER_DELAYED_SCHEDULER_H_
#define NEXUS_DISPATCHER_DELAYED_SCHEDULER_H_

#include <boost/asio.hpp>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "nexus/common/metric.h"
#include "nexus/common/model_db.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/accessor.h"
#include "nexus/dispatcher/backend_delegate.h"
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
  double GetRequestRate();

  ModelSession model_session;
  std::string string_id;
  std::unordered_map<NodeId, std::shared_ptr<InstanceContext>> instances;
  // ORDER BY deadline ASC
  std::vector<std::shared_ptr<QueryContext>> sorted_queries;

  // TODO: replace the metric library used.
  std::shared_ptr<IntervalCounter> req_counter;
  EWMA req_rate;

  // TODO: GPU performance heterogeneity
  const ModelProfile* profile = nullptr;
};

struct BackendContext {
  BackendContext(NodeId backend_id, std::shared_ptr<BackendDelegate> delegate);

  NodeId backend_id;
  std::shared_ptr<BackendDelegate> delegate;
  std::unordered_map<std::string, std::shared_ptr<InstanceContext>> instances;
  TimePoint next_available_time;
};

class DelayedScheduler {
 public:
  explicit DelayedScheduler(DispatcherAccessor dispatcher);
  void RunAsWorker();
  void Stop();
  void AddModelSession(ModelSession model_session);
  void AddBackend(NodeId backend_id);
  void EnqueueQuery(QueryProto query_without_input);

 private:
  void WorkFullSchedule();

  DispatcherAccessor dispatcher_;

  boost::asio::io_context io_context_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      io_context_work_guard_;

  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<ModelSessionContext>> models_;
  std::unordered_map<NodeId, std::shared_ptr<BackendContext>> backends_;
  std::unordered_map<GlobalId, std::shared_ptr<QueryContext>> queries_;
};

}  // namespace delayed

using DelayedScheduler = delayed::DelayedScheduler;

}  // namespace dispatcher
}  // namespace nexus

#endif
