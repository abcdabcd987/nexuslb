#ifndef NEXUS_DISPATCHER_DELAYED_SCHEDULER_H_
#define NEXUS_DISPATCHER_DELAYED_SCHEDULER_H_

#include <boost/asio.hpp>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "nexus/common/model_db.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {
namespace delayed {

class QueryContext {
 public:
  QueryContext(QueryProto query_without_input, TimePoint deadline);

  QueryProto& proto() { return query_without_input_; }
  TimePoint deadline() { return deadline_; }

 private:
  QueryProto query_without_input_;
  TimePoint deadline_;
};

class InstanceContext {
 public:
  InstanceContext(ModelSession model_session, NodeId backend_id,
                  const ModelProfile& profile);

  const ModelSession& model_session() const { return model_session_; }
  NodeId backend_id() const { return backend_id_; }
  const ModelProfile& profile() const { return profile_; }
  uint32_t max_batch() const { return max_batch_; }

 private:
  ModelSession model_session_;
  NodeId backend_id_;
  const ModelProfile& profile_;
  uint32_t max_batch_;
};

class ModelSessionContext {
 public:
  explicit ModelSessionContext(ModelSession model_session);

  const ModelSession& model_session() const { return model_session_; }
  const std::string& string_id() const { return string_id_; }

  std::shared_ptr<InstanceContext> GetInstanceContext(NodeId backend_id) const;
  void AddInstanceContext(NodeId backend_id,
                          std::shared_ptr<InstanceContext> inst);
  void EnqueueQuery(std::unique_ptr<QueryContext> qctx);

 private:
  ModelSession model_session_;
  std::string string_id_;
  std::unordered_map<NodeId, std::shared_ptr<InstanceContext>> instances_;
  std::vector<std::unique_ptr<QueryContext>> pending_queries_; // ORDER BY deadline ASC
};

class BackendContext {
 public:
  explicit BackendContext(NodeId backend_id);

  NodeId backend_id() const { return backend_id_; }

 private:
  NodeId backend_id_;
  std::unordered_map<std::string, std::shared_ptr<InstanceContext>> instances_;
  TimePoint next_available_time_;
};

class DelayedScheduler {
 public:
  void AddModelSession(ModelSession model_session);
  void AddBackend(NodeId backend_id);
  void EnqueueQuery(QueryProto query_without_input);

 private:
  boost::asio::io_context io_context_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      io_context_work_guard_;

  std::mutex mutex_;
  std::unordered_map<std::string, std::unique_ptr<ModelSessionContext>> models_;
  std::unordered_map<NodeId, std::unique_ptr<BackendContext>> backends_;
};

}  // namespace delayed

using DelayedScheduler = delayed::DelayedScheduler;

}  // namespace dispatcher
}  // namespace nexus

#endif
