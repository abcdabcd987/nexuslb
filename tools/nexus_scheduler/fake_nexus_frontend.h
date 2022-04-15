#ifndef NEXUS_APP_FRONTEND_H_
#define NEXUS_APP_FRONTEND_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <random>
#include <unordered_map>
#include <unordered_set>

#include "nexus/common/model_def.h"
#include "nexus/common/spinlock.h"
#include "nexus/proto/nexus.pb.h"
#include "nexus_scheduler/fake_nexus_backend.h"
#include "nexus_scheduler/fake_object_accessor.h"
#include "nexus_scheduler/model_handler.h"

namespace nexus {
namespace app {

class FakeNexusFrontend {
 public:
  FakeNexusFrontend(uint32_t node_id, const FakeObjectAccessor* accessor);

  virtual ~FakeNexusFrontend();

  uint32_t node_id() const { return node_id_; }

  void Run();

  void Stop();

  void UpdateModelRoutes(const ModelRouteUpdates& request);

  std::shared_ptr<ModelHandler> LoadModel(const NexusLoadModelReply& reply);

 private:
  void Register();

  void Unregister();

  void KeepAlive();

  bool UpdateBackendPoolAndModelRoute(const ModelRouteProto& route);

  void Daemon();

  void ReportWorkload(const WorkloadStatsProto& request);

 private:
  /*! \brief Indicator whether backend is running */
  std::atomic_bool running_;
  /*! \brief Interval to update stats to scheduler in seconds */
  uint32_t beacon_interval_sec_;
  /*! \brief Frontend node ID */
  uint32_t node_id_;
  /*!
   * \brief Map from backend ID to model sessions servered at this backend.
   * Guarded by backend_sessions_mu_
   */
  std::unordered_map<uint32_t, std::unordered_set<std::string>>
      backend_sessions_;
  /*!
   * \brief Map from model session ID to model handler.
   */
  std::unordered_map<std::string, std::shared_ptr<ModelHandler>> model_pool_;

  std::thread daemon_thread_;

  std::mutex backend_sessions_mu_;
  /*! \brief Random number generator */
  std::random_device rd_;
  std::mt19937 rand_gen_;

  // Scheduler-only test related:
 public:
  enum class QueryStatus {
    kPending,
    kDropped,
    kTimeout,
    kSuccess,
  };

  struct QueryContext {
    QueryStatus status;
    int64_t frontend_recv_ns;
  };

  const QueryContext* queries(size_t model_idx) const {
    return model_ctx_.at(model_idx)->queries.get();
  }
  size_t reserved_size(size_t model_idx) const {
    return model_ctx_.at(model_idx)->reserved_size;
  }

  void ReceivedQuery(size_t model_idx, uint64_t query_id,
                     int64_t frontend_recv_ns);
  void GotDroppedReply(size_t model_idx, uint64_t query_id);
  void GotSuccessReply(size_t model_idx, uint64_t query_id, int64_t finish_ns);

 private:
  struct ModelContext {
    size_t reserved_size;
    ModelSession model_session;
    std::unique_ptr<QueryContext[]> queries;
  };
  const FakeObjectAccessor& accessor_;
  std::vector<std::unique_ptr<ModelContext>> model_ctx_;
};

}  // namespace app
}  // namespace nexus

#endif  // NEXUS_APP_APP_BASE_H_
