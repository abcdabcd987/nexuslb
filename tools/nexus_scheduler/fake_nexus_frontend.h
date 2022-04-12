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
#include "nexus_scheduler/model_handler.h"

namespace nexus {
namespace app {

class Frontend {
 public:
  Frontend(uint32_t node_id);

  virtual ~Frontend();

  uint32_t node_id() const { return node_id_; }

  void Run();

  void Stop();

  void UpdateModelRoutes(const ModelRouteUpdates& request);

 protected:
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
  FakeNexusBackendPool backend_pool_;
  /*!
   * \brief Map from backend ID to model sessions servered at this backend.
   * Guarded by backend_sessions_mu_
   */
  std::unordered_map<uint32_t, std::unordered_set<std::string> >
      backend_sessions_;
  /*!
   * \brief Map from model session ID to model handler.
   */
  std::unordered_map<std::string, std::shared_ptr<ModelHandler> > model_pool_;

  std::thread daemon_thread_;
  /*! \brief Mutex for connection_pool_ and user_sessions_ */
  std::mutex user_mutex_;

  std::mutex backend_sessions_mu_;
  /*! \brief Random number generator */
  std::random_device rd_;
  std::mt19937 rand_gen_;
};

}  // namespace app
}  // namespace nexus

#endif  // NEXUS_APP_APP_BASE_H_
