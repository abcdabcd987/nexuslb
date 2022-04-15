#ifndef NEXUS_COMMON_MODEL_HANDLER_H_
#define NEXUS_COMMON_MODEL_HANDLER_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>

#include "nexus/common/data_type.h"
#include "nexus/common/metric.h"
#include "nexus/proto/nexus.pb.h"
#include "nexus_scheduler/fake_object_accessor.h"

namespace nexus {
namespace app {

class ModelHandler {
 public:
  ModelHandler(const std::string& model_session_id,
               const FakeObjectAccessor* accessor);

  ~ModelHandler();

  ModelSession model_session() const { return model_session_; }

  std::string model_session_id() const { return model_session_id_; }

  std::shared_ptr<IntervalCounter> counter() const { return counter_; }

  void HandleReply(const QueryResultProto& result);

  void UpdateRoute(const ModelRouteProto& route);

  std::vector<uint32_t> BackendList();

  uint32_t GetBackend();

 private:
  uint32_t GetBackendDeficitRoundRobin();

  ModelSession model_session_;
  std::string model_session_id_;
  const FakeObjectAccessor& accessor_;

  std::vector<uint32_t> backends_;
  /*!
   * \brief Mapping from backend id to its serving rate,
   *
   *   Guarded by route_mu_
   */
  std::unordered_map<uint32_t, double> backend_rates_;

  std::unordered_map<uint32_t, double> backend_quanta_;
  double quantum_to_rate_ratio_ = 0;
  size_t current_drr_index_ = 0;
  float total_throughput_;
  /*! \brief Interval counter to count number of requests within each
   *  interval.
   */
  std::shared_ptr<IntervalCounter> counter_;

  std::mutex route_mu_;
  /*! \brief random number generator */
  std::random_device rd_;
  std::mt19937 rand_gen_;

  std::atomic<bool> running_;
};

}  // namespace app
}  // namespace nexus

#endif  // NEXUS_COMMON_MODEL_HANDLER_H_
