#ifndef NEXUS_SCHEDULER_FRONTEND_DELEGATE_H_
#define NEXUS_SCHEDULER_FRONTEND_DELEGATE_H_

#include <chrono>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "nexus/proto/nexus.pb.h"

namespace nexus {
namespace scheduler {

class Scheduler;

class FrontendDelegate {
 public:
  explicit FrontendDelegate(uint32_t node_id);

  uint32_t node_id() const { return node_id_; }

  void SubscribeModel(const std::string& model_session_id);

  const std::unordered_set<std::string>& subscribe_models() const {
    return subscribe_models_;
  }

  void UpdateModelRoutesRpc(const ModelRouteUpdates& request);

 private:
  uint32_t node_id_;
  std::unordered_set<std::string> subscribe_models_;
};

}  // namespace scheduler
}  // namespace nexus

#endif  // NEXUS_SCHEDULER_FRONTEND_DELEGATE_H_
