#pragma once

#include "nexus/dispatcher/backend_delegate.h"

namespace nexus {
namespace dispatcher {

class FakeBackendDelegate : public BackendDelegate {
 public:
  FakeBackendDelegate(uint32_t node_id, std::string gpu_device,
                      std::string gpu_uuid, size_t gpu_available_memory);

  void Tick() override;
  void SendLoadModelCommand(const ModelSession& model_session,
                            uint32_t max_batch) override;
  void EnqueueBatchPlan(BatchPlanProto&& request) override;

 private:
};

}  // namespace dispatcher
}  // namespace nexus
