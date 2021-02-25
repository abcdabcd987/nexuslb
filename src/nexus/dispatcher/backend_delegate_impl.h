#ifndef NEXUS_DISPATCHER_BACKEND_DELEGATE_IMPL_H_
#define NEXUS_DISPATCHER_BACKEND_DELEGATE_IMPL_H_

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <unordered_map>

#include "ario/ario.h"
#include "nexus/common/rdma_sender.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class BackendDelegateImpl : public BackendDelegate {
 public:
  BackendDelegateImpl(uint32_t node_id, std::string ip, uint16_t port,
                      std::string gpu_device, std::string gpu_uuid,
                      size_t gpu_available_memory, int beacon_sec,
                      ario::RdmaQueuePair* conn, RdmaSender rdma_sender);

  void Tick() override;
  void SendLoadModelCommand(const ModelSession& model_session,
                            uint32_t max_batch) override;
  void EnqueueBatchPlan(BatchPlanProto&& request) override;

 private:
  std::string ip_;
  uint16_t port_;
  int beacon_sec_;
  long timeout_ms_;
  ario::RdmaQueuePair* conn_;
  RdmaSender rdma_sender_;
  std::chrono::time_point<std::chrono::system_clock> last_time_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
