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
  BackendDelegateImpl(NodeId backend_id, std::string ip, uint16_t port,
                      std::vector<GpuInfo> gpus, int beacon_sec,
                      ario::RdmaQueuePair* conn, RdmaSender rdma_sender);

  void Tick() override;
  void SendLoadModelCommand(uint32_t gpu_idx, const ModelSession& model_session,
                            uint32_t max_batch,
                            ModelIndex model_index) override;
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
