#ifndef NEXUS_DISPATCHER_BACKEND_DELEGATE_H_
#define NEXUS_DISPATCHER_BACKEND_DELEGATE_H_

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <unordered_map>

#include "ario/ario.h"
#include "nexus/common/rdma_sender.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class BackendDelegate {
 public:
  BackendDelegate(uint32_t node_id, std::string ip, uint16_t port,
                  std::string gpu_device, std::string gpu_uuid,
                  size_t gpu_available_memory, int beacon_sec,
                  ario::RdmaQueuePair* conn, RdmaSender rdma_sender);

  uint32_t node_id() const { return node_id_; }
  const std::string& gpu_device() const { return gpu_device_; }
  const std::string& gpu_uuid() const { return gpu_uuid_; }
  size_t gpu_available_memory() const { return gpu_available_memory_; }
  const BackendInfo& backend_info() const { return backend_info_; }

  void Tick();
  void SendLoadModelCommand(const ModelSession& model_session,
                            uint32_t max_batch);
  void EnqueueBatchPlan(BatchPlanProto&& request);

 private:
  uint32_t node_id_;
  std::string ip_;
  uint16_t port_;
  std::string gpu_device_;
  std::string gpu_uuid_;
  size_t gpu_available_memory_;
  int beacon_sec_;
  long timeout_ms_;
  ario::RdmaQueuePair* conn_;
  RdmaSender rdma_sender_;
  std::chrono::time_point<std::chrono::system_clock> last_time_;
  BackendInfo backend_info_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
