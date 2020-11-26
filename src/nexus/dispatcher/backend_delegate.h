#ifndef NEXUS_DISPATCHER_BACKEND_DELEGATE_H_
#define NEXUS_DISPATCHER_BACKEND_DELEGATE_H_

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <unordered_map>

#include "nexus/proto/control.grpc.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class InstanceInfo;

class BackendDelegate {
 public:
  BackendDelegate(uint32_t node_id, const std::string& ip,
                  const std::string& server_port, const std::string& rpc_port,
                  const std::string& gpu_device, const std::string& gpu_uuid,
                  size_t gpu_available_memory, int beacon_sec);

  uint32_t node_id() const { return node_id_; }
  const std::string& gpu_device() const { return gpu_device_; }
  const std::string& gpu_uuid() const { return gpu_uuid_; }
  size_t gpu_available_memory() const { return gpu_available_memory_; }
  const BackendInfo& backend_info() const { return backend_info_; }

  std::shared_ptr<InstanceInfo> GetInstanceInfo(
      const std::string& model_sess_id) const;
  void AddInstanceInfo(const std::string& model_sess_id,
                       std::shared_ptr<InstanceInfo> inst);

  void Tick();
  bool IsAlive();
  void SendLoadModelCommand(const ModelSession& model_session,
                            uint32_t max_batch);
  void EnqueueBatchPlan(const BatchPlanProto& req);

 private:
  uint32_t node_id_;
  std::string ip_;
  std::string server_port_;
  std::string rpc_port_;
  std::string gpu_device_;
  std::string gpu_uuid_;
  size_t gpu_available_memory_;
  int beacon_sec_;
  long timeout_ms_;
  std::unique_ptr<BackendCtrl::Stub> stub_;
  std::chrono::time_point<std::chrono::system_clock> last_time_;
  std::unordered_map<std::string, std::shared_ptr<InstanceInfo>> instances_;
  BackendInfo backend_info_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
