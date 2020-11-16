#ifndef NEXUS_DISPATCHER_BACKEND_DELEGATE_H_
#define NEXUS_DISPATCHER_BACKEND_DELEGATE_H_

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include "nexus/proto/control.grpc.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class BackendDelegate {
 public:
  BackendDelegate(uint32_t node_id, const std::string& ip,
                  const std::string& server_port, const std::string& rpc_port,
                  const std::string& gpu_device, const std::string& gpu_uuid,
                  size_t gpu_available_memory, int beacon_sec);

  uint32_t node_id() const { return node_id_; }
  std::string gpu_device() const { return gpu_device_; }
  size_t gpu_available_memory() const { return gpu_available_memory_; }

  void Tick();
  bool IsAlive();
  void SendLoadModelCommand(const ModelSession& model_session);

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
};

}  // namespace dispatcher
}  // namespace nexus

#endif
