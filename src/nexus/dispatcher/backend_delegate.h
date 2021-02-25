#ifndef NEXUS_DISPATCHER_BACKEND_DELEGATE_H_
#define NEXUS_DISPATCHER_BACKEND_DELEGATE_H_

#include <cstdint>
#include <cstdlib>
#include <string>

#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class BackendDelegate {
 public:
  BackendDelegate(uint32_t node_id, std::string gpu_device,
                  std::string gpu_uuid, size_t gpu_available_memory)
      : node_id_(node_id),
        gpu_device_(std::move(gpu_device)),
        gpu_uuid_(std::move(gpu_uuid)),
        gpu_available_memory_(gpu_available_memory) {}
  uint32_t node_id() const { return node_id_; }
  const std::string& gpu_device() const { return gpu_device_; }
  const std::string& gpu_uuid() const { return gpu_uuid_; }
  size_t gpu_available_memory() const { return gpu_available_memory_; }
  const BackendInfo& backend_info() const { return backend_info_; }

  virtual ~BackendDelegate() = default;
  virtual void Tick() = 0;
  virtual void SendLoadModelCommand(const ModelSession& model_session,
                                    uint32_t max_batch) = 0;
  virtual void EnqueueBatchPlan(BatchPlanProto&& request) = 0;

 protected:
  uint32_t node_id_;
  std::string gpu_device_;
  std::string gpu_uuid_;
  size_t gpu_available_memory_;
  BackendInfo backend_info_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
