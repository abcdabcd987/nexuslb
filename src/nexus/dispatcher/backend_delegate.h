#ifndef NEXUS_DISPATCHER_BACKEND_DELEGATE_H_
#define NEXUS_DISPATCHER_BACKEND_DELEGATE_H_

#include <cstdint>
#include <cstdlib>
#include <string>

#include "nexus/common/typedef.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

struct GpuInfo {
  GpuId gpu_id;      // internal ID assigned by Dispatcher
  uint32_t gpu_idx;  // gpu_idx inside the backend
  std::string gpu_device;
  std::string gpu_uuid;
  size_t gpu_available_memory;
};

class GpuDelegate;
class BackendDelegate {
 public:
  BackendDelegate(NodeId backend_id, std::vector<GpuInfo> gpus);
  NodeId node_id() const { return backend_id_; }
  const BackendInfo& backend_info() const { return backend_info_; }
  std::vector<GpuDelegate*> GetGpuDelegates();

  virtual ~BackendDelegate() = default;
  virtual void Tick() = 0;
  virtual void SendLoadModelCommand(uint32_t gpu_idx,
                                    const ModelSession& model_session,
                                    uint32_t max_batch,
                                    ModelIndex model_index) = 0;
  virtual void EnqueueBatchPlan(BatchPlanProto&& request) = 0;

 protected:
  NodeId backend_id_;
  std::vector<GpuDelegate> gpus_;
  BackendInfo backend_info_;
};

class GpuDelegate {
 public:
  GpuDelegate(GpuInfo gpu_info, BackendDelegate* backend);
  void EnqueueBatchPlan(BatchPlanProto&& request);
  GpuId gpu_id() const { return gpu_info_.gpu_id; };
  uint32_t gpu_idx() const { return gpu_info_.gpu_idx; };
  const std::string& gpu_device() const { return gpu_info_.gpu_device; };
  const std::string& gpu_uuid() const { return gpu_info_.gpu_uuid; };
  NodeId backend_id() const { return backend_.node_id(); }

 private:
  GpuInfo gpu_info_;
  BackendDelegate& backend_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
