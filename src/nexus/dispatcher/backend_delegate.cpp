#include "nexus/dispatcher/backend_delegate.h"

#include <glog/logging.h>

namespace nexus {
namespace dispatcher {

GpuDelegate::GpuDelegate(GpuInfo gpu_info, BackendDelegate* backend)
    : gpu_info_(std::move(gpu_info)), backend_(*CHECK_NOTNULL(backend)) {}

void GpuDelegate::EnqueueBatchPlan(BatchPlanProto&& request) {
  CHECK_EQ(request.gpu_idx(), gpu_info_.gpu_idx);
  backend_.EnqueueBatchPlan(std::move(request));
}

BackendDelegate::BackendDelegate(NodeId backend_id, std::vector<GpuInfo> gpus)
    : backend_id_(backend_id) {
  gpus_.reserve(gpus.size());
  for (auto& gpu : gpus) {
    gpus_.emplace_back(std::move(gpu), this);
  }
}

std::vector<GpuDelegate*> BackendDelegate::GetGpuDelegates() {
  std::vector<GpuDelegate*> ret;
  ret.reserve(gpus_.size());
  for (auto& gpu : gpus_) {
    ret.push_back(&gpu);
  }
  return ret;
}

}  // namespace dispatcher
}  // namespace nexus
