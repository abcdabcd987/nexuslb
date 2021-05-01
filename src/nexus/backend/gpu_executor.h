#ifndef NEXUS_BACKEND_BASE_GPU_EXECUTOR_H_
#define NEXUS_BACKEND_BASE_GPU_EXECUTOR_H_

#include <atomic>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ario/ario.h"
#include "nexus/backend/batch_plan_context.h"
#include "nexus/backend/model_exec.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace backend {

class GpuExecutorPlanFollower {
 public:
  GpuExecutorPlanFollower(int gpu_id, ario::PollerType poller_type);
  virtual ~GpuExecutorPlanFollower();
  void Start(int core = -1);
  void Stop();
  void AddModel(std::shared_ptr<ModelExecutor> model);
  void RemoveModel(std::shared_ptr<ModelExecutor> model);
  void AddBatchPlan(std::shared_ptr<BatchPlanContext> plan);

 private:
  void UpdateTimer() /* REQUIRES(mutex_) */;
  void OnTimer(ario::ErrorCode error);

  int gpu_id_;
  std::thread thread_;

  ario::EpollExecutor executor_;

  std::atomic_flag is_executing_ = ATOMIC_FLAG_INIT;
  std::mutex mutex_;
  std::vector<std::shared_ptr<BatchPlanContext>>
      plans_ /* GUARDED_BY(mutex_) */;
  std::vector<std::shared_ptr<ModelExecutor>> models_ /* GUARDED_BY(mutex_) */;
  ario::Timer next_timer_ /* GUARDED_BY(mutex_) */;
};

}  // namespace backend
}  // namespace nexus

#endif  // NEXUS_BACKEND_BASE_GPU_EXECUTOR_H_
