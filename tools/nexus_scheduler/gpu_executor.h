#ifndef NEXUS_BACKEND_BASE_GPU_EXECUTOR_H_
#define NEXUS_BACKEND_BASE_GPU_EXECUTOR_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "nexus_scheduler/model_exec.h"

namespace nexus {
namespace backend {

class GpuExecutorMultiBatching {
 public:
  GpuExecutorMultiBatching(int gpu_id);

  int gpu_id() { return gpu_id_; }

  void SetDutyCycle(double duty_cycle_us) {
    duty_cycle_us_.store(duty_cycle_us);
  }

  void Start(int core = -1);

  void Stop();

  void AddModel(std::shared_ptr<ModelExecutor> model);

  void RemoveModel(std::shared_ptr<ModelExecutor> model);

 private:
  void Run();

  int gpu_id_;
  std::atomic<double> duty_cycle_us_;
  std::atomic_bool running_;
  std::thread thread_;
  std::vector<std::shared_ptr<ModelExecutor> > models_;
  std::mutex models_mu_;
  std::mutex util_mu_;
};

}  // namespace backend
}  // namespace nexus

#endif  // NEXUS_BACKEND_BASE_GPU_EXECUTOR_H_
