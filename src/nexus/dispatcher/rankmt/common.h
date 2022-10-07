#ifndef NEXUS_DISPATCHER_RANKMT_COMMON_H_
#define NEXUS_DISPATCHER_RANKMT_COMMON_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <variant>

#include "nexus/common/model_db.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

struct RankmtConfig {
  // Dispatcher -> Backend: Batchplan
  std::chrono::duration<long, std::nano> ctrl_latency;

  // Frontend -> Backend: Image
  std::chrono::duration<long, std::nano> data_latency;

  // Backend -> Frontend: Result
  std::chrono::duration<long, std::nano> resp_latency;

  // RPS Meter
  std::chrono::duration<long, std::nano> rpsmeter_rate;
  size_t rpsmeter_window;

  static RankmtConfig Default() {
    RankmtConfig config;
    config.ctrl_latency = std::chrono::microseconds(100);
    config.data_latency = std::chrono::microseconds(50);
    config.resp_latency = std::chrono::microseconds(2000);
    config.rpsmeter_rate = std::chrono::milliseconds(50);
    config.rpsmeter_window = 100;
    return config;
  };

  static RankmtConfig FromFlags();
};

std::chrono::nanoseconds EstimateExecElapse(const ModelProfile& profile,
                                            uint32_t batch_size);

struct ExecutionCandidate {
  uint32_t batch_size;
  TimePoint exec_at;
  TimePoint invalid_after;

  static ExecutionCandidate Invalid() {
    return {0, TimePoint::max(), TimePoint::min()};
  }
};

// RankThread -> ModelThread
struct GrantedGpuMessage {
  GpuId gpu_id;
  PlanId plan_id;
  TimePoint free_at;
};

// ModelThread -> RankThread
struct UpdateGpuCommand {
  GpuId gpu_id;
  TimePoint free_at;
};

using RankCommand = std::variant<UpdateGpuCommand>;

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus

#endif
