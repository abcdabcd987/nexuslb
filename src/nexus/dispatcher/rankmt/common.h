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

constexpr size_t kRpsMeterHistoryLength = 32;

struct RankmtConfig {
  std::chrono::duration<long, std::nano> ctrl_latency;
  std::chrono::duration<long, std::nano> data_latency;

  static RankmtConfig Default() {
    RankmtConfig config;
    config.ctrl_latency = std::chrono::microseconds(2000);
    config.data_latency = std::chrono::microseconds(5000);
    return config;
  };
};

std::chrono::nanoseconds EstimateExecElapse(const ModelProfile& profile,
                                            uint32_t batch_size);

struct ExecutionCandidate {
  TimePoint exec_at;

  static ExecutionCandidate Invalid() { return {TimePoint::max()}; }
};

// RankThread -> ModelThread
struct GrantedGpuMessage {
  GpuId gpu_id;
  PlanId plan_id;
  TimePoint _debug_free_at;
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
