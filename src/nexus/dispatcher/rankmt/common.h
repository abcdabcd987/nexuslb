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
constexpr auto kCtrlPlaneLatency = std::chrono::microseconds(2000);
constexpr auto kDataPlaneLatency = std::chrono::microseconds(4000);

std::chrono::nanoseconds EstimateExecElapse(const ModelProfile& profile,
                                            uint32_t batch_size);

struct ExecutionCandidate {
  TimePoint earliest_exec_time;
  TimePoint latest_exec_time;
  TimePoint deadline;
  uint32_t batch_size;

  static ExecutionCandidate Invalid() {
    return {TimePoint::max(), TimePoint::max(), TimePoint::max(), 0};
  }
};

// RankThread -> ModelThread
struct GrantedBackendMessage {
  NodeId backend_id;
  PlanId plan_id;
  TimePoint next_available_time;
};

// ModelThread -> RankThread
struct UpdateBackendCommand {
  NodeId backend_id;
  TimePoint next_available_time;
};

using RankCommand = std::variant<UpdateBackendCommand>;

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus

#endif
