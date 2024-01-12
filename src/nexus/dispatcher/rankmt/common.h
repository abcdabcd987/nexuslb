#ifndef NEXUS_DISPATCHER_RANKMT_COMMON_H_
#define NEXUS_DISPATCHER_RANKMT_COMMON_H_

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>

#include "nexus/common/model_db.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/batch_policy.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

// Condition for a Candidate to become schedulable.
class SchedulableCondition {
 public:
  enum Value {
    // As soon as the Candidate is created.
    kImmediately = 1,
    // As soon as the Candidate reaches the target batch size.
    kTargetBatchSize = 2,
    // As soon as the Candidate has waited the target queuing delay.
    kTargetQueuingDelay = 3,
    // As soon as the current time reaches the Candidate's frontrun_at.
    kFrontrun = 4,
    // Latest possible
    kLatest = 5,
    // Comparing the current batch size with the product of target batch size
    // and request rate.
    kBetaLambda = 6,
  };

  constexpr SchedulableCondition() : value_(kFrontrun) {}
  constexpr SchedulableCondition(Value value) : value_(value) {}
  constexpr operator Value() const { return value_; }
  constexpr const char* ToString() const;
  static constexpr const char* ToString(SchedulableCondition c);
  static constexpr std::optional<SchedulableCondition> Parse(
      std::string_view s);

 private:
  Value value_;
};

// Priority of a candidate when a GPU becomes free.
class CandidatePriority {
 public:
  enum Value {
    // Disabled
    kDisabled = 1,
    // Earliest (deadline - exec_elapse)
    kInvalidAfter = 2,
    // Pick the one with earliest deadline.
    kDeadline = 3,
    // Slack := exec_at - sched_at. Note: this one does not make sense.
    kSlack = 4,
    // Efficiency := (current_bs/l(current_bs)) / (target_bs/l(target_bs))
    kEfficiency = 5,
  };

  constexpr CandidatePriority() : value_(kDisabled) {}
  constexpr CandidatePriority(Value value) : value_(value) {}
  constexpr operator Value() const { return value_; }
  constexpr const char* ToString() const;
  static constexpr const char* ToString(CandidatePriority c);
  static constexpr std::optional<CandidatePriority> Parse(std::string_view s);

 private:
  Value value_;
};

// Allowed Model-GPU matchmaking methods.
class MatchMethods {
 public:
  enum Value {
    // Model finding GPU
    kPrimaryOnly = 1,
    // GPU finding model
    kSecondaryOnly = 2,
    // First model finding GPU, then GPU finding model.
    kBoth = 3,
  };

  constexpr MatchMethods() : value_(kSecondaryOnly) {}
  constexpr MatchMethods(Value value) : value_(value) {}
  constexpr operator Value() const { return value_; }
  constexpr const char* ToString() const;
  static constexpr const char* ToString(MatchMethods c);
  static constexpr std::optional<MatchMethods> Parse(std::string_view s);

  bool IsPrimaryEnabled() const {
    return value_ == kPrimaryOnly || value_ == kBoth;
  }

  bool IsSecondaryEnabled() const {
    return value_ == kSecondaryOnly || value_ == kBoth;
  }

 private:
  Value value_;
};

struct RankmtConfig {
  SchedulableCondition schedulable;
  DropPolicy drop;
  CandidatePriority priority;
  MatchMethods match;

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
  long priority;

  static ExecutionCandidate Invalid() {
    return {0, TimePoint::max(), TimePoint::min(), 0};
  }
};

enum class ExecutionGrantedBy {
  kModel = 1,
  kGpu = 2,
};

// RankThread -> ModelThread
struct GrantedGpuMessage {
  GpuId gpu_id;
  PlanId plan_id;
  TimePoint free_at;

  // Stats
  ExecutionGrantedBy match_method;
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
