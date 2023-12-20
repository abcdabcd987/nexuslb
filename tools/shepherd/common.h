#pragma once
#include <chrono>
#include <vector>

#include "nexus/common/time_util.h"

namespace nexus::shepherd {

struct Query {
  int query_id;
  int model_id;
  TimePoint arrival_at;
};

struct BatchPlan {
  int model_id;
  std::vector<int> query_ids;
  TimePoint exec_at;
  TimePoint finish_at;

  long exec_time_ns() const { return exec_at.time_since_epoch().count(); }
  long expected_finish_time_ns() const {
    return finish_at.time_since_epoch().count();
  }
};

enum class Preemption { kNo, kYes };

struct ShepherdConfig {
  std::chrono::duration<long, std::nano> ctrl_latency;
  std::chrono::duration<long, std::nano> data_latency;
  float preempt_lambda;

  static ShepherdConfig Default() {
    ShepherdConfig config;
    config.ctrl_latency = std::chrono::microseconds(25);
    config.data_latency = std::chrono::microseconds(75);
    config.preempt_lambda = 3.03;
    return config;
  }

  static ShepherdConfig FromFlags();
};

class BackendStub {
 public:
  virtual void RunBatch(BatchPlan plan, Preemption preempt) = 0;
};

class FrontendStub {
 public:
  virtual void MarkQueryDropped(int query_id) = 0;
};

}  // namespace nexus::shepherd
