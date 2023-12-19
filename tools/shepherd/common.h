#pragma once
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
};

enum class Preemption { kNo, kYes };

class BackendStub {
 public:
  virtual void RunBatch(BatchPlan plan, Preemption preempt) = 0;
};

class FrontendStub {
 public:
  virtual void MarkQueryDropped(int query_id) = 0;
};

}  // namespace nexus::shepherd
