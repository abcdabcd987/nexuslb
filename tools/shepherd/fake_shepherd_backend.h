#pragma once
#include "shepherd/common.h"

namespace nexus::shepherd {

class FakeShepherdBackend : public BackendStub {
 public:
  void RunBatch(BatchPlan plan, Preemption preempt) override;
};

}  // namespace nexus::shepherd
