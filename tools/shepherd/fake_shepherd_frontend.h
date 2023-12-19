#pragma once
#include "shepherd/common.h"

namespace nexus::shepherd {

class FakeShepherdFrontend : public FrontendStub {
 public:
  void MarkQueryDropped(int query_id) override;
};

}  // namespace nexus::shepherd
