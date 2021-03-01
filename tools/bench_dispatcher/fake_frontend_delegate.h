#pragma once

#include "nexus/dispatcher/frontend_delegate.h"

namespace nexus {
namespace dispatcher {

class FakeFrontendDelegate : public FrontendDelegate {
 public:
  explicit FakeFrontendDelegate(uint32_t node_id);

  void Tick() override;
  void UpdateBackendList(BackendListUpdates&& request) override;
  void MarkQueryDroppedByDispatcher(DispatchReply&& request) override;

 private:
};

}  // namespace dispatcher
}  // namespace nexus
