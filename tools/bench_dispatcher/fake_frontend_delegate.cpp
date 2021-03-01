#include "bench_dispatcher/fake_frontend_delegate.h"

#include "nexus/dispatcher/frontend_delegate.h"

namespace nexus {
namespace dispatcher {

FakeFrontendDelegate::FakeFrontendDelegate(uint32_t node_id)
    : FrontendDelegate(node_id) {}

void FakeFrontendDelegate::Tick() {}

void FakeFrontendDelegate::UpdateBackendList(BackendListUpdates&& request) {}

void FakeFrontendDelegate::MarkQueryDroppedByDispatcher(
    DispatchReply&& request) {}

}  // namespace dispatcher
}  // namespace nexus
