#include "bench_dispatcher/fake_backend_delegate.h"

#include "nexus/dispatcher/backend_delegate.h"

namespace nexus {
namespace dispatcher {

FakeBackendDelegate::FakeBackendDelegate(uint32_t node_id,
                                         std::string gpu_device,
                                         std::string gpu_uuid,
                                         size_t gpu_available_memory)
    : BackendDelegate(node_id, std::move(gpu_device), std::move(gpu_uuid),
                      gpu_available_memory) {}

void FakeBackendDelegate::Tick() {}

void FakeBackendDelegate::SendLoadModelCommand(
    const ModelSession& model_session, uint32_t max_batch) {}

void FakeBackendDelegate::EnqueueBatchPlan(BatchPlanProto&& request) {}

}  // namespace dispatcher
}  // namespace nexus
