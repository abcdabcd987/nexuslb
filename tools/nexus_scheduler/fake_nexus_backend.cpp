#include "nexus_scheduler/fake_nexus_backend.h"

namespace nexus {

FakeNexusBackend::FakeNexusBackend(const BackendInfo& info)
    : node_id_(info.node_id()) {}

std::shared_ptr<FakeNexusBackend> FakeNexusBackendPool::GetBackend(
    uint32_t backend_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto iter = backends_.find(backend_id);
  if (iter == backends_.end()) {
    return nullptr;
  }
  return iter->second;
}

void FakeNexusBackendPool::AddBackend(
    std::shared_ptr<FakeNexusBackend> backend) {
  std::lock_guard<std::mutex> lock(mu_);
  backends_.emplace(backend->node_id(), backend);
}

void FakeNexusBackendPool::RemoveBackend(
    std::shared_ptr<FakeNexusBackend> backend) {
  std::lock_guard<std::mutex> lock(mu_);
  backends_.erase(backend->node_id());
}

void FakeNexusBackendPool::RemoveBackend(uint32_t backend_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto iter = backends_.find(backend_id);
  if (iter == backends_.end()) {
    return;
  }
  backends_.erase(iter);
}
}  // namespace nexus
