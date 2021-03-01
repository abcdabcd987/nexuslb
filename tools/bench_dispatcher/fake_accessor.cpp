#include "bench_dispatcher/fake_accessor.h"

#include <glog/logging.h>

namespace nexus {
namespace dispatcher {

std::shared_ptr<BackendDelegate> FakeDispatcherAccessor::GetBackend(
    NodeId backend_id) {
  auto iter = backends_.find(backend_id);
  if (iter == backends_.end()) {
    LOG(FATAL) << "Cannot find Backend " << backend_id.t;
  }
  return iter->second;
}

std::shared_ptr<FrontendDelegate> FakeDispatcherAccessor::GetFrontend(
    NodeId frontend_id) {
  auto iter = frontends_.find(frontend_id);
  if (iter == frontends_.end()) {
    LOG(FATAL) << "Cannot find Frontend " << frontend_id.t;
  }
  return iter->second;
}

void FakeDispatcherAccessor::AddBackend(
    NodeId backend_id, std::shared_ptr<BackendDelegate> backend) {
  auto res = backends_.try_emplace(backend_id, backend);
  if (!res.second) {
    LOG(FATAL) << "Backend " << backend_id.t
               << " has already been added before.";
  }
}

void FakeDispatcherAccessor::AddFrontend(
    NodeId frontend_id, std::shared_ptr<FrontendDelegate> frontend) {
  auto res = frontends_.try_emplace(frontend_id, frontend);
  if (!res.second) {
    LOG(FATAL) << "Frontend " << frontend_id.t
               << " has already been added before.";
  }
}

}  // namespace dispatcher
}  // namespace nexus
