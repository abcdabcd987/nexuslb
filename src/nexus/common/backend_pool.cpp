#include "nexus/common/backend_pool.h"

#include <glog/logging.h>

#include "nexus/common/util.h"

namespace nexus {

BackendSession::BackendSession(const BackendInfo& info,
                               ario::RdmaQueuePair* conn,
                               RdmaSender rdma_sender)
    : node_id_(info.node_id()),
      ip_(info.ip()),
      server_port_(info.server_port()),
      rpc_port_(info.rpc_port()),
      conn_(conn),
      rdma_sender_(rdma_sender),
      running_(true) {}

BackendSession::~BackendSession() { Stop(); }

void BackendSession::Stop() {
  if (running_) {
    conn_->Shutdown();
  }
}

std::shared_ptr<BackendSession> BackendPool::GetBackend(uint32_t backend_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto iter = backends_.find(backend_id);
  if (iter == backends_.end()) {
    return nullptr;
  }
  return iter->second;
}

void BackendPool::AddBackend(std::shared_ptr<BackendSession> backend) {
  std::lock_guard<std::mutex> lock(mu_);
  backends_.emplace(backend->node_id(), backend);
}

void BackendPool::RemoveBackend(std::shared_ptr<BackendSession> backend) {
  std::lock_guard<std::mutex> lock(mu_);
  LOG(INFO) << "Remove backend " << backend->node_id();
  backend->Stop();
  backends_.erase(backend->node_id());
}

void BackendPool::RemoveBackend(uint32_t backend_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto iter = backends_.find(backend_id);
  if (iter == backends_.end()) {
    return;
  }
  LOG(INFO) << "Remove backend " << backend_id;
  iter->second->Stop();
  backends_.erase(iter);
}

std::vector<uint32_t> BackendPool::UpdateBackendList(
    std::unordered_set<uint32_t> list) {
  std::lock_guard<std::mutex> lock(mu_);
  // Remove backends that are not on the list
  for (auto iter = backends_.begin(); iter != backends_.end();) {
    if (list.count(iter->first) == 0) {
      auto backend_id = iter->first;
      iter->second->Stop();
      iter = backends_.erase(iter);
      LOG(INFO) << "Remove backend " << backend_id;
    } else {
      ++iter;
    }
  }
  // Find out new backends
  std::vector<uint32_t> missing;
  for (auto backend_id : list) {
    if (backends_.count(backend_id) == 0) {
      missing.push_back(backend_id);
    }
  }
  return missing;
}

void BackendPool::StopAll() {
  std::lock_guard<std::mutex> lock(mu_);
  for (auto iter : backends_) {
    iter.second->Stop();
  }
  backends_.clear();
}

}  // namespace nexus
