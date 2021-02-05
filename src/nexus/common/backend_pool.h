#ifndef NEXUS_COMMON_BACKEND_POOL_H_
#define NEXUS_COMMON_BACKEND_POOL_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include "ario/ario.h"
#include "nexus/common/connection.h"
#include "nexus/common/rdma_sender.h"
#include "nexus/common/time_util.h"
#include "nexus/proto/control.pb.h"

namespace nexus {

class BackendPool;

class BackendSession {
 public:
  explicit BackendSession(const BackendInfo& info, ario::RdmaQueuePair* conn,
                          RdmaSender rdma_sender);

  ~BackendSession();

  uint32_t node_id() const { return node_id_; }

  const std::string& ip() const { return ip_; }

  uint16_t port() const { return port_; }

  virtual void Stop();

 protected:
  uint32_t node_id_;
  std::string ip_;
  uint16_t port_;
  ario::RdmaQueuePair* conn_;
  RdmaSender rdma_sender_;
  std::atomic_bool running_;
  TimePoint expire_;
  std::mutex util_mu_;
};

class BackendPool {
 public:
  BackendPool() {}

  std::shared_ptr<BackendSession> GetBackend(uint32_t backend_id);

  void AddBackend(std::shared_ptr<BackendSession> backend);

  void RemoveBackend(std::shared_ptr<BackendSession> backend);

  void RemoveBackend(uint32_t backend_id);

  std::vector<uint32_t> UpdateBackendList(std::unordered_set<uint32_t> list);

  void StopAll();

 protected:
  std::unordered_map<uint32_t, std::shared_ptr<BackendSession> > backends_;
  std::mutex mu_;
};

}  // namespace nexus

#endif  // NEXUS_COMMON_BACKEND_POOL_H_
