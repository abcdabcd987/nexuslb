#pragma once
#include <memory>
#include <mutex>
#include <unordered_map>

#include "nexus/proto/nexus.pb.h"

namespace nexus {

class FakeNexusBackend {
 public:
  explicit FakeNexusBackend(const BackendInfo& info);
  uint32_t node_id() const { return node_id_; }

 private:
  uint32_t node_id_;
};

class FakeNexusBackendPool {
 public:
  std::shared_ptr<FakeNexusBackend> GetBackend(uint32_t backend_id);
  void AddBackend(std::shared_ptr<FakeNexusBackend> backend);
  void RemoveBackend(std::shared_ptr<FakeNexusBackend> backend);
  void RemoveBackend(uint32_t backend_id);

 private:
  std::unordered_map<uint32_t, std::shared_ptr<FakeNexusBackend>> backends_;
  std::mutex mu_;
};

}  // namespace nexus
