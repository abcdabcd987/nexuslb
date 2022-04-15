#pragma once
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

namespace nexus {
namespace app {
class FakeNexusFrontend;
}
namespace backend {
class FakeNexusBackend;
}
namespace scheduler {
class Scheduler;
}
class QueryCollector;

struct FakeObjectAccessor {
  QueryCollector* query_collector = nullptr;
  scheduler::Scheduler* scheduler = nullptr;
  std::unordered_map<uint32_t, backend::FakeNexusBackend*> backends;
  std::unordered_map<uint32_t, app::FakeNexusFrontend*> frontends;
};

};  // namespace nexus
