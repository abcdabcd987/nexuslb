#pragma once
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

namespace nexus {
class FakeNexusBackend;

namespace app {
class FakeNexusFrontend;
}

namespace scheduler {
class Scheduler;
}

struct FakeObjectAccessor {
  scheduler::Scheduler* scheduler = nullptr;
  std::unordered_map<uint32_t, FakeNexusBackend*> backends;
  std::unordered_map<uint32_t, app::FakeNexusFrontend*> frontends;
};

};  // namespace nexus
