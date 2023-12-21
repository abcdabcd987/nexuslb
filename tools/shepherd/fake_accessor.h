#pragma once
#include <memory>
#include <unordered_map>

namespace nexus::shepherd {

class FakeShepherdFrontend;

class FakeObjectAccessor {
 public:
  std::shared_ptr<FakeShepherdFrontend> GetFrontend(int model_id) {
    return frontends_.at(model_id);
  }

  void AddFrontend(int model_id,
                   std::shared_ptr<FakeShepherdFrontend> frontend) {
    frontends_.emplace(model_id, frontend);
  }

 private:
  std::unordered_map<int, std::shared_ptr<FakeShepherdFrontend>> frontends_;
};

}  // namespace nexus::shepherd
