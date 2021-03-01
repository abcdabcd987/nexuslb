#pragma once

#include <memory>
#include <unordered_map>

#include "nexus/common/typedef.h"
#include "nexus/dispatcher/accessor.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/dispatcher/frontend_delegate.h"

namespace nexus {
namespace dispatcher {

class FakeDispatcherAccessor : public DispatcherAccessor {
 public:
  std::shared_ptr<BackendDelegate> GetBackend(NodeId backend_id) override;
  std::shared_ptr<FrontendDelegate> GetFrontend(NodeId frontend_id) override;

  void AddBackend(NodeId backend_id, std::shared_ptr<BackendDelegate> backend);
  void AddFrontend(NodeId frontend_id,
                   std::shared_ptr<FrontendDelegate> frontend);

 private:
  std::unordered_map<NodeId, std::shared_ptr<BackendDelegate>> backends_;
  std::unordered_map<NodeId, std::shared_ptr<FrontendDelegate>> frontends_;
};

}  // namespace dispatcher
}  // namespace nexus
