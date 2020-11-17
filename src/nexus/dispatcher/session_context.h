#ifndef NEXUS_DISPATCHER_SESSION_CONTEXT_H_
#define NEXUS_DISPATCHER_SESSION_CONTEXT_H_

#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class InstanceInfo;

class ModelSessionContext {
 public:
  explicit ModelSessionContext(ModelSession model_session);

  const ModelSession& model_session() const { return model_session_; }

  std::shared_ptr<InstanceInfo> GetInstanceInfo(uint32_t backend_id) const;
  void AddInstanceInfo(uint32_t backend_id, std::shared_ptr<InstanceInfo> inst);

 private:
  ModelSession model_session_;
  std::unordered_map<uint32_t, std::shared_ptr<InstanceInfo>> instances_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
