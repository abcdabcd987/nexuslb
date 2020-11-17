#ifndef NEXUS_DISPATCHER_INST_INFO_H_
#define NEXUS_DISPATCHER_INST_INFO_H_

#include "nexus/common/model_db.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class InstanceInfo {
 public:
  InstanceInfo(ModelSession model_session, uint32_t backend_id,
               const ModelProfile& profile);

  const ModelSession& model_session() const { return model_session_; }
  uint32_t backend_id() const { return backend_id_; }
  const ModelProfile& profile() const { return profile_; }
  uint32_t max_batch() const { return max_batch_; }

 private:
  ModelSession model_session_;
  uint32_t backend_id_;
  const ModelProfile& profile_;
  uint32_t max_batch_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
