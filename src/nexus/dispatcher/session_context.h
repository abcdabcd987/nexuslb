#ifndef NEXUS_DISPATCHER_SESSION_CONTEXT_H_
#define NEXUS_DISPATCHER_SESSION_CONTEXT_H_

#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class ModelSessionContext {
 public:
  explicit ModelSessionContext(ModelSession model_session);

  const ModelSession& model_session() const { return model_session_; }

 private:
  ModelSession model_session_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
