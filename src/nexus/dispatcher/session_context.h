#ifndef NEXUS_DISPATCHER_SESSION_CONTEXT_H_
#define NEXUS_DISPATCHER_SESSION_CONTEXT_H_

#include "nexus/common/typedef.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class ModelSessionContext {
 public:
  ModelSessionContext(ModelSession model_session, ModelIndex model_index);

  const ModelSession& model_session() const { return model_session_; }
  ModelIndex model_index() const { return model_index_; }

 private:
  ModelSession model_session_;
  ModelIndex model_index_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
