#include "nexus/dispatcher/session_context.h"

#include <glog/logging.h>

namespace nexus {
namespace dispatcher {

ModelSessionContext::ModelSessionContext(ModelSession model_session,
                                         ModelIndex model_index)
    : model_session_(std::move(model_session)), model_index_(model_index) {}

}  // namespace dispatcher
}  // namespace nexus
