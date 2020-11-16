#include "nexus/dispatcher/session_context.h"

namespace nexus {
namespace dispatcher {

ModelSessionContext::ModelSessionContext(ModelSession model_session)
    : model_session_(std::move(model_session)) {}

}  // namespace dispatcher
}  // namespace nexus
