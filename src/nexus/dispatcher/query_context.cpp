#include "nexus/dispatcher/query_context.h"

namespace nexus {
namespace dispatcher {

QueryContext::QueryContext(DispatchRequest request, TimePoint deadline)
    : request(std::move(request)),
      global_id(this->request.query_without_input().global_id()),
      deadline(deadline) {}

}  // namespace dispatcher
}  // namespace nexus
