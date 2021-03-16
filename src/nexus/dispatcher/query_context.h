#ifndef NEXUS_DISPATCHER_QUERY_CONTEXT_H_
#define NEXUS_DISPATCHER_QUERY_CONTEXT_H_

#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace dispatcher {

struct QueryContext {
  QueryContext(DispatchRequest request, TimePoint deadline);

  DispatchRequest request;
  GlobalId global_id;
  TimePoint deadline;

  struct OrderByDeadlineASC {
    bool operator()(const std::shared_ptr<QueryContext>& lhs,
                    const std::shared_ptr<QueryContext>& rhs) const {
      return lhs->deadline < rhs->deadline ||
             (lhs->deadline == rhs->deadline &&
              lhs->global_id.t < rhs->global_id.t);
    }
  };
};

using SortedQueryList =
    std::set<std::shared_ptr<QueryContext>, QueryContext::OrderByDeadlineASC>;

}  // namespace dispatcher
}  // namespace nexus

#endif
