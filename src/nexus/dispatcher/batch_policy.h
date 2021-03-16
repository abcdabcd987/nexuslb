#ifndef NEXUS_DISPATCHER_BATCH_POLICY_H_
#define NEXUS_DISPATCHER_BATCH_POLICY_H_

#include <memory>
#include <vector>

#include "nexus/common/model_db.h"
#include "nexus/dispatcher/query_context.h"

namespace nexus {
namespace dispatcher {

struct BatchResult {
  std::vector<std::shared_ptr<QueryContext>> inputs, drops, remains;
};

class BatchPolicyBySlidingWindowWithFreeLunch {
 public:
  BatchResult Getbatch(TimePoint exec_time, uint32_t target_batch_size,
                       const SortedQueryList& queries,
                       const ModelProfile& profile);
};

}  // namespace dispatcher
}  // namespace nexus

#endif
