#include "nexus/dispatcher/inst_info.h"

namespace nexus {
namespace dispatcher {

InstanceInfo::InstanceInfo(ModelSession model_session, uint32_t backend_id,
                           const ModelProfile& profile)
    : model_session_(std::move(model_session)),
      backend_id_(backend_id),
      profile_(profile) {
  max_batch_ = profile_.GetMaxBatchWithFullBudget(model_session_.latency_sla());
}

}  // namespace dispatcher
}  // namespace nexus
