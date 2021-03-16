#include "nexus/dispatcher/batch_policy.h"

namespace nexus {
namespace dispatcher {

BatchResult BatchPolicyBySlidingWindowWithFreeLunch::Getbatch(
    TimePoint exec_time, uint32_t target_batch_size,
    const SortedQueryList& queries, const ModelProfile& profile) {
  using namespace std::chrono;
  BatchResult ret;
  auto proc_elapse = nanoseconds(0);
  // TODO: WithNStd
  proc_elapse += nanoseconds(
      static_cast<long>(profile.GetPreprocessLatency() * 1e3));
  proc_elapse += nanoseconds(
      static_cast<long>(profile.GetPostprocessLatency() * 1e3));

  // Sliding window policy
  std::vector<std::shared_ptr<QueryContext>> inputs, drops, remains;
  auto qiter = queries.begin();
  size_t qremain = queries.size();
  uint32_t bs =
      std::min(static_cast<uint32_t>(qremain), target_batch_size);
  while (inputs.size() < bs && qiter != queries.end()) {
    --qremain;
    const auto& qctx = *qiter;
    auto deadline = inputs.empty() ? qctx->deadline : inputs[0]->deadline;
    auto fwd_elapse = nanoseconds(
        static_cast<long>(profile.GetForwardLatency(bs) * 1e3));
    auto finish_time = exec_time + fwd_elapse + proc_elapse;
    if (deadline > finish_time) {
      inputs.push_back(qctx);
    } else {
      drops.push_back(qctx);
      bs = std::min(static_cast<uint32_t>(inputs.size() + qremain),
                    target_batch_size);
    }
    ++qiter;
  }

  // See if there is any free lunch.
  while (qiter != queries.end()) {
    --qremain;
    const auto& qctx = *qiter;
    auto deadline = inputs.empty() ? qctx->deadline : inputs[0]->deadline;
    bs = inputs.size() + 1;
    auto fwd_elapse = nanoseconds(
        static_cast<long>(profile.GetForwardLatency(bs) * 1e3));
    auto finish_time = exec_time + fwd_elapse + proc_elapse;
    if (deadline > finish_time) {
      inputs.push_back(qctx);
    } else {
      break;
    }
    ++qiter;
  }

  remains.assign(qiter, queries.end());
  return {std::move(inputs), std::move(drops), std::move(remains)};
}

}  // namespace dispatcher
}  // namespace nexus
