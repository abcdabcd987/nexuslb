#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "ario/ario.h"
#include "bench_dispatcher/fake_accessor.h"
#include "nexus/common/time_util.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {

class FakeBackendDelegate : public BackendDelegate {
 public:
  FakeBackendDelegate(ario::EpollExecutor* executor, uint32_t node_id,
                      FakeDispatcherAccessor* accessor);

  void Tick() override;
  void SendLoadModelCommand(uint32_t gpu_idx, const ModelSession& model_session,
                            uint32_t max_batch,
                            ModelIndex model_index) override;
  void EnqueueBatchPlan(BatchPlanProto&& request) override;
  void DrainBatchPlans();

 private:
  void OnBatchFinish(const BatchPlanProto& plan);
  void OnTimer(ario::ErrorCode);

  ario::EpollExecutor* executor_;
  FakeDispatcherAccessor* accessor_;
  ario::Timer timer_;
  std::mutex mutex_;
  std::vector<BatchPlanProto> batchplans_;
};

}  // namespace dispatcher
}  // namespace nexus
