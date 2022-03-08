#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
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
                      FakeDispatcherAccessor* accessor, bool save_archive);
  const std::deque<BatchPlanProto>& batchplan_archive() const {
    return batchplan_archive_;
  }

  void Tick() override;
  void SendLoadModelCommand(uint32_t gpu_idx, const ModelSession& model_session,
                            uint32_t max_batch,
                            ModelIndex model_index) override;
  void EnqueueBatchPlan(BatchPlanProto&& request) override;
  void DrainBatchPlans();

 private:
  void OnBatchFinish(const BatchPlanProto& plan);
  void OnTimer(ario::ErrorCode);
  void SetupTimer();
  void SaveBatchPlan(BatchPlanProto&& plan);

  ario::EpollExecutor* executor_;
  FakeDispatcherAccessor* accessor_;
  bool save_archive_;
  ario::Timer timer_;
  std::mutex mutex_;
  std::vector<BatchPlanProto> batchplans_;
  std::deque<BatchPlanProto> batchplan_archive_;
};

}  // namespace dispatcher
}  // namespace nexus
