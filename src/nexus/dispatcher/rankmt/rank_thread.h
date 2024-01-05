#ifndef NEXUS_DISPATCHER_RANKMT_RANK_THREAD_H_
#define NEXUS_DISPATCHER_RANKMT_RANK_THREAD_H_

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "ario/ario.h"
#include "nexus/common/model_db.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/common/value_ranked_splay_map.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/dispatcher/rankmt/common.h"
#include "nexus/proto/nnquery.pb.h"
#include "readerwriterqueue/readerwriterqueue.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

class ModelThread;

class RankThread {
 public:
  RankThread(RankmtConfig config, ario::EpollExecutor* executor);
  RankThread(const RankThread& other) = delete;
  RankThread& operator=(const RankThread& other) = delete;
  RankThread(RankThread&& other) = delete;
  RankThread& operator=(RankThread&& other) = delete;
  ~RankThread();
  void Stop(std::mutex& mutex, size_t& cnt, std::condition_variable& cv);

  ario::EpollExecutor& executor() const { return executor_; }

  // Control plane commands
  void PostAddModelThread(ModelIndex model_index, ModelThread* model_thread);
  void PostAddBackend(NodeId backend_id,
                      std::shared_ptr<BackendDelegate> backend);
  void PostRemoveBackend(NodeId backend_id);

  // Commands from model threads
  void PostResumeCandidateUpdate(ModelIndex model_index);
  void PostExecutionCandidate(ModelIndex model_index,
                              ExecutionCandidate candidate);

 private:
  class Poller : public ario::EventPoller {
   public:
    explicit Poller(RankThread* outer) : outer_(*outer) {}
    void Poll() override { outer_.Poll(); }

   private:
    RankThread& outer_;
  };

  struct PerModelThreadData;

  struct MessagesFromModelThread {
    std::optional<ExecutionCandidate> new_candidate;
    bool resume_candidate_update;
  };

  struct PerModelThreadData {
    ModelThread& model_thread;
    ModelIndex model_index;
    const ModelProfile& profile;
    moodycamel::ReaderWriterQueue<RankCommand>& rank_command_queue;
    ario::Timer send_timer;
    bool rejecting_candidates;
    std::optional<ExecutionCandidate> candidate;

    std::mutex model_msg_mutex;
    MessagesFromModelThread model_msg /* GUARDED_BY(model_msg_mutex) */;
  };

  struct GpuContext {
    GpuContext(ario::EpollExecutor* executor, GpuId gpu_id,
               GpuDelegate* delegate);

    GpuId gpu_id;
    GpuDelegate* delegate;
    TimePoint free_at;
    ario::Timer free_timer;
  };

  // Handlers for commands from model threads
  void ExecuteCommand(PerModelThreadData& mdata);
  void DoUpdateGpuCommand(UpdateGpuCommand& cmd);
  void DoUpdateCandidate(PerModelThreadData& mdata);

  PlanId NextPlanId();
  void SetupActivePlan(PerModelThreadData& mdata);
  void OnPlanTimer(PerModelThreadData& mdata);
  void GrantGpuToModel(PerModelThreadData& mdata, GpuContext* gctx);
  void UpdateGpu(GpuContext* gctx, TimePoint free_at);
  void SetGpuTimer();
  void OnGpuTimer(GpuContext* gctx);
  long GetPriority(const ExecutionCandidate& c, const ModelProfile& profile,
                   TimePoint now) const;

  void Poll();

  RankmtConfig config_;
  ario::EpollExecutor& executor_;
  bool stop_flag_;
  Poller poller_;
  PlanId next_plan_id_{1};
  std::unordered_map<NodeId, std::shared_ptr<BackendDelegate>> backends_;
  std::unordered_map<GpuId, std::shared_ptr<GpuContext>> gpus_;
  std::vector<std::unique_ptr<PerModelThreadData>> model_threads_;

  ValueRankedSplayMap<GpuId, TimePoint> gpu_availability_pool_;
  ValueRankedSplayMap<PerModelThreadData*, TimePoint> plans_rank_invalid_after_;
  ValueRankedSplayMap<PerModelThreadData*, std::chrono::nanoseconds>
      plans_rank_latency_;
  ValueRankedSplayMap<PerModelThreadData*, long> plans_rank_priority_;
};

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus

#endif
