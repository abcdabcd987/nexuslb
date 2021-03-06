#ifndef NEXUS_DISPATCHER_RANKMT_RANK_THREAD_H_
#define NEXUS_DISPATCHER_RANKMT_RANK_THREAD_H_

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
  explicit RankThread(ario::EpollExecutor* executor);
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
                      std::shared_ptr<BackendDelegate> delegate);
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
  struct CandidateInfo {
    PerModelThreadData& mdata;
    ExecutionCandidate candidate;

    struct CompareKeyFn {
      TimePoint operator()(const std::shared_ptr<CandidateInfo>& obj) const {
        return obj->candidate.latest_exec_time;
      }
    };
  };

  struct ActivePlan {
    explicit ActivePlan(ario::EpollExecutor& executor);

    PlanId plan_id;
    TimePoint exec_time;
    PerModelThreadData* mdata;

    ario::Timer send_timer;
  };

  struct MessagesFromModelThread {
    std::optional<ExecutionCandidate> new_candidate;
    bool resume_candidate_update;
  };

  struct PerModelThreadData {
    ModelThread& model_thread;
    ModelIndex model_index;
    const ModelProfile& profile;
    moodycamel::ReaderWriterQueue<RankCommand>& rank_command_queue;
    std::shared_ptr<ActivePlan> active_plan;
    bool rejecting_candidates;

    std::mutex model_msg_mutex;
    MessagesFromModelThread model_msg /* GUARDED_BY(model_msg_mutex) */;
  };

  struct BackendContext {
    BackendContext(NodeId backend_id,
                   std::shared_ptr<BackendDelegate> delegate);

    NodeId backend_id;
    std::shared_ptr<BackendDelegate> delegate;
    TimePoint next_available_time;
  };

  // Handlers for commands from model threads
  void ExecuteCommand(PerModelThreadData& mdata);
  void DoUpdateBackendCommand(UpdateBackendCommand& cmd);
  void DoUpdateCandidate(PerModelThreadData& mdata);

  PlanId NextPlanId();
  void SetupActivePlan(PerModelThreadData& mdata);
  void OnPlanTimer(PlanId plan_id);
  void UpdateBackend(BackendContext* bctx, TimePoint next_available_time);

  void Poll();

  ario::EpollExecutor& executor_;
  bool stop_flag_;
  Poller poller_;
  PlanId next_plan_id_{1};
  std::unordered_map<NodeId, std::shared_ptr<BackendContext>> backends_;
  std::vector<std::unique_ptr<PerModelThreadData>> model_threads_;

  ValueRankedSplayMap<ModelIndex, std::shared_ptr<CandidateInfo>,
                      CandidateInfo::CompareKeyFn>
      candidate_pool_;
  ValueRankedSplayMap<NodeId, TimePoint> backend_availability_pool_;

  std::unordered_map<PlanId, std::shared_ptr<ActivePlan>> plans_;
};

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus

#endif
