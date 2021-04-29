#ifndef NEXUS_DISPATCHER_RANKMT_RANK_THREAD_H_
#define NEXUS_DISPATCHER_RANKMT_RANK_THREAD_H_

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

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
  void PostAddModelThread(ModelSession model_session,
                          ModelThread* model_thread);
  void PostAddBackend(NodeId backend_id,
                      std::shared_ptr<BackendDelegate> delegate);
  void PostRemoveBackend(NodeId backend_id);

  // Commands from model threads
  void PostCommandFromModelThread(const std::string* ptr_model_session_id);

 private:
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

  struct PerModelThreadData {
    ModelThread& model_thread;  // TODO: replace with capability
    std::string model_session_id;
    const ModelProfile& profile;
    moodycamel::ReaderWriterQueue<ModelCommand>& model_command_queue;
    moodycamel::ReaderWriterQueue<RankCommand>& rank_command_queue;
    std::shared_ptr<ActivePlan> active_plan;
  };

  struct BackendContext {
    BackendContext(ario::EpollExecutor* executor, NodeId backend_id,
                   std::shared_ptr<BackendDelegate> delegate);

    NodeId backend_id;
    std::shared_ptr<BackendDelegate> delegate;
    TimePoint next_available_time;

    ario::Timer schedule_timer;
  };

  // Handlers for commands from model threads
  void ExecuteCommand(const std::string* ptr_model_session_id);
  void DoUpdateCandidateCommand(UpdateCandidateCommand& cmd,
                                const std::string* ptr_model_session_id);
  void DoUpdateBackendCommand(UpdateBackendCommand& cmd);

  PlanId NextPlanId();
  void UpdateActivePlans(TimePoint earliest_exec_time,
                         PerModelThreadData& mdata);
  void SetupActivePlan(TimePoint earliest_exec_time, PerModelThreadData& mdata,
                       std::shared_ptr<CandidateInfo> cinfo);
  void RemoveActivePlan(PerModelThreadData& mdata);
  void OnBackendAvailableSoon(NodeId backend_id);
  void OnPlanTimer(PlanId plan_id);
  void UpdateBackend(BackendContext* bctx, TimePoint next_available_time);

  ario::EpollExecutor& executor_;
  bool stop_flag_;
  PlanId next_plan_id_{1};
  std::unordered_map<NodeId, std::shared_ptr<BackendContext>> backends_;

  // PERFORMANCE: model_session_id
  std::unordered_map<std::string, std::unique_ptr<PerModelThreadData>>
      model_threads_;

  // PERFORMANCE: model_session_id
  ValueRankedSplayMap<std::string, std::shared_ptr<CandidateInfo>,
                      CandidateInfo::CompareKeyFn>
      candidate_pool_;
  ValueRankedSplayMap<NodeId, TimePoint> backend_availability_pool_;

  std::unordered_map<PlanId, std::shared_ptr<ActivePlan>> plans_;
};

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus

#endif
