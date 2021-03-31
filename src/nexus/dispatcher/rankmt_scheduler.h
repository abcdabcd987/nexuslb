#ifndef NEXUS_DISPATCHER_RANKMT_SCHEDULER_H_
#define NEXUS_DISPATCHER_RANKMT_SCHEDULER_H_

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "ario/epoll.h"
#include "ario/timer.h"
#include "nexus/common/metric.h"
#include "nexus/common/model_db.h"
#include "nexus/common/rps_meter.h"
#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"
#include "nexus/common/value_ranked_splay_map.h"
#include "nexus/dispatcher/accessor.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/dispatcher/batch_policy.h"
#include "nexus/dispatcher/batch_size_estimator.h"
#include "nexus/dispatcher/query_context.h"
#include "nexus/dispatcher/scheduler.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"
#include "readerwriterqueue/readerwriterqueue.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

class ModelThread;
class RankThread;

struct ExecutionCandidate {
  TimePoint earliest_exec_time;
  TimePoint latest_exec_time;
  TimePoint deadline;
  uint32_t batch_size;

  static ExecutionCandidate Invalid() {
    return {TimePoint::max(), TimePoint::max(), TimePoint::max(), 0};
  }
};

struct ModelSessionContext {
  ModelSessionContext(ModelSession model_session, const ModelProfile& profile);

  ModelSession model_session;
  std::string string_id;
  SortedQueryList queries;
  IncrementalBatchPolicy batch_policy;
  RpsMeter rps_meter;

  // TODO: GPU performance heterogeneity
  const ModelProfile& profile;
  uint32_t target_batch_size = 0;
};

struct BackendContext {
  BackendContext(NodeId backend_id, std::shared_ptr<BackendDelegate> delegate);

  NodeId backend_id;
  std::shared_ptr<BackendDelegate> delegate;
  TimePoint schedule_time;
  TimePoint next_available_time;

  ario::Timer schedule_timer;
};

struct PopQueueHeadCommand {
  TimePoint now;
  TimePoint earliest_exec_time;
};

struct SendPlanCommand {
  ExecutionCandidate candidate;
  NodeId backend_id;
  PlanId plan_id;
  TimePoint send_time;
  TimePoint exec_time;
  TimePoint finish_time;
};

using ModelCommand = std::variant<PopQueueHeadCommand, SendPlanCommand>;

struct AddModelThreadCommand {
  ModelSession model_session;
  ModelThread* model_thread;
};

struct AddBackendCommand {
  NodeId backend_id;
  std::shared_ptr<BackendDelegate> backend_delegate;
};

struct UpdateCandidateCommand {
  TimePoint now;
  std::string model_session_id;  // PERFORMANCE: remove
  ExecutionCandidate candidate;
};

using RankCommand = std::variant<AddModelThreadCommand, AddBackendCommand,
                                 UpdateCandidateCommand>;

class ModelThread {
 public:
  ModelThread(ario::EpollExecutor* executor, ModelSession model_session,
              const ModelProfile& profile, RankThread* rank_thread,
              DispatcherAccessor* dispatcher);
  ModelThread(const ModelThread& other) = delete;
  ModelThread& operator=(const ModelThread& other) = delete;
  ModelThread(ModelThread&& other) = delete;
  ModelThread& operator=(ModelThread&& other) = delete;
  ~ModelThread();
  void Stop();

  // Getters
  moodycamel::ReaderWriterQueue<ModelCommand>* model_command_queue() {
    return &model_command_queue_;
  };
  moodycamel::ReaderWriterQueue<RankCommand>* rank_command_queue() {
    return &rank_command_queue_;
  };
  const ModelProfile& profile() const { return mctx_.profile; }

  CtrlStatus EnqueueQuery(DispatchRequest&& request);
  void PostCommand();

 private:
  // Command handlers
  void ExecuteCommand();
  void DoPopQueueHeadCommand(PopQueueHeadCommand& cmd);
  void DoSendPlanCommand(SendPlanCommand& cmd);

  void UpdateTargetBatchSize(const std::optional<AvgStd>& rps);
  void UpdateCandidate(TimePoint now, TimePoint earliest_exec_time);
  void SendDroppedQueries(
      const std::vector<std::shared_ptr<QueryContext>>& drops);

  ario::EpollExecutor& executor_;
  RankThread& rank_thread_;
  DispatcherAccessor& dispatcher_;
  moodycamel::ReaderWriterQueue<ModelCommand> model_command_queue_;
  moodycamel::ReaderWriterQueue<RankCommand> rank_command_queue_;
  BatchSizeEstimator bse_;
  std::unordered_map<GlobalId, std::shared_ptr<QueryContext>> queries_;
  ModelSessionContext mctx_;
};

class RankThread {
 public:
  RankThread(
      ario::EpollExecutor* executor,
      moodycamel::ReaderWriterQueue<RankCommand>* scheduler_command_queue);
  RankThread(const RankThread& other) = delete;
  RankThread& operator=(const RankThread& other) = delete;
  RankThread(RankThread&& other) = delete;
  RankThread& operator=(RankThread&& other) = delete;
  ~RankThread();
  void Stop();

  ario::EpollExecutor& executor() const { return executor_; }

  void PostCommandFromScheduler();
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
    TimePoint send_time;
    TimePoint exec_time;
    TimePoint finish_time;
    TimePoint deadline;
    std::shared_ptr<CandidateInfo> cinfo;

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

  // Command handlers
  void ExecuteCommand(moodycamel::ReaderWriterQueue<RankCommand>& queue);
  void DoAddModelThreadCommand(AddModelThreadCommand& cmd);
  void DoAddBackendCommand(AddBackendCommand& cmd);
  void DoUpdateCandidateCommand(UpdateCandidateCommand& cmd);

  PlanId NextPlanId();
  void UpdateActivePlans(TimePoint now, TimePoint earliest_exec_time,
                         PerModelThreadData& mdata, size_t num_idle_backends);
  void SetupActivePlan(TimePoint now, TimePoint earliest_exec_time,
                       PerModelThreadData& mdata,
                       std::shared_ptr<CandidateInfo> cinfo);
  void RemoveActivePlan(PerModelThreadData& mdata);
  void OnBackendAvailableSoon(NodeId backend_id);
  std::shared_ptr<CandidateInfo> PopCandidatePool(TimePoint now,
                                                  TimePoint earliest_exec_time,
                                                  size_t rank);
  void OnPlanTimer(PlanId plan_id);

  ario::EpollExecutor& executor_;
  moodycamel::ReaderWriterQueue<RankCommand>& scheduler_command_queue_;
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

class MultiThreadRankScheduler {
 public:
  class Builder {
   public:
    explicit Builder(ario::EpollExecutor* scheduler_executor,
                     ario::EpollExecutor* rank_thread_executor);
    std::unique_ptr<MultiThreadRankScheduler> Build(
        std::unique_ptr<DispatcherAccessor> dispatcher);

   private:
    ario::EpollExecutor* scheduler_executor_;
    ario::EpollExecutor* rank_thread_executor_;
  };

  MultiThreadRankScheduler(std::unique_ptr<DispatcherAccessor> dispatcher,
                           ario::EpollExecutor* scheduler_executor,
                           ario::EpollExecutor* rank_thread_executor);
  ~MultiThreadRankScheduler();
  void Stop();
  void AddModelSession(ario::EpollExecutor* model_thread_executor,
                       ModelSession model_session);
  void AddBackend(NodeId backend_id);

 private:
  struct GpuInfoForProfile {
    std::string gpu_device;
    std::string gpu_uuid;
  };

  std::unique_ptr<DispatcherAccessor> dispatcher_;
  ario::EpollExecutor& executor_;
  moodycamel::ReaderWriterQueue<RankCommand> rank_thread_command_queue_;
  RankThread rank_thread_;

  std::mutex mutex_;
  // PERFORMANCE: model_session_id
  std::unordered_map<std::string, std::unique_ptr<ModelThread>> model_threads_;
  // TODO: GPU heterogeneity.
  std::optional<GpuInfoForProfile> gpu_info_for_profile_;
};

}  // namespace rankmt

using MultiThreadRankScheduler = rankmt::MultiThreadRankScheduler;

}  // namespace dispatcher
}  // namespace nexus

#endif
