#ifndef NEXUS_DISPATCHER_RANKMT_MODEL_THREAD_H_
#define NEXUS_DISPATCHER_RANKMT_MODEL_THREAD_H_

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
#include "nexus/common/rps_meter.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/dispatcher/batch_policy.h"
#include "nexus/dispatcher/batch_size_estimator.h"
#include "nexus/dispatcher/frontend_delegate.h"
#include "nexus/dispatcher/query_context.h"
#include "nexus/dispatcher/rankmt/common.h"
#include "nexus/proto/nnquery.pb.h"
#include "readerwriterqueue/readerwriterqueue.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

class RankThread;

class ModelThread {
 public:
  ModelThread(
      ario::EpollExecutor* executor, ModelSession model_session,
      const ModelProfile& profile, RankThread* rank_thread,
      std::unordered_map<NodeId, std::shared_ptr<FrontendDelegate>> frontends,
      std::unordered_map<NodeId, std::shared_ptr<BackendDelegate>> backends);
  ModelThread(const ModelThread& other) = delete;
  ModelThread& operator=(const ModelThread& other) = delete;
  ModelThread(ModelThread&& other) = delete;
  ModelThread& operator=(ModelThread&& other) = delete;
  ~ModelThread();
  void Stop(std::mutex& mutex, size_t& cnt, std::condition_variable& cv);

  // Getters
  moodycamel::ReaderWriterQueue<ModelCommand>* model_command_queue() {
    return &model_command_queue_;
  };
  moodycamel::ReaderWriterQueue<RankCommand>* rank_command_queue() {
    return &rank_command_queue_;
  };
  const ModelProfile& profile() const { return profile_; }
  const ModelSession& model_session() const { return model_session_; }
  const std::string& model_session_id() const { return model_session_id_; }

  CtrlStatus EnqueueQuery(DispatchRequest&& request);
  void PostCommand();

  // Control plane commands
  void PostAddBackend(NodeId backend_id,
                      std::shared_ptr<BackendDelegate> delegate);
  void PostAddFrontend(NodeId frontend_id,
                       std::shared_ptr<FrontendDelegate> delegate);
  void PostRemoveBackend(NodeId backend_id);
  void PostRemoveFrontend(NodeId frontend_id);

 private:
  // Command handlers
  void ExecuteCommand();
  void DoGrantedBackendMessage(GrantedBackendMessage& cmd);

  void UpdateTargetBatchSize(const std::optional<AvgStd>& rps);
  void UpdateCandidate(TimePoint earliest_exec_time);
  void OnDropTimer();
  void SendDroppedQueries(
      const std::vector<std::shared_ptr<QueryContext>>& drops);

  ario::EpollExecutor& executor_;
  RankThread& rank_thread_;
  ModelSession model_session_;
  std::string model_session_id_;
  // TODO: GPU performance heterogeneity
  const ModelProfile& profile_;
  bool stop_flag_;
  moodycamel::ReaderWriterQueue<ModelCommand> model_command_queue_;
  moodycamel::ReaderWriterQueue<RankCommand> rank_command_queue_;
  std::unordered_map<NodeId, std::shared_ptr<FrontendDelegate>> frontends_;
  std::unordered_map<NodeId, std::shared_ptr<BackendDelegate>> backends_;
  BatchSizeEstimator bse_;
  RpsMeter rps_meter_;
  SortedQueryList unprocessed_queries_;
  IncrementalBatchPolicy batch_policy_;
  uint32_t target_batch_size_;
  ExecutionCandidate candidate_;
  ario::Timer drop_timer_;
};

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus

#endif
