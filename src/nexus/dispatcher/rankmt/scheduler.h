#ifndef NEXUS_DISPATCHER_RANKMT_SCHEDULER_H_
#define NEXUS_DISPATCHER_RANKMT_SCHEDULER_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "ario/ario.h"
#include "nexus/common/typedef.h"
#include "nexus/dispatcher/backend_delegate.h"
#include "nexus/dispatcher/frontend_delegate.h"
#include "nexus/dispatcher/rankmt/model_thread.h"
#include "nexus/dispatcher/rankmt/rank_thread.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

class MultiThreadRankScheduler {
 public:
  class Builder {
   public:
    explicit Builder(ario::EpollExecutor* scheduler_executor,
                     ario::EpollExecutor* rank_thread_executor);
    std::unique_ptr<MultiThreadRankScheduler> Build();

   private:
    ario::EpollExecutor* scheduler_executor_;
    ario::EpollExecutor* rank_thread_executor_;
  };

  class RequestEntrance {
   public:
    RequestEntrance() : model_thread_(nullptr) {}
    RequestEntrance(const RequestEntrance& other) = default;
    RequestEntrance& operator=(const RequestEntrance& other) = default;
    RequestEntrance(RequestEntrance&& other) = default;
    RequestEntrance& operator=(RequestEntrance&& other) = default;
    CtrlStatus EnqueueQuery(DispatchRequest&& request);

    ModelIndex model_index() const { return model_thread_->model_index(); }
    const ModelSession& model_session() const {
      return model_thread_->model_session();
    }
    const std::string& model_session_id() const {
      return model_thread_->model_session_id();
    }

   private:
    friend class MultiThreadRankScheduler;
    explicit RequestEntrance(ModelThread* model_thread);

    ModelThread* model_thread_;
  };

  MultiThreadRankScheduler(ario::EpollExecutor* scheduler_executor,
                           ario::EpollExecutor* rank_thread_executor);
  ~MultiThreadRankScheduler();
  void Stop();
  [[nodiscard]] RequestEntrance AddModelSession(
      ario::EpollExecutor* model_thread_executor, ModelSession model_session);
  void AddBackend(NodeId backend_id, std::shared_ptr<BackendDelegate> delegate);
  void AddFrontend(NodeId frontend_id,
                   std::shared_ptr<FrontendDelegate> delegate);
  void RemoveBackend(NodeId backend_id);
  void RemoveFrontend(NodeId frontend_id);

 private:
  struct GpuInfoForProfile {
    std::string gpu_device;
    std::string gpu_uuid;
  };

  ario::EpollExecutor& executor_;
  RankThread rank_thread_;

  std::unordered_map<std::string, ModelIndex> model_index_table_;
  std::vector<std::unique_ptr<ModelThread>> model_threads_;

  // TODO: GPU heterogeneity.
  std::optional<GpuInfoForProfile> gpu_info_for_profile_;
  std::unordered_map<NodeId, std::shared_ptr<FrontendDelegate>> frontends_;
  std::unordered_map<NodeId, std::shared_ptr<BackendDelegate>> backends_;
};

}  // namespace rankmt

using MultiThreadRankScheduler = rankmt::MultiThreadRankScheduler;

}  // namespace dispatcher
}  // namespace nexus

#endif
