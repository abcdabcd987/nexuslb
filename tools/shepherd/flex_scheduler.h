#pragma once
#include <boost/asio/io_context.hpp>
#include <boost/asio/system_timer.hpp>
#include <boost/container/small_vector.hpp>
#include <memory>
#include <optional>
#include <set>
#include <unordered_map>
#include <vector>

#include "nexus/common/model_db.h"
#include "shepherd/common.h"

namespace nexus::shepherd {

class FlexScheduler {
 public:
  FlexScheduler(boost::asio::io_context* io_context,
                std::chrono::nanoseconds dctrl, std::chrono::nanoseconds ddata,
                float preempt_lambda);

  void AddModel(int model_id, int slo_ms, const ModelProfile* profile);
  void AddGpu(int gpu_id, BackendStub* backend_stub);
  void AddQuery(Query query, FrontendStub* frontend_stub);

 private:
  struct QueryContext {
    Query query;
    TimePoint deadline;
    FrontendStub& frontend_stub;

    struct OrderByDeadlineASC {
      bool operator()(const std::shared_ptr<QueryContext>& lhs,
                      const std::shared_ptr<QueryContext>& rhs) const {
        return lhs->deadline < rhs->deadline ||
               (lhs->deadline == rhs->deadline &&
                lhs->query.query_id < rhs->query.query_id);
      }
    };
  };
  using SortedQueryList =
      std::set<std::shared_ptr<QueryContext>, QueryContext::OrderByDeadlineASC>;

  struct ModelContext {
    int slo_ms;
    const ModelProfile& profile;
    SortedQueryList queue;
  };

  struct GpuContext {
    int gpu_id;
    BackendStub& backend_stub;
    TimePoint free_at;
    boost::asio::system_timer free_timer;
    std::optional<BatchPlan> current_batch;
  };

  struct BatchGenResult {
    std::optional<BatchPlan> batch;
    std::vector<std::shared_ptr<QueryContext>> timeouts;
  };

  BatchGenResult BatchGen(TimePoint sched_at, int gpu_id,
                          boost::container::small_vector<int, 2> model_ids);
  void OnGpuCompletion(int gpu_id);
  void AssignBatchPlan(const BatchPlan& batch, int gpu_id, Preemption preempt);
  void SendDroppedQueries(
      const std::vector<std::shared_ptr<QueryContext>>& dropped);

  boost::asio::io_context& io_context_;
  std::chrono::nanoseconds dctrl_;
  std::chrono::nanoseconds ddata_;
  float preempt_lambda_;
  std::unordered_map<int, std::shared_ptr<ModelContext>> models_;
  std::unordered_map<int, std::shared_ptr<GpuContext>> gpus_;
  std::unordered_map<int, std::shared_ptr<QueryContext>> queries_;
};

}  // namespace nexus::shepherd
