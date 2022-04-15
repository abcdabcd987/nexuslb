#ifndef NEXUS_APP_FRONTEND_H_
#define NEXUS_APP_FRONTEND_H_

#include <boost/asio.hpp>
#include <memory>
#include <unordered_map>
#include <vector>

#include "nexus/proto/nexus.pb.h"
#include "nexus_scheduler/fake_object_accessor.h"
#include "nexus_scheduler/model_handler.h"

namespace nexus {
namespace app {

class FakeNexusFrontend {
 public:
  FakeNexusFrontend(boost::asio::io_context* io_context,
                    const FakeObjectAccessor* accessor, uint32_t node_id,
                    uint32_t beacon_interval_sec);
  void Start();
  void Stop();

  uint32_t node_id() const { return node_id_; }

  void UpdateModelRoutes(const ModelRouteUpdates& request);

  std::shared_ptr<ModelHandler> LoadModel(const NexusLoadModelReply& reply,
                                          size_t reserved_size);

  std::shared_ptr<ModelHandler> GetModelHandler(size_t model_idx);

 private:
  void UpdateBackendPoolAndModelRoute(const ModelRouteProto& route);

  void ReportWorkload();

 private:
  boost::asio::io_context& io_context_;
  const FakeObjectAccessor& accessor_;

  /*! \brief Frontend node ID */
  uint32_t node_id_;
  /*! \brief Interval to update stats to scheduler in seconds */
  uint32_t beacon_interval_sec_;
  /*!
   * \brief Map from model session ID to model handler.
   */
  std::unordered_map<std::string, std::shared_ptr<ModelHandler>> model_pool_;
  std::vector<std::shared_ptr<ModelHandler>> models_;

  // Scheduler-only test related:
 public:
  enum class QueryStatus {
    kPending,
    kDropped,
    kTimeout,
    kSuccess,
  };

  struct QueryContext {
    QueryStatus status;
    int64_t frontend_recv_ns;
  };

  const QueryContext* queries(size_t model_idx) const {
    return model_ctx_.at(model_idx)->queries.get();
  }
  size_t reserved_size(size_t model_idx) const {
    return model_ctx_.at(model_idx)->reserved_size;
  }

  void ReceivedQuery(size_t model_idx, uint64_t query_id,
                     int64_t frontend_recv_ns);
  void GotDroppedReply(size_t model_idx, uint64_t query_id);
  void GotSuccessReply(size_t model_idx, uint64_t query_id, int64_t finish_ns);

 private:
  struct ModelContext {
    size_t reserved_size;
    long slo_ns;
    std::unique_ptr<QueryContext[]> queries;
  };
  std::vector<std::unique_ptr<ModelContext>> model_ctx_;
  boost::asio::system_timer report_workload_timer_;
};

}  // namespace app
}  // namespace nexus

#endif  // NEXUS_APP_APP_BASE_H_
