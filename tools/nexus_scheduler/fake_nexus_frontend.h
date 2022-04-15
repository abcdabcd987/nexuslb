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
                    const FakeObjectAccessor* accessor, uint32_t node_id);
  void Start();
  void Stop();

  uint32_t node_id() const { return node_id_; }

  void UpdateModelRoutes(const ModelRouteUpdates& request);

  std::shared_ptr<ModelHandler> LoadModel(const NexusLoadModelReply& reply);

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

  boost::asio::system_timer report_workload_timer_;
};

}  // namespace app
}  // namespace nexus

#endif  // NEXUS_APP_APP_BASE_H_
