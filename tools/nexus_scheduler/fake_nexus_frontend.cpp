#include "nexus_scheduler/fake_nexus_frontend.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "nexus/common/config.h"
#include "nexus_scheduler/fake_nexus_backend.h"
#include "nexus_scheduler/scheduler.h"

namespace nexus {
namespace app {

FakeNexusFrontend::FakeNexusFrontend(boost::asio::io_context* io_context,
                                     const FakeObjectAccessor* accessor,
                                     uint32_t node_id)
    : io_context_(*io_context),
      accessor_(*accessor),
      node_id_(node_id),
      beacon_interval_sec_(accessor_.scheduler->beacon_interval_sec()),
      report_workload_timer_(io_context_) {}

void FakeNexusFrontend::Start() { ReportWorkload(); }

void FakeNexusFrontend::Stop() { report_workload_timer_.cancel(); }

void FakeNexusFrontend::UpdateModelRoutes(const ModelRouteUpdates& request) {
  for (auto model_route : request.model_route()) {
    UpdateBackendPoolAndModelRoute(model_route);
  }
}

std::shared_ptr<ModelHandler> FakeNexusFrontend::LoadModel(
    const NexusLoadModelReply& reply) {
  CHECK(reply.status() == CTRL_OK)
      << "Load model error: " << CtrlStatus_Name(reply.status());
  auto model_handler = std::make_shared<ModelHandler>(
      reply.model_route().model_session_id(), &accessor_);
  // Only happens at Setup stage, so no concurrent modification to model_pool_
  CHECK(!model_pool_.count(model_handler->model_session_id()));
  model_pool_.emplace(model_handler->model_session_id(), model_handler);
  UpdateBackendPoolAndModelRoute(reply.model_route());

  models_.push_back(model_handler);
  return model_handler;
}

std::shared_ptr<ModelHandler> FakeNexusFrontend::GetModelHandler(
    size_t model_idx) {
  return models_.at(model_idx);
}

void FakeNexusFrontend::UpdateBackendPoolAndModelRoute(
    const ModelRouteProto& route) {
  auto& model_session_id = route.model_session_id();
  LOG(INFO) << "Update model route for " << model_session_id;
  auto model_handler = model_pool_.at(model_session_id);

  // Update route to backends with throughput in model handler
  model_handler->UpdateRoute(route);
}

void FakeNexusFrontend::ReportWorkload() {
  WorkloadStatsProto workload_stats;
  workload_stats.set_node_id(node_id_);
  for (auto const& iter : model_pool_) {
    auto model_session_id = iter.first;
    auto history = iter.second->counter()->GetHistory();
    auto model_stats = workload_stats.add_model_stats();
    model_stats->set_model_session_id(model_session_id);
    for (auto nreq : history) {
      model_stats->add_num_requests(nreq);
    }
  }
  accessor_.scheduler->ReportWorkload(workload_stats);

  report_workload_timer_.expires_from_now(
      std::chrono::seconds(beacon_interval_sec_));
  report_workload_timer_.async_wait([this](boost::system::error_code ec) {
    if (ec) return;
    ReportWorkload();
  });
}

}  // namespace app
}  // namespace nexus
