#include "nexus_scheduler/fake_nexus_frontend.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "nexus/common/config.h"
#include "nexus_scheduler/fake_nexus_backend.h"

namespace nexus {
namespace app {

Frontend::Frontend(uint32_t node_id) : node_id_(node_id), rand_gen_(rd_()) {}

Frontend::~Frontend() {}

void Frontend::Run() {
  running_ = true;
  daemon_thread_ = std::thread(&Frontend::Daemon, this);
}

void Frontend::Stop() {
  running_ = false;
  daemon_thread_.join();
}

void Frontend::UpdateModelRoutes(const ModelRouteUpdates& request) {
  int success = true;
  for (auto model_route : request.model_route()) {
    if (!UpdateBackendPoolAndModelRoute(model_route)) {
      success = false;
    }
  }
  CHECK(success) << "MODEL_SESSION_NOT_LOADED";
}

std::shared_ptr<ModelHandler> Frontend::LoadModel(
    const NexusLoadModelReply& reply) {
  CHECK(reply.status() == CTRL_OK)
      << "Load model error: " << CtrlStatus_Name(reply.status());
  auto model_handler = std::make_shared<ModelHandler>(
      reply.model_route().model_session_id(), backend_pool_);
  // Only happens at Setup stage, so no concurrent modification to model_pool_
  model_pool_.emplace(model_handler->model_session_id(), model_handler);
  UpdateBackendPoolAndModelRoute(reply.model_route());

  return model_handler;
}

bool Frontend::UpdateBackendPoolAndModelRoute(const ModelRouteProto& route) {
  auto& model_session_id = route.model_session_id();
  LOG(INFO) << "Update model route for " << model_session_id;
  // LOG(INFO) << route.DebugString();
  auto iter = model_pool_.find(model_session_id);
  if (iter == model_pool_.end()) {
    LOG(ERROR) << "Cannot find model handler for " << model_session_id;
    return false;
  }
  auto model_handler = iter->second;
  // Update backend pool first
  {
    std::lock_guard<std::mutex> lock(backend_sessions_mu_);
    auto old_backends = model_handler->BackendList();
    std::unordered_set<uint32_t> new_backends;
    // Add new backends
    for (auto backend : route.backend_rate()) {
      uint32_t backend_id = backend.info().node_id();
      if (backend_sessions_.count(backend_id) == 0) {
        backend_sessions_.emplace(
            backend_id, std::unordered_set<std::string>{model_session_id});
        backend_pool_.AddBackend(
            std::make_shared<FakeNexusBackend>(backend.info()));
      } else {
        backend_sessions_.at(backend_id).insert(model_session_id);
      }
      new_backends.insert(backend_id);
    }
    // Remove unused backends
    for (auto backend_id : old_backends) {
      if (new_backends.count(backend_id) == 0) {
        backend_sessions_.at(backend_id).erase(model_session_id);
        if (backend_sessions_.at(backend_id).empty()) {
          LOG(INFO) << "Remove backend " << backend_id;
          backend_sessions_.erase(backend_id);
          backend_pool_.RemoveBackend(backend_id);
        }
      }
    }
  }
  // Update route to backends with throughput in model handler
  model_handler->UpdateRoute(route);
  return true;
}

void Frontend::Daemon() {
  while (running_) {
    auto next_time = Clock::now() + std::chrono::seconds(beacon_interval_sec_);
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
    ReportWorkload(workload_stats);
    std::this_thread::sleep_until(next_time);
  }
}

void Frontend::ReportWorkload(const WorkloadStatsProto& request) {
  CHECK(false) << "TODO: call scheduler in the scheduler-only benchmark "
                  "driver.";
}

}  // namespace app
}  // namespace nexus
