#include "nexus_scheduler/scheduler.h"

#include <glog/logging.h>

#include <cmath>
#include <unordered_set>

#include "nexus/common/config.h"
#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"

namespace nexus {
namespace scheduler {

Scheduler::Config Scheduler::Config::Default() {
  Config cfg;
  cfg.beacon = 1;
  cfg.epoch = 30;
  cfg.min_epoch = 10;
  cfg.avg_interval = 10;
  return cfg;
}

Scheduler::Scheduler(boost::asio::io_context* io_context,
                     const FakeObjectAccessor* accessor, const Config& config)
    : io_context_(*io_context),
      accessor_(*accessor),
      epoch_schedule_timer_(io_context_),
      beacon_interval_sec_(config.beacon),
      epoch_interval_sec_(config.epoch),
      min_epoch_(config.min_epoch),
      avg_interval_(config.avg_interval) {
  history_len_ =
      (avg_interval_ * 3 + beacon_interval_sec_ - 1) / beacon_interval_sec_;
}

void Scheduler::Start() { CheckEpochSchedule(); }

void Scheduler::Stop() { epoch_schedule_timer_.cancel(); }

void Scheduler::LoadWorkload(const YAML::Node& config) {
  // Load static workload configuration
  for (uint i = 0; i < config.size(); ++i) {
    const YAML::Node& backend_workload = config[i];
    LOG(INFO) << "Backend " << i << ":";
    std::vector<YAML::Node> models;
    for (uint j = 0; j < backend_workload.size(); ++j) {
      LOG(INFO) << "- " << backend_workload[j];
      models.push_back(backend_workload[j]);
    }
    static_workloads_.push_back(models);
  }
}

void Scheduler::CheckEpochSchedule() {
  auto now = Clock::now();
  auto elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_epoch_schedule_)
                    .count();
  bool trigger = BeaconCheck();
  if (trigger) {
    if (elapse >= avg_interval_ * 1000) {
      EpochSchedule();
      last_epoch_schedule_ = Clock::now();
    }
  } else {
    if (elapse >= epoch_interval_sec_ * 1000) {
      EpochSchedule();
      last_epoch_schedule_ = Clock::now();
    }
  }

  epoch_schedule_timer_.expires_from_now(
      std::chrono::seconds(beacon_interval_sec_));
  epoch_schedule_timer_.async_wait([this](boost::system::error_code ec) {
    if (ec) return;
    CheckEpochSchedule();
  });
}

void Scheduler::LoadModel(const LoadModelRequest& request,
                          NexusLoadModelReply* reply) {
  ModelSession model_sess(request.model_session());
  std::string model_sess_id = ModelSessionToString(model_sess);
  double workload = request.estimate_workload();

  std::lock_guard<std::mutex> lock(mutex_);
  auto frontend = GetFrontend(request.node_id());
  if (frontend == nullptr) {
    reply->set_status(CTRL_SERVER_NOT_REGISTERED);
    return;
  }
  auto session_iter = session_table_.find(model_sess_id);
  if (session_iter != session_table_.end()) {
    // TODO: For now, if model session is already loaded, don't allocate
    // new backends, just rely on epoch scheduling
    reply->set_status(CTRL_OK);
    GetModelRoute(model_sess_id, reply->mutable_model_route());
    session_iter->second->SubscribeModelSession(frontend->node_id(),
                                                model_sess_id, avg_interval_);
    return;
  }

  // Find best-fit backends to serve the workload
  std::vector<std::pair<BackendDelegatePtr, InstanceInfo> > assign_backends;
  std::unordered_set<uint32_t> used;
  if (workload == 0) {
    BackendDelegatePtr backend;
    InstanceInfo inst_info;
    FindBestBackend(model_sess, workload, used, &backend, &inst_info);
    if (backend == nullptr) {
      reply->set_status(NOT_ENOUGH_BACKENDS);
      return;
    }
    assign_backends.emplace_back(backend, inst_info);
  } else {
    while (workload > 1e-3) {
      BackendDelegatePtr backend;
      InstanceInfo inst_info;
      FindBestBackend(model_sess, workload, used, &backend, &inst_info);
      if (backend == nullptr) {
        reply->set_status(NOT_ENOUGH_BACKENDS);
        return;
      }
      assign_backends.emplace_back(backend, inst_info);
      used.insert(backend->node_id());
      workload -= inst_info.throughput;
    }
  }

  // Load models
  auto session_info = std::make_shared<SessionInfo>();
  for (auto iter : assign_backends) {
    auto backend = iter.first;
    auto const& inst_info = iter.second;
    backend->LoadModel(inst_info);
    backend->UpdateModelTableRpc();
    session_info->backend_weights.emplace(backend->node_id(),
                                          inst_info.GetWeight());
  }
  session_info->model_sessions.push_back(model_sess);
  session_table_.emplace(model_sess_id, session_info);
  session_info->SubscribeModelSession(frontend->node_id(), model_sess_id,
                                      avg_interval_);

  // Fill route table in the reply
  reply->set_status(CTRL_OK);
  GetModelRoute(model_sess_id, reply->mutable_model_route());
}

void Scheduler::ReportWorkload(const WorkloadStatsProto& request) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto frontend = GetFrontend(request.node_id());
  CHECK(frontend != nullptr) << "CTRL_SERVER_NOT_REGISTERED";
  for (auto const& model_stats : request.model_stats()) {
    auto session_info = session_table_.at(model_stats.model_session_id());
    session_info->UpdateWorkload(request.node_id(), model_stats);
  }
}

void Scheduler::RegisterFrontend(FrontendDelegatePtr frontend) {
  // lock protected
  std::lock_guard<std::mutex> lock(mutex_);
  CHECK(frontends_.find(frontend->node_id()) == frontends_.end());
  // add the frontend client in the frontend map
  frontends_[frontend->node_id()] = frontend;
}

void Scheduler::RegisterBackend(BackendDelegatePtr backend) {
  std::lock_guard<std::mutex> lock(mutex_);
  CHECK(backends_.find(backend->node_id()) == backends_.end());
  // Add the backend client in the backend map
  backends_[backend->node_id()] = backend;

  // Update workload to new backend
  AddBackend(backend);
}

void Scheduler::AddBackend(BackendDelegatePtr backend) {
  std::unordered_set<SessionInfoPtr> changed_sessions;
  std::unordered_set<BackendDelegatePtr> changed_backends;

  // 1. Check if there is any static configured workload to assign
  int assign_load_id = -1;
  for (uint id = 0; id < static_workloads_.size(); ++id) {
    if (assigned_static_workloads_.find(id) ==
        assigned_static_workloads_.end()) {
      assign_load_id = id;
      assigned_static_workloads_[id] = backend->node_id();
      break;
    }
  }
  if (assign_load_id >= 0) {
    // Assign static workload
    LOG(INFO) << "Assign workload " << assign_load_id << " to backend "
              << backend->node_id();
    auto workload = static_workloads_[assign_load_id];
    for (auto session_info : workload) {
      backend->LoadModel(session_info);
    }
    backend->set_workload_id(assign_load_id);
    changed_backends.insert(backend);

    // Update session info
    for (auto const& model_sess_id : backend->GetModelSessions()) {
      if (session_table_.find(model_sess_id) == session_table_.end()) {
        auto session_info = std::make_shared<SessionInfo>();
        session_info->has_static_workload = true;
        ModelSession model_sess;
        ParseModelSession(model_sess_id, &model_sess);
        session_info->model_sessions.push_back(model_sess);
        session_table_.emplace(model_sess_id, session_info);
      }
      auto session_info = session_table_.at(model_sess_id);
      session_info->backend_weights.emplace(
          backend->node_id(), backend->GetModelWeight(model_sess_id));
      changed_sessions.insert(session_info);
    }
  } else {
    // 2. Check if there are unassigned workloads
    AllocateUnassignedWorkloads(&changed_sessions, &changed_backends);
    for (auto session : changed_sessions) {
      LOG(INFO) << "Changed session: "
                << ModelSessionToString(session->model_sessions[0]);
    }
  }

  // 3. Update backend model table
  for (auto b : changed_backends) {
    b->UpdateModelTableRpc();
  }

  // 4. Update model info and route
  UpdateModelRoutes(changed_sessions);
}

BackendDelegatePtr Scheduler::GetBackend(uint32_t node_id) {
  auto iter = backends_.find(node_id);
  if (iter == backends_.end()) {
    LOG(ERROR) << "Cannot find backend " << node_id;
    return nullptr;
  }
  return iter->second;
}

FrontendDelegatePtr Scheduler::GetFrontend(uint32_t node_id) {
  auto iter = frontends_.find(node_id);
  if (iter == frontends_.end()) {
    LOG(ERROR) << "Cannot find frontend " << node_id;
    return nullptr;
  }
  return iter->second;
}

void Scheduler::GetModelRoute(const std::string& model_sess_id,
                              ModelRouteProto* route) {
  route->set_model_session_id(model_sess_id);
  for (auto iter : session_table_.at(model_sess_id)->backend_weights) {
    auto backend_rate = route->add_backend_rate();
    backends_.at(iter.first)->GetInfo(backend_rate->mutable_info());
    backend_rate->set_throughput(iter.second);
  }
}

void Scheduler::FindBestBackend(const ModelSession& model_sess,
                                double request_rate,
                                const std::unordered_set<uint32_t>& skips,
                                BackendDelegatePtr* best_backend,
                                InstanceInfo* inst_info) {
  using ModelLoad = std::tuple<BackendDelegatePtr, InstanceInfo, double>;
  ModelLoad max_tp_load;
  ModelLoad max_occ_load;
  for (auto iter : backends_) {
    auto backend = iter.second;
    if (skips.find(backend->node_id()) != skips.end()) {
      continue;
    }
    if (backend->workload_id() >= 0) {
      continue;
    }
    if (std::fabs(request_rate) < 1e-3 && !backend->IsIdle()) {
      continue;
    }
    InstanceInfo tmp_info;
    double occupancy;
    bool ret = backend->PrepareLoadModel(model_sess, request_rate, &tmp_info,
                                         &occupancy);
    if (!ret) {
      continue;
    }
    if (std::get<0>(max_tp_load) == nullptr ||
        tmp_info.throughput > std::get<1>(max_tp_load).throughput) {
      max_tp_load = std::make_tuple(backend, tmp_info, occupancy);
    }
    if (std::get<0>(max_occ_load) == nullptr ||
        occupancy > std::get<2>(max_occ_load)) {
      max_occ_load = std::make_tuple(backend, tmp_info, occupancy);
    }
  }
  if (std::fabs(request_rate) < 1e-3) {
    // for request rate = 0, return backend that provides highest throughput
    *best_backend = std::get<0>(max_tp_load);
    *inst_info = std::get<1>(max_tp_load);
  } else if (std::get<1>(max_tp_load).throughput < request_rate) {
    // If no backend can achieve request rate, return backend that provides
    // highest throughput
    *best_backend = std::get<0>(max_tp_load);
    *inst_info = std::get<1>(max_tp_load);
  } else {
    // Otherwise, return backend that has highest occupancy
    *best_backend = std::get<0>(max_occ_load);
    *inst_info = std::get<1>(max_occ_load);
  }
}

bool Scheduler::BeaconCheck() {
  std::lock_guard<std::mutex> lock(mutex_);
  // 1. Remove dead frontends

  // 2. Aggregate model session rps
  for (auto iter : session_table_) {
    const auto& model_sess_id = iter.first;
    auto session_info = iter.second;
    double rps = 0.;
    for (auto const& wk_iter : session_info->workloads) {
      rps += std::max(0., wk_iter.second->rate());
    }
    if (session_info->rps_history.size() > 0 || rps > 0) {
      // Don't push 0 in the begining
      session_info->rps_history.push_back(rps);
    }
    if (session_info->rps_history.size() > history_len_) {
      session_info->rps_history.pop_front();
    }
    VLOG(2) << "Model " << model_sess_id << " rps: " << rps
            << " req/s (avg over " << avg_interval_ << " seconds)";
  }

  // 3. Remove dead backend

  // 4. Check if need to trigger epoch schedule
  bool trigger = false;
  for (auto iter : session_table_) {
    const auto& model_sess_id = iter.first;
    auto session_info = iter.second;
    if (session_info->rps_history.size() < history_len_) {
      continue;
    }
    double estimate_rps =
        std::max(session_info->rps_history[history_len_ - 1], 0.1);
    double throughput = session_info->TotalThroughput();
    if (estimate_rps < throughput * 0.8 || estimate_rps > throughput * 1.1) {
      trigger = true;
      break;
    }
  }
  return trigger;
}

void Scheduler::EpochSchedule() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::unordered_set<std::shared_ptr<SessionInfo> > visited;
  std::unordered_set<std::shared_ptr<SessionInfo> > changed_sessions;
  std::vector<BackendDelegatePtr> overload_backends;

  VLOG(1) << "Epoch schedule";
  // 1. Adjust the GPU allocation based on the workload
  for (auto iter : session_table_) {
    auto const& model_sess_id = iter.first;
    auto session_info = iter.second;
    if (visited.count(session_info) > 0) {
      continue;
    }
    visited.insert(session_info);
    double throughput = session_info->TotalThroughput();
    // Compute the workload mean and std
    uint32_t n = session_info->rps_history.size();
    if (n < history_len_) {
      continue;
    }
    double rps_mean = 0., rps_std = 0.;
    for (double rps : session_info->rps_history) {
      rps_mean += rps;
    }
    rps_mean /= n;
    for (double rps : session_info->rps_history) {
      rps_std += (rps - rps_mean) * (rps - rps_mean);
    }
    rps_std = std::sqrt(rps_std / (n - 1));
    // double estimate_rps = std::max(
    //     session_info->rps_history[n - 1] + rps_std, 0.1);
    // double estimate_rps = std::max(rps_mean + rps_std, 0.1);
    double estimate_rps = std::max(session_info->rps_history[n - 1], 0.1);
    session_info->unassigned_workload = std::max(0., estimate_rps - throughput);
    VLOG(1) << model_sess_id << " estimate rps: " << estimate_rps
            << " (last: " << session_info->rps_history[n - 1]
            << ", mean: " << rps_mean << ", std: " << rps_std
            << "), throughput: " << throughput;

    if (estimate_rps < throughput * 0.97) {
      // Workload is smaller than throughput, can release some GPUs
      std::vector<std::pair<uint32_t, double> > adjust_backends;
      // Backends with static configured workload are still fixed
      for (auto iter : session_info->backend_weights) {
        auto backend = backends_.at(iter.first);
        if (backend->workload_id() >= 0) {
          estimate_rps -= iter.second;
        } else {
          adjust_backends.push_back(iter);
        }
      }
      // Sort the backends based on throughput
      std::sort(
          adjust_backends.begin(), adjust_backends.end(),
          [](std::pair<uint32_t, double> a, std::pair<uint32_t, double> b) {
            return a.second > b.second;
          });
      for (auto iter : adjust_backends) {
        if (estimate_rps < 1e-3) {
          auto backend = backends_.at(iter.first);
          backend->UnloadModel(model_sess_id);
          session_info->backend_weights.erase(iter.first);
        } else if (iter.second > estimate_rps) {
          auto backend = backends_.at(iter.first);
          double new_tp =
              backend->UpdateModelThroughput(model_sess_id, estimate_rps);
          session_info->backend_weights[iter.first] =
              backend->GetModelWeight(model_sess_id);
          estimate_rps -= new_tp;
        } else {
          estimate_rps -= iter.second;
        }
      }
      changed_sessions.insert(session_info);
    } else if (estimate_rps > throughput) {
      // Workload is larger than throughput, need to allocate more gpus
      std::vector<std::pair<uint32_t, double> > adjust_backends;
      // Backends with static configured workload are still fix
      for (auto iter : session_info->backend_weights) {
        auto backend = backends_.at(iter.first);
        if (backend->workload_id() >= 0) {
          estimate_rps -= iter.second;
        } else {
          adjust_backends.push_back(iter);
        }
      }
      // Second sort the backends based on throughput in descending order
      std::sort(
          adjust_backends.begin(), adjust_backends.end(),
          [](std::pair<uint32_t, double> a, std::pair<uint32_t, double> b) {
            return a.second > b.second;
          });
      for (auto iter : adjust_backends) {
        if (estimate_rps < 1e-3) {
          auto backend = backends_.at(iter.first);
          backend->UnloadModel(model_sess_id);
          session_info->backend_weights.erase(iter.first);
        } else {
          auto backend = backends_.at(iter.first);
          double new_tp =
              backend->UpdateModelThroughput(model_sess_id, estimate_rps);
          session_info->backend_weights[iter.first] =
              backend->GetModelWeight(model_sess_id);
          estimate_rps -= new_tp;
          if (backend->overload() && backend->Occupancy() > 1.05) {
            overload_backends.push_back(backend);
          }
        }
      }
      if (estimate_rps > 1e-3) {
        session_info->unassigned_workload = estimate_rps;
      } else {
        session_info->unassigned_workload = 0;
      }
      changed_sessions.insert(session_info);
    }
  }

  // 2. Adjust overloaded backends
  for (auto backend : overload_backends) {
    std::vector<std::pair<SessionGroup, double> > spillout;
    backend->SpillOutWorkload(&spillout);
    for (auto iter : spillout) {
      auto model_sess_id = ModelSessionToString(iter.first[0]);
      auto session_info = session_table_.at(model_sess_id);
      session_info->backend_weights.erase(backend->node_id());
      session_info->unassigned_workload += iter.second;
    }
  }

  // 3. Consolidate low utilization backends
  // ConsolidateBackends(&changed_sessions);

  // 4. Allocate the unassigned workloads to backends that still have space
  AllocateUnassignedWorkloads(&changed_sessions);

  // 5. Update model table to backends and model routes to frontends
  for (auto iter : backends_) {
    iter.second->UpdateModelTableRpc();
  }
  UpdateModelRoutes(changed_sessions);

  DisplayModelTable();
}

void Scheduler::AllocateUnassignedWorkloads(
    std::unordered_set<SessionInfoPtr>* changed_sessions,
    std::unordered_set<BackendDelegatePtr>* changed_backends) {
  // Sort unassigned workloads by request rate
  std::vector<SessionInfoPtr> unassigned_workloads;
  std::unordered_set<SessionInfoPtr> visited;
  for (auto iter : session_table_) {
    auto session_info = iter.second;
    if (visited.count(session_info) > 0) {
      continue;
    }
    visited.insert(session_info);
    if (session_info->unassigned_workload > 1e-3) {
      VLOG(1) << iter.first << " has unassigned workload "
              << session_info->unassigned_workload;
      unassigned_workloads.emplace_back(session_info);
    }
  }
  if (unassigned_workloads.empty()) {
    return;
  }
  std::sort(unassigned_workloads.begin(), unassigned_workloads.end(),
            [](SessionInfoPtr a, SessionInfoPtr b) {
              return a->unassigned_workload > b->unassigned_workload;
            });
  for (auto session_info : unassigned_workloads) {
    double request_rate = session_info->unassigned_workload;
    auto const& sessions = session_info->model_sessions;
    // LOG(INFO) << "Try to assign workload " << model_sess_id << ", " <<
    //     request_rate << " req/s";
    while (request_rate > 1e-3) {
      BackendDelegatePtr backend;
      InstanceInfo inst_info;
      FindBestBackend(sessions[0], request_rate, {}, &backend, &inst_info);
      if (backend == nullptr) {
        std::string model_sess_id = ModelSessionToString(sessions[0]);
        LOG(INFO) << "Unassigned workload " << model_sess_id << ", "
                  << request_rate << " req/s";
        break;
      }
      request_rate -= inst_info.throughput;
      backend->LoadModel(inst_info);
      CHECK_EQ(sessions.size(), 1) << "PrefixModel not supported";
      session_info->backend_weights.emplace(backend->node_id(),
                                            inst_info.GetWeight());
      changed_sessions->insert(session_info);
      if (changed_backends != nullptr) {
        changed_backends->insert(backend);
      }
    }
    if (std::fabs(request_rate) < 1e-3) request_rate = 0.;
    session_info->unassigned_workload = std::max(0., request_rate);
  }
}

void Scheduler::ConsolidateBackends(
    std::unordered_set<SessionInfoPtr>* changed_sessions) {
  std::vector<BackendDelegatePtr> backends;
  std::unordered_set<uint32_t> skip_backends;
  for (auto iter : backends_) {
    auto backend = iter.second;
    if (backend->Occupancy() == 0) {
      skip_backends.insert(backend->node_id());
    } else {
      backends.push_back(backend);
    }
  }
  while (!backends.empty()) {
    std::sort(backends.begin(), backends.end(),
              [](const BackendDelegatePtr& a, const BackendDelegatePtr& b) {
                return a->Occupancy() > b->Occupancy();
              });
    auto backend = backends.back();
    backends.pop_back();
    skip_backends.insert(backend->node_id());
    bool full = false;
    // changed_backends->insert(backend);
    for (auto inst_info : backend->GetModels()) {
      auto const& model_sess = inst_info->model_sessions[0];
      std::string model_sess_id = ModelSessionToString(model_sess);
      BackendDelegatePtr assign;
      InstanceInfo new_inst_info;
      FindBestBackend(model_sess, inst_info->workload, skip_backends, &assign,
                      &new_inst_info);
      if (assign == nullptr) {
        full = true;
        break;
      }
      backend->UnloadModel(model_sess_id);
      assign->LoadModel(new_inst_info);
      CHECK_EQ(inst_info->model_sessions.size(), 1)
          << "PrefixModel not supported";
      // changed_backends->insert(assign);
      auto session_info = session_table_.at(model_sess_id);
      session_info->backend_weights.erase(backend->node_id());
      session_info->backend_weights.emplace(assign->node_id(),
                                            new_inst_info.GetWeight());
      changed_sessions->insert(session_info);
      LOG(INFO) << "Move model " << model_sess_id << " from "
                << backend->node_id() << " to " << assign->node_id();
    }
    if (full) {
      break;
    }
  }
}

void Scheduler::UpdateModelRoutes(std::unordered_set<SessionInfoPtr> sessions) {
  std::unordered_map<uint32_t, ModelRouteUpdates> frontend_updates;
  for (auto session_info : sessions) {
    for (auto const& iter : session_info->session_subscribers) {
      for (auto frontend_id : iter.second) {
        if (frontend_updates.find(frontend_id) == frontend_updates.end()) {
          frontend_updates.emplace(frontend_id, ModelRouteUpdates());
        }
        GetModelRoute(iter.first,
                      frontend_updates.at(frontend_id).add_model_route());
      }
    }
  }
  for (auto iter : frontend_updates) {
    auto frontend = GetFrontend(iter.first);
    if (frontend != nullptr) {
      frontend->UpdateModelRoutesRpc(iter.second);
    }
  }
}

void Scheduler::DisplayModelTable() {
  std::unordered_set<uint32_t> used_backends;
  std::stringstream ss;
  for (auto iter : backends_) {
    auto backend = iter.second;
    double occ = backend->Occupancy();
    if (occ > 0) {
      used_backends.insert(backend->node_id());
      ss << "  Backend " << backend->node_id() << ": " << occ << "\n";
    }
  }
  if (!used_backends.empty()) {
    LOG(INFO) << "Total used GPUs: " << used_backends.size();
    VLOG(1) << "GPU occupancy:\n" << ss.str();
    std::stringstream ss1;
    for (auto iter : session_table_) {
      auto const& model_sess_id = iter.first;
      auto session_info = iter.second;
      double total_gpu_share = 0.;
      ss1 << "  " << model_sess_id << ":";
      for (auto backend_iter : session_info->backend_weights) {
        auto backend = GetBackend(backend_iter.first);
        double share = backend->GetModelGPUShare(model_sess_id);
        total_gpu_share += share;
        ss1 << " " << backend_iter.first << "/" << backend_iter.second << "/"
            << share;
        used_backends.insert(backend_iter.first);
      }
      ss1 << ", total share: " << total_gpu_share << "\n";
    }
    LOG(INFO) << "Model table: \n" << ss1.str();
  }
}

}  // namespace scheduler
}  // namespace nexus
