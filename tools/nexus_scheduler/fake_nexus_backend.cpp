#include "nexus_scheduler/fake_nexus_backend.h"

#include <glog/logging.h>

#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"

namespace nexus {

FakeNexusBackend::FakeNexusBackend(const BackendInfo& info)
    : node_id_(info.node_id()) {}

void FakeNexusBackend::UpdateModelTable(const ModelTableConfig& request) {
  // Update backend pool
  std::unordered_set<uint32_t> backend_list;
  std::unordered_map<uint32_t, BackendInfo> backend_infos;
  for (auto config : request.model_instance_config()) {
    for (auto const& info : config.backup_backend()) {
      backend_list.insert(info.node_id());
      backend_infos.emplace(info.node_id(), info);
    }
  }

  // Count all sessions in model table
  std::unordered_set<std::string> all_sessions;
  for (auto const& config : request.model_instance_config()) {
    for (auto const& model_sess : config.model_session()) {
      all_sessions.insert(ModelSessionToString(model_sess));
    }
  }

  // Start to update model table
  std::lock_guard<std::mutex> lock(model_table_mu_);
  // Remove unused model instances
  std::vector<std::string> to_remove;
  for (auto iter : model_table_) {
    if (all_sessions.count(iter.first) == 0) {
      to_remove.push_back(iter.first);
    }
  }
  for (auto session_id : to_remove) {
    auto model = model_table_.at(session_id);
    model_table_.erase(session_id);
    LOG(INFO) << "Remove model instance " << session_id;
    gpu_executor_->RemoveModel(model);
  }

  // Add new models and update model batch size
  for (auto config : request.model_instance_config()) {
    // Regular model session
    auto model_sess = config.model_session(0);
    std::string session_id = ModelSessionToString(model_sess);
    auto model_iter = model_table_.find(session_id);
    if (model_iter == model_table_.end()) {
      // Load new model instance
      auto model = std::make_shared<backend::ModelExecutor>(gpu_id_, config,
                                                            task_queue_);
      model_table_.emplace(session_id, model);
      gpu_executor_->AddModel(model);
      LOG(INFO) << "Load model instance " << session_id
                << ", batch: " << config.batch()
                << ", backup: " << config.backup();
    } else {
      auto model = model_iter->second;
      if (model->model()->batch() != config.batch()) {
        // Update the batch size
        LOG(INFO) << "Update model instance " << session_id
                  << ", batch: " << model->model()->batch() << " -> "
                  << config.batch();
        model->SetBatch(config.batch());
      }
      model->UpdateBackupBackends(config);
    }
  }

  // Update duty cycle
  gpu_executor_->SetDutyCycle(request.duty_cycle_us());
  LOG(INFO) << "Duty cycle: " << request.duty_cycle_us() << " us";
}

std::shared_ptr<FakeNexusBackend> FakeNexusBackendPool::GetBackend(
    uint32_t backend_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto iter = backends_.find(backend_id);
  if (iter == backends_.end()) {
    return nullptr;
  }
  return iter->second;
}

void FakeNexusBackendPool::AddBackend(
    std::shared_ptr<FakeNexusBackend> backend) {
  std::lock_guard<std::mutex> lock(mu_);
  backends_.emplace(backend->node_id(), backend);
}

void FakeNexusBackendPool::RemoveBackend(
    std::shared_ptr<FakeNexusBackend> backend) {
  std::lock_guard<std::mutex> lock(mu_);
  backends_.erase(backend->node_id());
}

void FakeNexusBackendPool::RemoveBackend(uint32_t backend_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto iter = backends_.find(backend_id);
  if (iter == backends_.end()) {
    return;
  }
  backends_.erase(iter);
}

}  // namespace nexus
