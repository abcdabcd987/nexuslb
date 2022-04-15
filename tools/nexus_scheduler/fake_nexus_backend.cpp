#include "nexus_scheduler/fake_nexus_backend.h"

#include <glog/logging.h>

#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"

namespace nexus {

FakeNexusBackend::FakeNexusBackend(boost::asio::io_context* io_context,
                                   uint32_t node_id,
                                   const FakeObjectAccessor* accessor)
    : io_context_(*io_context), node_id_(node_id), accessor_(*accessor) {}

void FakeNexusBackend::UpdateModelTable(const ModelTableConfig& request) {
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
      auto* profile = CHECK_NOTNULL(ModelDatabase::Singleton().GetModelProfile(
          "FakeGPU", "FakeUUID", ModelSessionToProfileID(model_sess)));
      auto model =
          std::make_shared<backend::ModelExecutor>(model_sess, *profile);
      model_table_.emplace(session_id, model);
      gpu_executor_->AddModel(model);
      LOG(INFO) << "Load model instance " << session_id
                << ", batch: " << config.batch();
    } else {
      auto model = model_iter->second;
      if (model->batch() != config.batch()) {
        // Update the batch size
        LOG(INFO) << "Update model instance " << session_id
                  << ", batch: " << model->batch() << " -> " << config.batch();
        model->SetBatch(config.batch());
      }
    }
  }

  // Update duty cycle
  gpu_executor_->SetDutyCycle(request.duty_cycle_us());
  LOG(INFO) << "Duty cycle: " << request.duty_cycle_us() << " us";
}

}  // namespace nexus
