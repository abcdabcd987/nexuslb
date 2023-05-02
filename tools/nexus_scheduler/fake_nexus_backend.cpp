#include "nexus_scheduler/fake_nexus_backend.h"

#include <glog/logging.h>

#include "nexus/common/model_db.h"
#include "nexus/common/model_def.h"
#include "nexus/common/time_util.h"
#include "nexus_scheduler/fake_nexus_frontend.h"
#include "nexus_scheduler/query_collector.h"

namespace nexus {
namespace backend {

FakeNexusBackend::FakeNexusBackend(
    boost::asio::io_context* io_context, const FakeObjectAccessor* accessor,
    uint32_t node_id, const std::vector<ModelSession>& model_sessions)
    : io_context_(*io_context),
      accessor_(*accessor),
      node_id_(node_id),
      exec_timer_(io_context_) {
  for (const auto& model_sess : model_sessions) {
    // Load new model instance
    auto* profile = CHECK_NOTNULL(ModelDatabase::Singleton().GetModelProfile(
        "FakeGPU", "FakeUUID", ModelSessionToProfileID(model_sess)));
    auto model = std::make_shared<ModelExecutor>(model_sess, *profile);
    std::string session_id = ModelSessionToString(model_sess);
    model_table_.emplace(session_id, model);
    models_.push_back(model);
  }
}

void FakeNexusBackend::Start() { StartExecution(); }

void FakeNexusBackend::Stop() { exec_timer_.cancel(); }

void FakeNexusBackend::EnqueueQuery(size_t model_idx, const QueryProto& query) {
  models_.at(model_idx)->AddQuery(query);
}

void FakeNexusBackend::UpdateModelTable(const ModelTableConfig& request) {
  // Start to update model table
  // Add new models and update model batch size
  for (auto config : request.model_instance_config()) {
    // Regular model session
    auto model_sess = config.model_session(0);
    std::string session_id = ModelSessionToString(model_sess);
    auto model = model_table_.at(session_id);
    if (model->batch() != config.batch()) {
      // Update the batch size
      LOG(INFO) << "Update model instance " << session_id
                << ", batch: " << model->batch() << " -> " << config.batch();
      model->SetBatch(config.batch());
    }
  }
}

void FakeNexusBackend::StartExecution() {
  CHECK(!exec_.has_value());
  exec_ = ExecContext{};
  ContinueExecution();
}

void FakeNexusBackend::ContinueExecution() {
  CHECK(exec_.has_value());
  if (exec_->model_idx < models_.size()) {
    auto& model = models_.at(exec_->model_idx);
    exec_->batch = model->GetBatchTaskSlidingWindow(model->batch());
    auto batchsize = exec_->batch.inputs.size();
    double latency_us = 0;
    if (batchsize > 0) {
      latency_us = model->profile()->GetForwardLatency(batchsize);
    }
    exec_->exec_cycle_us += latency_us;

    auto exec_at = Clock::now();
    exec_timer_.expires_at(exec_at + std::chrono::nanoseconds(
                                         static_cast<long>(latency_us * 1e3)));
    exec_timer_.async_wait([this, exec_at](boost::system::error_code ec) {
      if (ec) return;
      CHECK(exec_.has_value());
      auto finish_at = exec_timer_.expires_at();
      auto finish_ns = finish_at.time_since_epoch().count();
      for (const auto& q : exec_->batch.inputs) {
        accessor_.query_collector->GotSuccessReply(exec_->model_idx, q.query_id,
                                                   finish_ns);
      }
      for (const auto& q : exec_->batch.drops) {
        accessor_.query_collector->GotDroppedReply(exec_->model_idx,
                                                   q.query_id);
      }
      if (!exec_->batch.inputs.empty()) {
        auto p = ExecutionHistoryEntry{
            .model_idx = static_cast<int>(exec_->model_idx),
            .batch_size = static_cast<int>(exec_->batch.inputs.size()),
            .exec_at = exec_at,
            .finish_at = finish_at,
        };
        for (const auto& q : exec_->batch.inputs) {
          p.query_ids.push_back(q.query_id);
        }
        exec_history_.push_back(p);
      }

      ++exec_->model_idx;
      ContinueExecution();
    });
  } else {
    double min_cycle_us = 50.;
    double wait_us = std::max(0.0, min_cycle_us - exec_->exec_cycle_us);
    exec_.reset();

    exec_timer_.expires_from_now(
        std::chrono::microseconds(static_cast<long>(wait_us)));
    exec_timer_.async_wait([this](boost::system::error_code ec) {
      if (ec) return;
      StartExecution();
    });
  }
}

}  // namespace backend
}  // namespace nexus
