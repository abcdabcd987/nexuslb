#include "nexus_scheduler/model_exec.h"

#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <thread>

#include "nexus/common/model_db.h"
#include "nexus/common/time_util.h"

namespace nexus {
namespace backend {

ModelExecutor::ModelExecutor(ModelSession model_session,
                             const ModelProfile& profile)
    : model_session_(std::move(model_session)), profile_(&profile) {}

ModelExecutor::~ModelExecutor() {}

uint64_t ModelExecutor::Execute() {
  auto batch_task = GetBatchTaskSlidingWindow(batch_);

  if (batch_task.inputs.size() == 0) {
    return 0;
  }

  long latency_us = profile_->GetForwardLatency(batch_task.inputs.size());
  std::this_thread::sleep_for(std::chrono::microseconds(latency_us));

  // TODO: return results

  return latency_us;
}

ModelExecutor::GetBatchResult ModelExecutor::GetBatchTaskSlidingWindow(
    uint32_t expect_batch_size) {
  GetBatchResult ret;
  if (expect_batch_size > input_queue_.size()) {
    expect_batch_size = input_queue_.size();
  }
  if (expect_batch_size == 0) {
    return ret;
  }

  std::lock_guard<std::mutex> lock(task_mu_);
  TimePoint now = Clock::now();
  float latency = profile_->GetForwardLatency(expect_batch_size);
  TimePoint finish = now + std::chrono::microseconds(int(latency));
  while (ret.inputs.size() < expect_batch_size && !input_queue_.empty()) {
    auto input = std::move(input_queue_.top());
    input_queue_.pop();
    if (input.deadline_ns < finish.time_since_epoch().count()) {
      ret.drops.push_back(input);
    } else {
      ret.inputs.push_back(input);
    }
    // Check whether there is enough requests left to fill the batch size
    int est_max_batch = ret.inputs.size() + input_queue_.size();
    if (expect_batch_size > est_max_batch) {
      expect_batch_size = est_max_batch;
      if (expect_batch_size > 0) {
        float latency = profile_->GetForwardLatency(expect_batch_size);
        finish = now + std::chrono::microseconds(int(latency));
      }
    }
  }
  return ret;
}

void ModelExecutor::AddQuery(const QueryProto& query) {
  int64_t deadline_ns =
      query.clock().frontend_recv_ns() + model_session_.latency_sla() * 1000000;
  Query q{query.frontend_id(), query.query_id(), deadline_ns};
  std::lock_guard lock(task_mu_);
  input_queue_.push(q);
}

}  // namespace backend
}  // namespace nexus
