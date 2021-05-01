#include "nexus/backend/task.h"

#include <glog/logging.h>

#include "nexus/backend/model_exec.h"
#include "nexus/backend/model_ins.h"
#include "nexus/common/model_def.h"

namespace nexus {
namespace backend {

Input::Input(TimePoint deadline, uint64_t tid, int idx, ArrayPtr arr)
    : DeadlineItem(deadline), task_id(tid), index(idx), array(arr) {}

Output::Output(uint64_t tid, int idx,
               const std::unordered_map<std::string, ArrayPtr>& arrs)
    : task_id(tid), index(idx), arrays(arrs) {}

std::atomic<uint64_t> Task::global_task_id_(0);

Task::Task() : Task(nullptr, nullptr) {}

Task::Task(ario::RdmaQueuePair* conn, std::shared_ptr<ModelExecutor> model)
    : DeadlineItem(),
      connection(conn),
      model(std::move(model)),
      stage(Stage::kFetchImage),
      filled_outputs(0) {
  task_id = global_task_id_.fetch_add(1, std::memory_order_relaxed);
  timer.Record("begin");
}

void Task::SetConnection(ario::RdmaQueuePair* conn) { connection = conn; }

void Task::SetPlanId(PlanId plan_id) { this->plan_id = plan_id; }

void Task::SetQuery(QueryProto decoded_query, uint64_t rdma_read_offset,
                    uint64_t rdma_read_length) {
  CHECK(model != nullptr);
  query = std::move(decoded_query);
  this->rdma_read_offset = rdma_read_offset;
  this->rdma_read_length = rdma_read_length;
  uint32_t budget = model->model()->model_session().latency_sla();
  if (query.slack_ms() > 0) {
    budget += query.slack_ms();
    // LOG(INFO) << "slack " << query.slack_ms() << " ms";
  }
  SetDeadline(std::chrono::milliseconds(budget));
}

void Task::AppendInput(ArrayPtr arr) {
  auto input = std::make_shared<Input>(deadline(), task_id, inputs.size(), arr);
  inputs.push_back(input);
  // Put a placeholder in the outputs
  outputs.push_back(nullptr);
}

bool Task::AddOutput(std::shared_ptr<Output> output) {
  outputs[output->index] = output;
  uint32_t filled = ++filled_outputs;
  if (filled == outputs.size()) {
    return true;
  }
  return false;
}

bool Task::AddVirtualOutput(int index) {
  result.set_status(TIMEOUT);
  uint32_t filled = ++filled_outputs;
  if (filled == outputs.size()) {
    return true;
  }
  return false;
}

}  // namespace backend
}  // namespace nexus
