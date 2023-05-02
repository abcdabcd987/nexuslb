#pragma once
#include <boost/asio.hpp>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "nexus/common/time_util.h"
#include "nexus/proto/nexus.pb.h"
#include "nexus_scheduler/fake_object_accessor.h"
#include "nexus_scheduler/model_exec.h"

namespace nexus {
namespace backend {

struct ExecutionHistoryEntry {
  int model_idx;
  int batch_size;
  TimePoint exec_at;
  TimePoint finish_at;
  std::vector<uint64_t> query_ids;
};

class FakeNexusBackend {
 public:
  FakeNexusBackend(boost::asio::io_context* io_context,
                   const FakeObjectAccessor* accessor, uint32_t node_id,
                   const std::vector<ModelSession>& model_sessions);
  uint32_t node_id() const { return node_id_; }
  const std::vector<ExecutionHistoryEntry>& batchplan_archive() const {
    return exec_history_;
  }

  void Start();
  void Stop();

  void EnqueueQuery(size_t model_idx, const QueryProto& query);

  void UpdateModelTable(const ModelTableConfig& request);

 private:
  struct ExecContext {
    double exec_cycle_us = 0;
    size_t model_idx = 0;
    GetBatchResult batch;
  };

  void StartExecution();
  void ContinueExecution();

  boost::asio::io_context& io_context_;
  const FakeObjectAccessor& accessor_;
  uint32_t node_id_;

  std::unordered_map<std::string, std::shared_ptr<ModelExecutor>> model_table_;
  std::vector<std::shared_ptr<ModelExecutor>> models_;
  std::vector<ExecutionHistoryEntry> exec_history_;
  boost::asio::system_timer exec_timer_;
  std::optional<ExecContext> exec_;
};

}  // namespace backend
}  // namespace nexus
