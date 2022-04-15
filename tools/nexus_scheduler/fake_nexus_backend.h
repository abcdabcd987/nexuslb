#pragma once
#include <boost/asio.hpp>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "nexus/proto/nexus.pb.h"
#include "nexus_scheduler/fake_object_accessor.h"
#include "nexus_scheduler/gpu_executor.h"

namespace nexus {

class FakeNexusBackend {
 public:
  FakeNexusBackend(boost::asio::io_context* io_context, uint32_t node_id,
                   const FakeObjectAccessor* accessor);
  uint32_t node_id() const { return node_id_; }

  void UpdateModelTable(const ModelTableConfig& request);

 private:
  boost::asio::io_context& io_context_;
  uint32_t node_id_;
  const FakeObjectAccessor& accessor_;
  std::mutex model_table_mu_;
  std::unordered_map<std::string, backend::ModelExecutorPtr> model_table_;
  std::unique_ptr<backend::GpuExecutorMultiBatching> gpu_executor_;
};

}  // namespace nexus
