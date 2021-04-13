#ifndef NEXUS_DISPATCHER_MODEL_WORKER_H_
#define NEXUS_DISPATCHER_MODEL_WORKER_H_

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

#include "ario/ario.h"
#include "nexus/common/rdma_sender.h"
#include "nexus/dispatcher/global_id_issuer.h"
#include "nexus/dispatcher/rankmt_scheduler.h"

namespace nexus {
namespace dispatcher {

class ModelWorker {
 public:
  ModelWorker(std::optional<int> pin_cpu, std::string rdma_dev,
              uint16_t tcp_port, GlobalIdIssuer* global_id_issuer);
  ~ModelWorker();
  ario::EpollExecutor* executor() { return &executor_; }
  uint16_t tcp_port() const { return tcp_port_; }

  void Start();
  void Stop();
  void Join();

  void AddModelSession(std::string model_session_id,
                       MultiThreadRankScheduler::RequestEntrance entrance);

 private:
  class ModelWorkerRdmaHandler;

  void HandleDispatch(DispatchRequest&& request, DispatchReply* reply,
                      long dispatcher_recv_ns);

  std::optional<int> pin_cpu_;
  std::string rdma_dev_;
  uint16_t tcp_port_;
  GlobalIdIssuer& global_id_issuer_;

  std::atomic<bool> stop_ = false;
  ario::EpollExecutor executor_;
  std::unique_ptr<ModelWorkerRdmaHandler> rdma_handler_;
  ario::MemoryBlockAllocator small_buffers_;
  ario::RdmaManager rdma_;
  RdmaSender rdma_sender_;
  std::thread ev_thread_;

  std::unordered_map<std::string, MultiThreadRankScheduler::RequestEntrance>
      model_session_entrance_table_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
