#ifndef NEXUS_BACKEND_WORKER_H_
#define NEXUS_BACKEND_WORKER_H_

#include <memory>
#include <string>
#include <thread>

#include "nexus/backend/task.h"
#include "nexus/common/block_queue.h"
#include "nexus/common/rdma_sender.h"

namespace nexus {
namespace backend {

class BackendServer;

class Worker {
 public:
  Worker(int index, BackendServer* server, RdmaSender rdma_sender,
         BlockPriorityQueue<Task>& task_queue);

  void Start(int core = -1);

  void Stop();

  void Run();

 private:
  void Process(std::shared_ptr<Task> task);

  void SendReply(std::shared_ptr<Task> task);

 private:
  int index_;
  BackendServer* server_;
  RdmaSender rdma_sender_;
  BlockPriorityQueue<Task>& task_queue_;
  volatile bool running_;
  std::thread thread_;
};

}  // namespace backend
}  // namespace nexus

#endif  // NEXUS_BACKEND_WORKER_H_
