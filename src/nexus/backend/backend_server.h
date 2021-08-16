#ifndef NEXUS_BACKEND_BACKEND_SERVER_H_
#define NEXUS_BACKEND_BACKEND_SERVER_H_

#include <yaml-cpp/yaml.h>

#include <atomic>
#include <deque>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ario/ario.h"
#include "nexus/backend/batch_plan_context.h"
#include "nexus/backend/gpu_executor.h"
#include "nexus/backend/model_exec.h"
#include "nexus/backend/task.h"
#include "nexus/backend/worker.h"
#include "nexus/common/backend_pool.h"
#include "nexus/common/block_queue.h"
#include "nexus/common/model_def.h"
#include "nexus/common/rdma_sender.h"
#include "nexus/common/spinlock.h"
#include "nexus/common/typedef.h"
#include "nexus/proto/control.pb.h"

namespace nexus {
namespace backend {

/*!
 * \brief Backend server runs on top of a GPU, handles queries from frontends,
 *   and executes model instances on GPU.
 */
class BackendServer {
 public:
  /*!
   * \brief Constructs a backend server
   * \param port Port number for receiving requests
   * \param rpc_port Port number for RPC server and control messages
   * \param sch_addr Scheduler IP address, if no port specified, use default
   * port 10001 \param num_workers Number of worker threads \param gpu_id GPU
   * device ID \param model_db_root Model database root directory path
   */
  BackendServer(ario::PollerType poller_type, std::string rdma_dev,
                uint16_t port, std::string sch_addr, int gpu_id,
                size_t num_workers = 0, std::vector<int> cores = {});
  /*! \brief Deconstructs backend server */
  ~BackendServer();
  /*! \brief Get backend node ID */
  uint32_t node_id() const { return node_id_; }
  /*! \brief Get GPU device ID */
  int gpu_id() const { return gpu_id_; }
  /*! \brief Starts the backend server */
  void Run();
  /*! \brief Stops the backend server */
  void Stop();

  void LoadModelEnqueue(const BackendLoadModelCommand& req);
  void LoadModel(const BackendLoadModelCommand& req);
  void HandleEnqueueBatchPlan(BatchPlanProto&& req, RpcReply* reply);
  void MarkBatchPlanQueryPreprocessed(std::shared_ptr<Task> task);

 private:
  /*! \brief Daemon thread that sends stats to scheduler periodically. */
  void Daemon();

  void ModelTableDaemon();
  /*! \brief Register this backend server to global scheduler. */
  void Register();
  /*! \brief Unregister this backend server to global scheduler. */
  void Unregister();
  /*!
   * \brief Send model workload history to global scheduler.
   * \param request Workload history protobuf.
   */
  void KeepAlive();

  bool EnqueueQuery(std::shared_ptr<Task> task);
  void HandleFetchImageReply(ario::WorkRequestID wrid,
                             ario::OwnedMemoryBlock buf,
                             TimePoint got_image_time);

  class RdmaHandler : public ario::RdmaEventHandler {
   public:
    void OnConnected(ario::RdmaQueuePair* conn) override;
    void OnRemoteMemoryRegionReceived(ario::RdmaQueuePair* conn, uint64_t addr,
                                      size_t size) override;
    void OnRdmaReadComplete(ario::RdmaQueuePair* conn, ario::WorkRequestID wrid,
                            ario::OwnedMemoryBlock buf) override;
    void OnRecv(ario::RdmaQueuePair* conn, ario::OwnedMemoryBlock buf) override;
    void OnSent(ario::RdmaQueuePair* conn, ario::OwnedMemoryBlock buf) override;
    void OnError(ario::RdmaQueuePair* conn, ario::RdmaError error) override;

   private:
    void OnRecvInternal(ario::RdmaQueuePair* conn, ario::OwnedMemoryBlock buf);

    friend class BackendServer;
    explicit RdmaHandler(BackendServer& outer);
    BackendServer& outer_;
  };

  struct ConnContext {
    ario::RdmaQueuePair& conn;
    std::mutex mutex;

    size_t cnt_flying_image_fetch /* GUARDED_BY(mutex) */;
    std::deque<std::shared_ptr<Task>>
        pending_image_fetch_task /* GUARDED_BY(mutex) */;

    ConnContext(ario::RdmaQueuePair* conn);
  };

  void PostImageFetch(std::shared_ptr<ConnContext> ctx);

 private:
  /*! \brief GPU device index */
  int gpu_id_;
  std::string rdma_dev_;
  uint16_t rdma_port_;
  std::string gpu_name_;
  std::string gpu_uuid_;
  size_t gpu_memory_;

  ario::EpollExecutor executor_;
  RdmaHandler rdma_handler_;
  ario::MemoryBlockAllocator small_buffers_;
  ario::MemoryBlockAllocator large_buffers_;
  ario::RdmaManager rdma_;
  RdmaSender rdma_sender_;
  ario::RdmaQueuePair* dispatcher_conn_ = nullptr;

  // Ugly promises because ario doesn't have RPC
  std::promise<ario::RdmaQueuePair*> promise_dispatcher_conn_;
  std::promise<RegisterReply> promise_register_reply_;
  std::promise<RpcReply> promise_unregister_reply_;

  // Helper workers that handles replies.
  // TODO: unify with the pre-existing Worker threads
  ario::EpollExecutor helper_executor_;
  std::vector<std::thread> executor_threads_;

  /*! \brief Interval to update stats to scheduler in seconds */
  uint32_t beacon_interval_sec_;
  /*! \brief Flag for whether backend and daemon thread is running */
  std::atomic_bool running_;
  /*! \brief Backend node id */
  uint32_t node_id_;
  /*! \brief Daemon thread */
  std::thread daemon_thread_;

  /*! \brief Connection pool. Guraded by mu_connections_. */
  std::unordered_set<ario::RdmaQueuePair*> all_connections_;
  std::unordered_map<NodeId, std::shared_ptr<ConnContext>> node_connections_;
  std::unordered_map<ario::RdmaQueuePair*, NodeId> map_connection_nodeid_;
  /*! \brief Mutex for connections_ */
  std::mutex mu_connections_;

  /*! \brief Task queue for workers to work on */
  BlockPriorityQueue<Task> task_queue_;
  /*! \brief Worker thread pool */
  std::vector<std::unique_ptr<Worker>> workers_;
  /*! \brief GPU executor */
  std::unique_ptr<GpuExecutorPlanFollower> gpu_executor_;
  /*!
   * \brief Mapping from ModelIndex to model instance.
   * Guarded by model_table_mu_.p
   */
  std::vector<ModelExecutorPtr> model_table_;
  std::thread model_table_thread_;

  BlockQueue<BackendLoadModelCommand> model_table_requests_;
  /*! \brief Mutex for accessing model_table_ */
  std::mutex model_table_mu_;
  /*! \brief Random number genertor */
  std::random_device rd_;
  std::mt19937 rand_gen_;

  // Tasks pending FetchImageReply
  std::mutex mu_tasks_pending_fetch_image_;
  std::unordered_map<ario::WorkRequestID, std::shared_ptr<Task>>
      tasks_pending_fetch_image_;

  // Batch plans waiting for image and not added to gpu_executor_ yet.
  std::mutex mu_pending_plans_;
  std::unordered_map<PlanId, std::shared_ptr<BatchPlanContext>> pending_plans_;
};

}  // namespace backend
}  // namespace nexus

#endif  // NEXUS_BACKEND_BACKEND_SERVER_H_
