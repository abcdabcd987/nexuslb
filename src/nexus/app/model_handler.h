#ifndef NEXUS_COMMON_MODEL_HANDLER_H_
#define NEXUS_COMMON_MODEL_HANDLER_H_

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>

#include "ario/ario.h"
#include "nexus/common/backend_pool.h"
#include "nexus/common/data_type.h"
#include "nexus/common/metric.h"
#include "nexus/common/rdma_sender.h"
#include "nexus/common/typedef.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace app {

/*!
 * \brief QueryResult provides a mechanism to access the result of
 *   ansynchronous model execution.
 */
class QueryResult {
 public:
  /*!
   * \brief Constructor of OutputFuture
   * \param timeout_ms Timeout for output future in millisecond
   */
  QueryResult(uint64_t qid);

  bool ready() const { return ready_; }

  uint64_t query_id() const { return qid_; }
  /*! \brief Gets the status of output result */
  uint32_t status() const;
  /*! \brief Gets the error message if any error happens in the execution */
  std::string error_message() const;
  /*!
   * \brief Output the result to reply protobuf
   * \param reply ReplyProto to be filled
   */
  void ToProto(ReplyProto* reply) const;
  /*!
   * \brief Get the record given then index
   * \param idx Index of record
   * \return Record at idx
   */
  const Record& operator[](uint32_t idx) const;
  /*! \brief Get number of records in the output */
  uint32_t num_records() const;

  void SetResult(const QueryResultProto& result);

 private:
  void CheckReady() const;

  void SetError(uint32_t error, const std::string& error_msg);

 private:
  uint64_t qid_;
  std::atomic<bool> ready_;
  uint32_t status_;
  std::string error_message_;
  std::vector<Record> records_;
};

class RequestContext;

class ModelHandler {
 public:
  ModelHandler(ModelSession model_session, ModelIndex model_index,
               BackendPool& pool, NodeId frontend_id,
               ario::RdmaQueuePair* model_worker_conn, RdmaSender rdma_sender,
               ario::MemoryBlockAllocator* input_memory_allocator);

  ~ModelHandler();

  ModelSession model_session() const { return model_session_; }

  std::string model_session_id() const { return model_session_id_; }

  ModelIndex model_index() const { return model_index_; }

  std::shared_ptr<IntervalCounter> counter() const { return counter_; }

  std::shared_ptr<QueryResult> Execute(
      std::shared_ptr<RequestContext> ctx,
      std::vector<std::string> output_fields = {}, uint32_t topk = 1,
      std::vector<RectProto> windows = {});

  void SendBackendQuery(std::shared_ptr<RequestContext> ctx, uint64_t qid,
                        std::shared_ptr<BackendSession> backend);

  void HandleBackendReply(const QueryResultProto& result);

  void HandleDispatcherReply(const DispatchReply& reply);

  std::vector<uint32_t> BackendList();

 private:
  std::shared_ptr<BackendSession> GetBackendWeightedRoundRobin();

  std::shared_ptr<BackendSession> GetBackendDeficitRoundRobin();

  NodeId frontend_id_;
  ModelSession model_session_;
  std::string model_session_id_;
  ModelIndex model_index_;
  BackendPool& backend_pool_;
  static std::atomic<uint64_t> global_query_id_;

  ario::RdmaQueuePair* model_worker_conn_;
  RdmaSender rdma_sender_;
  ario::MemoryBlockAllocator* input_memory_allocator_;

  std::vector<uint32_t> backends_;
  /*!
   * \brief Mapping from backend id to its serving rate,
   *
   *   Guarded by route_mu_
   */
  std::unordered_map<uint32_t, double> backend_rates_;

  std::unordered_map<uint32_t, double> backend_quanta_;
  double quantum_to_rate_ratio_ = 0;
  size_t current_drr_index_ = 0;
  float total_throughput_;
  /*! \brief Interval counter to count number of requests within each
   *  interval.
   */
  std::shared_ptr<IntervalCounter> counter_;

  std::unordered_map<QueryId, std::shared_ptr<RequestContext>> query_ctx_;
  std::mutex route_mu_;
  std::mutex query_ctx_mu_;
  /*! \brief random number generator */
  std::random_device rd_;
  std::mt19937 rand_gen_;
};

}  // namespace app
}  // namespace nexus

#endif  // NEXUS_COMMON_MODEL_HANDLER_H_
