#ifndef NEXUS_BACKEND_MODEL_EXEC_H_
#define NEXUS_BACKEND_MODEL_EXEC_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

#include "nexus/common/model_db.h"
#include "nexus/proto/nexus.pb.h"

namespace nexus {
namespace backend {

class ModelExecutor {
 public:
  ModelExecutor(ModelSession model_session, const ModelProfile& profile);

  ~ModelExecutor();

  const ModelProfile* profile() const { return profile_; }

  uint32_t batch() const { return batch_; }

  void SetBatch(uint32_t batch) { batch_ = batch; }

  uint64_t Execute();

 private:
  struct Query {
    uint32_t frontend_id;
    uint64_t query_id;
    long deadline_ns;

    struct HeapCmp {
      bool operator()(const Query& lhs, const Query& rhs) const {
        return lhs.deadline_ns > rhs.deadline_ns;
      }
    };
  };

  struct GetBatchResult {
    std::vector<Query> inputs;
    std::vector<Query> drops;
  };

  GetBatchResult GetBatchTaskSlidingWindow(uint32_t batch_size);

  ModelSession model_session_;
  const ModelProfile* profile_;

  std::atomic<uint32_t> batch_;

  /*! \brief Priority queue of inputs based on deadline. Guarded by task_mu_. */
  std::priority_queue<Query, std::vector<Query>, Query::HeapCmp> input_queue_;
  /*! \brief Mutex to proect input_queue_. */
  std::mutex task_mu_;

  // Scheduler-only test related:
 public:
  void AddQuery(const QueryProto& query);
};

using ModelExecutorPtr = std::shared_ptr<ModelExecutor>;

}  // namespace backend
}  // namespace nexus

#endif  // NEXUS_BACKEND_MODEL_EXEC_H_
