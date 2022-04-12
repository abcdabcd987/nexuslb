#ifndef NEXUS_BACKEND_MODEL_EXEC_H_
#define NEXUS_BACKEND_MODEL_EXEC_H_

#include <atomic>
#include <memory>
#include <mutex>

#include "nexus/backend/batch_task.h"
#include "nexus/backend/task.h"
#include "nexus/common/block_queue.h"
#include "nexus/common/metric.h"
#include "nexus/common/model_db.h"
#include "nexus/proto/nexus.pb.h"

namespace nexus {
namespace backend {

class ModelExecutor {
 public:
  ModelExecutor(int gpu_id, const ModelInstanceConfig& config,
                BlockPriorityQueue<Task>& task_queue);

  ~ModelExecutor();

  const ModelProfile* profile() const { return profile_; }

  void SetBatch(uint32_t batch) { batch_ = batch; }

  bool Preprocess(std::shared_ptr<Task> task, bool force = false);

  bool AddPreprocessedTask(std::shared_ptr<Task> task, bool force = false);

  uint64_t Execute(uint32_t batch = 0);

 private:
  std::pair<std::shared_ptr<BatchTask>, int> GetBatchTaskSlidingWindow(
      uint32_t batch_size);

  /*!
   * \brief Get batch task from the task queue.
   * \param batch_size Expected batch size in the batch task.
   * \return Batch task and the number of inputs dequeued from input queue.
   */
  std::pair<std::shared_ptr<BatchTask>, int> GetBatchTask(uint32_t batch_size);

  void RemoveTask(std::shared_ptr<Task> task);

  std::atomic<uint32_t> batch_;

  const ModelProfile* profile_;
  BlockPriorityQueue<Task>& task_queue_;
  /*!
   * \brief Map from task id to current processing tasks.
   * Guarded by task_mu_.
   */
  std::unordered_map<uint64_t, std::shared_ptr<Task> > processing_tasks_;
  /*! \brief Priority queue of inputs based on deadline. Guarded by task_mu_. */
  std::priority_queue<std::shared_ptr<Input>,
                      std::vector<std::shared_ptr<Input> >, CompareDeadlineItem>
      input_queue_;
  /*! \brief Batch index. */
  std::atomic<uint64_t> batch_id_;

  /*! \brief Mutex to proect processing_tasks_ and input_queue_. */
  std::mutex task_mu_;
};

using ModelExecutorPtr = std::shared_ptr<ModelExecutor>;

}  // namespace backend
}  // namespace nexus

#endif  // NEXUS_BACKEND_MODEL_EXEC_H_
