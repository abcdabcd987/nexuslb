#ifndef NEXUS_BACKEND_MODEL_EXEC_H_
#define NEXUS_BACKEND_MODEL_EXEC_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_set>

#include "nexus/backend/batch_plan_context.h"
#include "nexus/backend/model_ins.h"
#include "nexus/common/block_queue.h"
#include "nexus/common/metric.h"
#include "nexus/common/model_db.h"

namespace nexus {
namespace backend {

class ModelExecutor {
 public:
  ModelExecutor(int gpu_id, const ModelInstanceConfig& config,
                ModelIndex model_index, BlockPriorityQueue<Task>& task_queue);

  ~ModelExecutor();

  ModelInstance* model() { return model_.get(); }

  const ModelInstance* model() const { return model_.get(); }
  /*! \brief Return whether this model is a backup model. */
  bool backup() const { return backup_; }

  const ModelProfile* profile() const { return profile_; }

  void SetBatch(uint32_t batch) { model_->set_batch(batch); }

  double GetRequestRate();

  double GetDropRate();

  bool IsSharePrefixModel() const;
  bool IsTFShareModel() const;

  bool HasBackup();

  std::vector<uint32_t> BackupBackends();

  void UpdateBackupBackends(const ModelInstanceConfig& config);

  std::shared_ptr<Array> AcquireInputArray();
  std::shared_ptr<Array> AcquirePinnedMemory();
  void ReleaseInputArray(std::shared_ptr<Array> array);
  void ReleasePinnedMemory(std::shared_ptr<Array> array);

  bool Preprocess(std::shared_ptr<Task> task, bool force = false);

  bool AddPreprocessedTask(std::shared_ptr<Task> task, bool force = false);

  void Postprocess(std::shared_ptr<Task> task);

  void ExecuteBatchPlan(std::shared_ptr<BatchPlanContext> plan);
  void DropBatchPlan(std::shared_ptr<BatchPlanContext> plan);

  TimePoint LastExecuteFinishTime();

  int NumberOfOpenRequests() const;

  uint64_t GetPeakMemoryUsage();

  uint64_t GetStaticMemoryUsage();

 private:
  std::shared_ptr<BatchTask> GetBatchTaskByBatchPlan(
      std::shared_ptr<BatchPlanContext> plan);

  bool IncreaseOpenRequests(int cnt, bool limit_max_batch);

  void DecreaseOpenRequests(int cnt);

  std::unique_ptr<ModelInstance> model_;
  bool backup_;
  const ModelProfile* profile_;
  BlockPriorityQueue<Task>& task_queue_;
  /*!
   * \brief Map from task id to current processing tasks.
   * Guarded by task_mu_.
   */
  std::unordered_map<uint64_t, std::shared_ptr<Task>> processing_tasks_;
  /*! \brief Priority queue of inputs based on deadline. Guarded by task_mu_. */
  std::priority_queue<std::shared_ptr<Input>,
                      std::vector<std::shared_ptr<Input>>, CompareDeadlineItem>
      input_queue_;
  /*! \brief Input array allocated in GPU memory to hold batch inputs. */
  std::mutex input_arrays_mutex_;
  std::unordered_set<std::shared_ptr<Array>>
      input_arrays_ /* GUARDED_BY(input_arrays_mutex_) */;
  std::unordered_set<std::shared_ptr<Array>>
      idle_input_arrays_ /* GUARDED_BY(input_arrays_mutex_) */;
  // Pinned memory for CUDA
  std::unordered_set<std::shared_ptr<Array>>
      pinned_arrays_ /* GUARDED_BY(input_arrays_mutex_) */;
  std::unordered_set<std::shared_ptr<Array>>
      idle_pinned_arrays_ /* GUARDED_BY(input_arrays_mutex_) */;
  /*! \brief Batch index. */
  std::atomic<uint64_t> batch_id_;
  /*! \brief Number of open requests. */
  std::atomic<int> open_requests_;
  /*! \brief Interval counter to count number of requests within each interval.
   */
  std::shared_ptr<IntervalCounter> req_counter_;
  std::shared_ptr<IntervalCounter> drop_counter_;

  EWMA req_rate_;
  EWMA drop_rate_;

  std::vector<uint32_t> backup_backends_;
  /*!
   * \brief Last time point that finishes the batch execution.
   * Guarded by time_mu_.
   */
  TimePoint last_exec_finish_;
  /*! \brief Mutex to proect processing_tasks_ and input_queue_. */
  std::mutex task_mu_;
  /*! \brief Mutex to proect last_exec_finish_. */
  std::mutex time_mu_;

  std::mutex backup_mu_;
};

using ModelExecutorPtr = std::shared_ptr<ModelExecutor>;

}  // namespace backend
}  // namespace nexus

#endif  // NEXUS_BACKEND_MODEL_EXEC_H_
