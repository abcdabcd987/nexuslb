#ifndef NEXUS_BACKEND_TASK_H_
#define NEXUS_BACKEND_TASK_H_

#include <yaml-cpp/yaml.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>

#include "ario/ario.h"
#include "nexus/common/block_queue.h"
#include "nexus/common/data_type.h"
#include "nexus/common/message.h"
#include "nexus/common/typedef.h"
#include "nexus/proto/control.pb.h"
#include "nexus/proto/nnquery.pb.h"

namespace nexus {
namespace backend {

class ModelExecutor;
class ModelInstance;
class Task;

/*!
 * \brief Input contains input data of a single input and related information
 *   to neural networks.
 */
class Input : public DeadlineItem {
 public:
  /*!
   * \brief Construct a Input
   * \param deadline Deadline of corresponding task
   * \param tid Task id of corresponding task
   * \param idx Index in the inputs of task
   * \param arr Input array that contains the input data
   */
  Input(TimePoint deadline, uint64_t tid, int idx, ArrayPtr arr);

  /*! \brief Task id */
  uint64_t task_id;
  /*! \brief Index in the input vector of task. */
  int index;
  /*! \brief Input array that contains the data. */
  std::shared_ptr<Array> array;
};

/*!
 * \brief Output contains the data of a single output.
 */
class Output {
 public:
  /*!
   * \brief Construct an Output.
   * \param tid Task id of corresponding task
   * \param idx Index in the outputs of task
   * \param arrs Map from name to output arrays.
   */
  Output(uint64_t tid, int idx,
         const std::unordered_map<std::string, ArrayPtr>& arrs);

  /*! \brief Task id */
  uint64_t task_id;
  /*! \brief Index in the output vector of task. */
  int index;
  /*! \brief Map from array name to array. */
  std::unordered_map<std::string, ArrayPtr> arrays;
};

/*! \brief Stage indicates the context processing stage */
enum Stage {
  /* !\brief Task is waiting for frontend to reply to FetchImage call */
  kFetchImage = 0,
  /* !\brief Task at the pre-processing stage */
  kPreprocess,
  /* !\brief Task at the forwarding model stage */
  kForward,
  /* !\brief Task at the post-processing stage */
  kPostprocess,
};

class Task : public DeadlineItem, public std::enable_shared_from_this<Task> {
 public:
  /*! \brief Construct a task without connection. */
  Task();
  /*!
   * \brief Construct a task with connection to frontend.
   * \param conn Connection to frontend server
   */
  Task(ario::RdmaQueuePair* conn, std::shared_ptr<ModelExecutor> model);
  /*!
   * \brief Decode query from message.
   * \param message Message received from frontend
   */
  void SetQuery(QueryProto decoded_query, uint64_t rdma_read_offset,
                uint64_t rdma_read_length);
  /*!
   * \brief Append preprocessed input array to task.
   * \param arr Input array
   */
  void AppendInput(ArrayPtr arr);
  /*!
   * \brief Add output at index location
   * \param index Index of the output
   * \param output Output content
   * \return whether all output has been filled in
   */
  bool AddOutput(std::shared_ptr<Output> output);
  /*!
   * \brief Add virtual output at index location due to error such as timeout
   * \param index Index of the virtual output
   * \return whether all output has been filled in
   */
  bool AddVirtualOutput(int index);

  void SetConnection(ario::RdmaQueuePair* conn);
  void SetPlanId(PlanId plan_id);

  /*! \brief Task id */
  uint64_t task_id;
  std::optional<PlanId> plan_id;
  /*! \brief Connection to frontend. */
  ario::RdmaQueuePair* connection;
  /*! \brief Message type */
  MessageType msg_type;
  /*! \brief Query to process */
  QueryProto query;
  uint64_t rdma_read_offset = 0;
  uint64_t rdma_read_length = 0;
  int index_in_batchplan = -1;
  /*! \brief Query result */
  QueryResultProto result;
  /*! \brief Model instance to execute for the task */
  std::shared_ptr<ModelExecutor> model;
  /*!
   * \brief Suffix model for postprocessing, only used in the share prefix
   * model.
   */
  std::shared_ptr<ModelInstance> suffix_model;
  /*! \brief Current task processing stage */
  volatile Stage stage;
  std::vector<std::shared_ptr<Input> > inputs;
  /*! \brief Outputs of the context */
  std::vector<std::shared_ptr<Output> > outputs;
  /*! \brief Number of outputs that has been filled in */
  std::atomic<uint32_t> filled_outputs;
  /*! \brief Attributes that needs to be kept during the task */
  YAML::Node attrs;
  /*! \brief Timer that counts the time spent in each stage */
  Timer timer;

 private:
  /*! \brief Global task ID */
  static std::atomic<uint64_t> global_task_id_;
};

}  // namespace backend
}  // namespace nexus

#endif  // NEXUS_BACKEND_TASK_H_
