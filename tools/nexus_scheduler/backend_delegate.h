#ifndef NEXUS_SCHEDULER_BACKEND_DELEGATE_H_
#define NEXUS_SCHEDULER_BACKEND_DELEGATE_H_

#include <yaml-cpp/yaml.h>

#include <chrono>
#include <unordered_map>
#include <vector>

#include "nexus/common/metric.h"
#include "nexus/common/model_db.h"
#include "nexus/proto/nexus.pb.h"
#include "nexus_scheduler/fake_object_accessor.h"
#include "nexus_scheduler/sch_info.h"

namespace nexus {
namespace scheduler {

class Scheduler;

using InstanceInfoPtr = std::shared_ptr<InstanceInfo>;

class BackendDelegate {
 public:
  BackendDelegate(const FakeObjectAccessor* accessor, uint32_t node_id,
                  const std::string& gpu_device, const std::string& gpu_uuid);

  uint32_t node_id() const { return node_id_; }

  std::string gpu_device() const { return gpu_device_; }

  int workload_id() const { return workload_id_; }

  void set_workload_id(int id) { workload_id_ = id; }

  bool overload() const { return overload_; }

  double Occupancy() const;

  void GetInfo(BackendInfo* info) const;

  bool Assign(const BackendDelegate& other);

  bool PrepareLoadModel(const ModelSession& model_sess, double workload,
                        InstanceInfo* inst_info, double* occupancy) const;

  void LoadModel(const InstanceInfo& inst_info);

  void LoadModel(const YAML::Node& model_info);

  void UnloadModel(const std::string& model_sess_id);

  /*!
   * \brief Update model throughput given model session id and throughput.
   * \param model_sess_id Model session ID.
   * \param throughput Expected throughput to be achieved.
   * \return Left over throughput if expected throughput is not achieved,
   *   otherwise 0.
   */
  double UpdateModelThroughput(const std::string& model_sess_id,
                               double throughput);

  void SpillOutWorkload(
      std::vector<std::pair<SessionGroup, double> >* spillout);

  void UpdateModelTableRpc();

  std::vector<std::string> GetModelSessions() const;

  std::vector<InstanceInfoPtr> GetModels() const { return models_; }

  const InstanceInfo* GetInstanceInfo(const std::string& model_sess_id) const;

  double GetModelThroughput(const std::string& model_sess_id) const;

  double GetModelGPUShare(const std::string& model_sess_id) const;

  double GetModelWeight(const std::string& model_sess_id) const;

  bool IsIdle() const;

 private:
  void ComputeBatchSize(InstanceInfo* inst_info, double workload) const;

  void UpdateCycle();

  const FakeObjectAccessor& accessor_;
  uint32_t node_id_;
  std::string gpu_device_;
  std::string gpu_uuid_;

  int workload_id_;

  std::vector<InstanceInfoPtr> models_;
  /*!
   * \brief Mapping from model session id to instance information.
   * It's possible that multiple model session ids mapping to same instance
   * info due to prefix batching.
   */
  std::unordered_map<std::string, InstanceInfoPtr> session_model_map_;
  double exec_cycle_us_;
  double duty_cycle_us_;
  bool overload_;
  /*! \brief Indicates whether model table is dirty. */
  bool dirty_model_table_;
};

}  // namespace scheduler
}  // namespace nexus

#endif  // NEXUS_SCHEDULER_BACKEND_DELEGATE_H_
