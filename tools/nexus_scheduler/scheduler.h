#ifndef NEXUS_SCHEDULER_SCHEDULER_H_
#define NEXUS_SCHEDULER_SCHEDULER_H_

#include <yaml-cpp/yaml.h>

#include <boost/asio.hpp>
#include <chrono>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "nexus/proto/nexus.pb.h"
#include "nexus_scheduler/backend_delegate.h"
#include "nexus_scheduler/fake_object_accessor.h"
#include "nexus_scheduler/frontend_delegate.h"
#include "nexus_scheduler/sch_info.h"

namespace nexus {
namespace scheduler {

using BackendDelegatePtr = std::shared_ptr<BackendDelegate>;
using FrontendDelegatePtr = std::shared_ptr<FrontendDelegate>;
using SessionInfoPtr = std::shared_ptr<SessionInfo>;

/*! \brief Scheduler acts as a global centralized scheduler server. */
class Scheduler {
 public:
  struct Config {
    uint32_t beacon;     // Beacon interval in seconds
    uint32_t epoch;      // Epoch scheduling interval in seconds
    uint32_t min_epoch;  // Minimum interval in seconds to invoke epoch schedule
    uint32_t avg_interval;  // Moving average interval for backend rate

    static Config Default();
  };
  Scheduler(boost::asio::io_context* io_context,
            const FakeObjectAccessor* accessor, const Config& config);
  uint32_t beacon_interval_sec() const { return beacon_interval_sec_; }
  void Start();
  void Stop();

  /*!
   * \brief Loads the workload configuation for backends from config.
   */
  void LoadWorkload(const YAML::Node& config);
  /*!
   * \brief Handles LoadModel RPC.
   *
   * This function acquires mutex_.
   *
   * \param request Load model request
   */
  void LoadModel(const LoadModelRequest& request, NexusLoadModelReply* reply);
  /*!
   * \brief Handles ReportWorkload RPC.
   *
   * This function acquires mutex_.
   *
   * \param request Workload stats information
   */
  void ReportWorkload(const WorkloadStatsProto& request);

  /*!
   * \brief Registers frontend RPC client and fills in the register reply.
   *
   * This function acquires mutex_.
   *
   * \param frontend Frontend RPC client pointer.
   */
  void RegisterFrontend(FrontendDelegatePtr frontend);
  /*!
   * \brief Registers backend RPC client and fills in the register reply.
   *
   * This function acquires mutex_.
   *
   * \param backend Backend RPC client pointer.
   */
  void RegisterBackend(BackendDelegatePtr backend);
  /*!
   * \brief Update workload to the new added backend
   *
   * This function doesn't acquire mutex_.
   *
   * \param backend Backend client pointer
   */
  void AddBackend(BackendDelegatePtr backend);

 private:
  void CheckEpochSchedule();

  /*!
   * \brief Get backend rpc client given the node id.
   *
   * This function doesn't acquire mutex_.
   *
   * \param node_id Backend node id.
   * \return BackendDelegate pointer if found, otherwise nullptr
   */
  BackendDelegatePtr GetBackend(uint32_t node_id);
  /*!
   * \brief Get frontend rpc client given the node id.
   *
   * This function doesn't acquire mutex_.
   *
   * \param node_id Frontend node id.
   * \return FrontendDelegate pointer if found, otherwise nullptr
   */
  FrontendDelegatePtr GetFrontend(uint32_t node_id);
  /*!
   * \brief Get the model route given the model session id.
   *
   * This function doesn't acquire mutex_.
   *
   * \param model_session_id Model session ID.
   * \param route Model route to fill in.
   */
  void GetModelRoute(const std::string& model_session_id,
                     ModelRouteProto* route);
  /*!
   * \brief Find the best-fit backend to load the model session with workload.
   *
   * This function doesn't acquire mutex_.
   *
   * \param model_sess Model session.
   * \param request_rate Requests per second.
   * \param skips Backends that should be skipped.
   * \param best_backend Best-fit backend pointer.
   * \param inst_cfg Model instance configuration to be loaded.
   */
  void FindBestBackend(const ModelSession& model_sess, double request_rate,
                       const std::unordered_set<uint32_t>& skips,
                       BackendDelegatePtr* best_backend,
                       InstanceInfo* inst_info);
  /*!
   * \brief At each beacon cycle, check whether frontends and backends are
   * alive, and aggregate model session request rates from backends.
   *
   * This function acquires mutex_.
   */
  bool BeaconCheck();
  /*!
   * \brief At each epoch cycle, re-schedule the resources for all model
   * sessions based on the request rates during last epoch
   *
   * This function acquires mutex_.
   */
  void EpochSchedule();
  /*!
   * \brief Try to allocate backends for unassigned workloads.
   *
   * This function doesn't acquire mutex_.
   *
   * \param changed_routes Output changed routing table during the allocation.
   * \param changed_backends Output backends that loads new workloads.
   */
  void AllocateUnassignedWorkloads(
      std::unordered_set<SessionInfoPtr>* changed_sessions,
      std::unordered_set<BackendDelegatePtr>* changed_backends = nullptr);

  void ConsolidateBackends(
      std::unordered_set<SessionInfoPtr>* changed_sessions);
  /*!
   * \brief Update model routing tables to subscribed frontends
   *
   * This function doesn't acquire mutex_.
   *
   * \param model_sessions Model Sessions of which routing table changed.
   */
  void UpdateModelRoutes(std::unordered_set<SessionInfoPtr> sessions);
  /*!
   * \brief Print out model table for debugging.
   *
   * This function doesn't acquire mutex_.
   */
  void DisplayModelTable();

  boost::asio::io_context& io_context_;
  const FakeObjectAccessor& accessor_;
  boost::asio::system_timer epoch_schedule_timer_;
  TimePoint last_epoch_schedule_;

  /*! \brief Beacon interval in seconds */
  uint32_t beacon_interval_sec_;
  /*! \brief Epoch duration in seconds */
  uint32_t epoch_interval_sec_;
  uint32_t min_epoch_;
  uint32_t avg_interval_;
  /*! \brief History length to keep in the model stats */
  uint32_t history_len_;
  /*! \brief Static workload configuration */
  std::vector<std::vector<YAML::Node> > static_workloads_;
  /*! \brief Mapping from static workload id to backend node id */
  std::unordered_map<int, uint32_t> assigned_static_workloads_;
  /*! \brief Mapping from frontend node id to frontend client */
  std::unordered_map<uint32_t, FrontendDelegatePtr> frontends_;
  /*! \brief Mapping from backend node id to backend client */
  std::unordered_map<uint32_t, BackendDelegatePtr> backends_;
  /*! \brief Mapping from model session ID to session information */
  std::unordered_map<std::string, SessionInfoPtr> session_table_;
  /*! \brief Mutex for accessing internal data */
  std::mutex mutex_;
};

}  // namespace scheduler
}  // namespace nexus

#endif  // NEXUS_SCHEDULER_SCHEDULER_H_
