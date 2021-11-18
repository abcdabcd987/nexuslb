#ifndef NEXUS_APP_APP_BASE_H_
#define NEXUS_APP_APP_BASE_H_

#include <gflags/gflags.h>

#include "nexus/app/frontend.h"

namespace nexus {
namespace app {

class AppBase : public Frontend {
 public:
  AppBase(ario::PollerType poller_type, std::string rdma_dev,
          uint16_t rdma_tcp_server_port, std::string nexus_server_port,
          std::string sch_addr, size_t nthreads);

  ~AppBase() override;

  void Start();

  virtual void Setup() {}

 protected:
  std::shared_ptr<ModelHandler> GetModelHandler(
      const std::string& framework, const std::string& model_name,
      uint32_t version, uint64_t latency_sla, float estimate_workload = 0.,
      std::vector<uint32_t> image_size = {});
  size_t nthreads_;
  QueryProcessor* qp_;
};

void LaunchApp(AppBase* app);

}  // namespace app
}  // namespace nexus

#endif  // NEXUS_APP_APP_BASE_H_
