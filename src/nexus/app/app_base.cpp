#include "nexus/app/app_base.h"

namespace nexus {
namespace app {

AppBase::AppBase(ario::PollerType poller_type, std::string rdma_dev,
                 uint16_t rdma_tcp_server_port, std::string nexus_server_port,
                 std::string sch_addr, size_t nthreads)
    : Frontend(poller_type, std::move(rdma_dev), rdma_tcp_server_port,
               std::move(nexus_server_port), std::move(sch_addr)),
      nthreads_(nthreads),
      qp_(nullptr) {}

AppBase::~AppBase() {
  if (qp_ != nullptr) {
    delete qp_;
  }
}

void AppBase::Start() {
  CHECK(qp_ != nullptr) << "Query processor is not initialized";
  Run(qp_, nthreads_);
}

std::shared_ptr<ModelHandler> AppBase::GetModelHandler(
    const std::string& framework, const std::string& model_name,
    uint32_t version, uint64_t latency_sla, float estimate_workload,
    std::vector<uint32_t> image_size) {
  LoadModelRequest req;
  req.set_node_id(node_id());
  auto model_sess = req.mutable_model_session();
  model_sess->set_framework(framework);
  model_sess->set_model_name(model_name);
  model_sess->set_version(version);
  model_sess->set_latency_sla(latency_sla);
  if (image_size.size() > 0) {
    if (image_size.size() != 2) {
      LOG(ERROR) << "Image size is not 2";
      return nullptr;
    }
    model_sess->set_image_height(image_size[0]);
    model_sess->set_image_width(image_size[1]);
  }
  if (estimate_workload < 0) {
    LOG(ERROR) << "Estimate workload must be non-negative value";
    return nullptr;
  }
  if (estimate_workload > 0) {
    req.set_estimate_workload(estimate_workload);
  }

  auto model_handler = LoadModel(req);
  if (model_handler == nullptr) {
    // TODO: load model failed, should retry after some time,
    // or wait for callback from scheduler
    LOG(FATAL) << "Load model failed";
  }
  return model_handler;
}

void LaunchApp(AppBase* app) {
  app->Setup();
  app->Start();
}

}  // namespace app
}  // namespace nexus
