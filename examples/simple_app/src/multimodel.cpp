#include <gflags/gflags.h>
#include <yaml-cpp/yaml.h>

#include <iostream>
#include <unordered_set>
#include <utility>

#include "nexus/app/app_base.h"
#include "nexus/common/model_def.h"

using namespace nexus;
using namespace nexus::app;

struct ModelConfig {
  std::string framework;
  std::string model;
  uint32_t slo;
  uint32_t version;
  uint32_t width;
  uint32_t height;
  float rps_hint;

  static ModelConfig FromYaml(size_t cfgidx, const YAML::Node& config);
};

class MultiModelApp : public AppBase {
 public:
  MultiModelApp(ario::PollerType poller_type, std::string rdma_dev,
                uint16_t rdma_tcp_server_port, std::string nexus_server_port,
                std::string sch_addr, size_t nthreads,
                std::vector<ModelConfig> model_configs)
      : AppBase(poller_type, std::move(rdma_dev), rdma_tcp_server_port,
                std::move(nexus_server_port), std::move(sch_addr), nthreads),
        model_configs_(std::move(model_configs)) {}

  void Setup() final {
    for (const auto& cfg : model_configs_) {
      auto model_ =
          GetModelHandler(cfg.framework, cfg.model, cfg.version, cfg.slo,
                          cfg.rps_hint, {cfg.height, cfg.width});
      models_.push_back(model_);
      auto func1 = [model_](std::shared_ptr<RequestContext> ctx) {
        auto output = model_->Execute(ctx);
        return std::vector<VariablePtr>{
            std::make_shared<Variable>("output", output)};
      };
      auto func2 = [model_](std::shared_ptr<RequestContext> ctx) {
        auto output = ctx->GetVariable("output")->result();
        output->ToProto(ctx->reply());
        return std::vector<VariablePtr>{};
      };
      ExecBlock* b1 = new ExecBlock(0, func1, {});
      ExecBlock* b2 = new ExecBlock(1, func2, {"output"});
      qp_ = new QueryProcessor({b1, b2});
    }
  }

 private:
  std::vector<ModelConfig> model_configs_;
  std::vector<std::shared_ptr<ModelHandler>> models_;
};

DEFINE_string(poller, "blocking", "options: blocking, spinning");
DEFINE_string(rdma_dev, "", "RDMA device name");
DEFINE_uint32(rdma_port, 9002, "TCP port used to setup RDMA connection.");
DEFINE_string(nexus_port, "9001", "Server port");
DEFINE_string(sch_addr, "127.0.0.1", "Scheduler address");
DEFINE_int32(nthread, 8, "Number of threads processing requests");

ModelConfig ModelConfig::FromYaml(size_t cfgidx, const YAML::Node& config) {
  ModelConfig opt;

  // Model framework
  opt.framework = config["framework"].as<std::string>("tensorflow");

  // Model name
  LOG_IF(FATAL, !config["model"]) << "Must specify `model`. cfgidx=" << cfgidx;
  opt.model = config["model"].as<std::string>();

  // Model latency SLO in milliseconds
  LOG_IF(FATAL, !config["slo"]) << "Must specify `slo`. cfgidx=" << cfgidx;
  opt.slo = config["slo"].as<uint32_t>();

  // Model version
  opt.version = config["version"].as<uint32_t>(1);

  // Input image width
  opt.width = config["width"].as<uint32_t>(0);

  // Input image height
  opt.height = config["height"].as<uint32_t>(0);

  // Request rate hint
  opt.rps_hint = config["rps_hint"].as<float>(0);

  return opt;
}

int main(int argc, char** argv) {
  // log to stderr
  FLAGS_alsologtostderr = 1;
  FLAGS_colorlogtostderr = 1;
  // Init glog
  google::InitGoogleLogging(argv[0]);
  // Parse command line flags
  google::ParseCommandLineFlags(&argc, &argv, true);
  // Setup backtrace on segfault
  google::InstallFailureSignalHandler();

  ario::PollerType poller_type;
  if (FLAGS_poller == "blocking") {
    poller_type = ario::PollerType::kBlocking;
  } else if (FLAGS_poller == "spinning") {
    poller_type = ario::PollerType::kSpinning;
  } else {
    LOG(FATAL) << "Invalid poller type";
  }

  // Parse models
  auto config = YAML::Load(std::cin);
  LOG_IF(FATAL, !config.IsSequence())
      << "Root of the YAML config must be an array.";
  std::vector<ModelConfig> models;
  for (size_t i = 0; i < config.size(); ++i) {
    models.push_back(ModelConfig::FromYaml(i, config[i]));
  }

  LOG(INFO) << "RDMA device " << FLAGS_rdma_dev << ", RDMA TCP port "
            << FLAGS_rdma_port << ", Nexus port " << FLAGS_nexus_port;
  // Create the frontend server
  MultiModelApp app(poller_type, FLAGS_rdma_dev, FLAGS_rdma_port,
                    FLAGS_nexus_port, FLAGS_sch_addr, FLAGS_nthread, models);
  LaunchApp(&app);

  return 0;
}
