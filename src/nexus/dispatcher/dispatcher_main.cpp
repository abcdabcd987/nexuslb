#include <gflags/gflags.h>
#include <glog/logging.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h>

#include "nexus/common/config.h"
#include "nexus/common/util.h"
#include "nexus/dispatcher/delayed_scheduler.h"
#include "nexus/dispatcher/dispatcher.h"
#include "nexus/dispatcher/round_robin_scheduler.h"
#include "nexus/dispatcher/scheduler.h"

using namespace nexus::dispatcher;

DEFINE_string(rdma_dev, "", "RDMA device name");
DEFINE_uint32(port, 7001, "TCP port used to setup RDMA connection.");
DEFINE_string(udp_threads_pin_cpus, "",
              "Pin udp threads to the CPUs specified by the list. The list "
              "should contain exactly twice elements as the `-udp_thread`. "
              "Example: `0,2-4,7-16`.");
DEFINE_string(scheduler, "delayed", "Available schedulers: delayed, rr");
DEFINE_string(rr_config_file, "",
              "Path to YAML config file for the RoundRobinScheduler");
DEFINE_string(rr_config_string, "",
              "YAML config string for the RoundRobinScheduler. "
              "Format: `model_name: num_backends`. "
              "For example, `{resnet_0: 10, googlenet: 3}");

std::vector<int> ParseCores(const std::string& s) {
  std::vector<int> cores;
  std::vector<std::string> segs;
  nexus::SplitString(s, ',', &segs);
  for (auto seg : segs) {
    if (seg.find('-') == std::string::npos) {
      cores.push_back(std::stoi(seg));
    } else {
      std::vector<std::string> range;
      nexus::SplitString(seg, '-', &range);
      CHECK_EQ(range.size(), 2) << "Wrong format of cores";
      int beg = std::stoi(range[0]);
      int end = std::stoi(range[1]);
      for (int i = beg; i <= end; ++i) {
        cores.push_back(i);
      }
    }
  }
  return cores;
}

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InstallFailureSignalHandler();
  auto cores = ParseCores(FLAGS_udp_threads_pin_cpus);

  if (!FLAGS_rr_config_file.empty() && !FLAGS_rr_config_string.empty()) {
    LOG(FATAL) << "Option `-rr_config_file` and `-rr_config_string` cannot be "
                  "both specified at the same time.";
  }

  std::unique_ptr<Scheduler::Builder> scheduler_builder;
  if (FLAGS_scheduler == "delayed") {
    if (!FLAGS_rr_config_string.empty() || !FLAGS_rr_config_string.empty()) {
      LOG(FATAL) << "Option `-rr_config_file` and `-rr_config_string` cannot "
                    "be used with `-scheduler=delayed`.";
    }
    scheduler_builder = std::make_unique<DelayedScheduler::Builder>();
  } else if (FLAGS_scheduler == "rr") {
    YAML::Node config;
    if (!FLAGS_rr_config_file.empty()) {
      config = YAML::LoadFile(FLAGS_rr_config_file);
    } else {
      config = YAML::Load(FLAGS_rr_config_string);
    }
    if (!config.IsMap()) {
      LOG(FATAL) << "YAML config for RoundRobinScheduler is empty";
    }
    scheduler_builder =
        std::make_unique<RoundRobinScheduler::Builder>(std::move(config));
  } else {
    LOG(FATAL) << "Unknown scheduler \"" << FLAGS_scheduler << "\"";
  }

  Dispatcher dispatcher(FLAGS_rdma_dev, FLAGS_port, std::move(cores),
                        std::move(scheduler_builder));
  dispatcher.Run();
}
