#include <gflags/gflags.h>
#include <glog/logging.h>
#include <yaml-cpp/yaml.h>

#include <thread>

#include "nexus/common/config.h"
#include "nexus/common/model_db.h"
#include "nexus/common/util.h"
#include "nexus/dispatcher/dispatcher.h"

using namespace nexus::dispatcher;

DEFINE_string(rdma_dev, "", "RDMA device name");
DEFINE_uint32(port, 7001, "TCP port used to setup RDMA connection.");
DEFINE_string(pin_cpus, "all", "Example: `0,2-4,7-16`. Example: `all`");

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
  std::vector<int> cores;
  if (FLAGS_pin_cpus == "all") {
    auto n = std::thread::hardware_concurrency();
    for (unsigned int i = 0; i < n; ++i) {
      cores.push_back(i);
    }
  } else {
    cores = ParseCores(FLAGS_pin_cpus);
  }

  Dispatcher dispatcher(FLAGS_rdma_dev, FLAGS_port, std::move(cores));
  dispatcher.Run();
}
