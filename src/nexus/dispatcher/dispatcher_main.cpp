#include <gflags/gflags.h>
#include <glog/logging.h>
#include <yaml-cpp/yaml.h>

#include <chrono>
#include <thread>

#include "nexus/common/config.h"
#include "nexus/common/model_db.h"
#include "nexus/common/util.h"
#include "nexus/dispatcher/dispatcher.h"
#include "nexus/dispatcher/rankmt/scheduler.h"

using namespace nexus::dispatcher;

DEFINE_string(poller, "spinning", "options: blocking, spinning");
DEFINE_string(rdma_dev, "", "RDMA device name");
DEFINE_uint32(port, SCHEDULER_DEFAULT_PORT,
              "TCP port used to setup RDMA connection.");
DEFINE_string(pin_cpus, "all", "Example: `0,2-4,7-16`. Example: `all`");
DEFINE_uint32(rankmt_dctrl, RankmtConfig::Default().ctrl_latency.count() / 1000,
              "Rankmt: control plane latency in microseconds.");
DEFINE_uint32(rankmt_ddata, RankmtConfig::Default().data_latency.count() / 1000,
              "Rankmt: data plane latency in microseconds.");
DEFINE_uint32(rankmt_dresp, RankmtConfig::Default().resp_latency.count() / 1000,
              "Rankmt: result latency in microseconds.");

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
  FLAGS_alsologtostderr = 1;
  FLAGS_colorlogtostderr = 1;
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

  ario::PollerType poller_type;
  if (FLAGS_poller == "blocking") {
    poller_type = ario::PollerType::kBlocking;
  } else if (FLAGS_poller == "spinning") {
    poller_type = ario::PollerType::kSpinning;
  } else {
    LOG(FATAL) << "Invalid poller type";
  }

  RankmtConfig rankmt;
  rankmt.ctrl_latency = std::chrono::microseconds(FLAGS_rankmt_dctrl);
  rankmt.data_latency = std::chrono::microseconds(FLAGS_rankmt_ddata);
  rankmt.resp_latency = std::chrono::microseconds(FLAGS_rankmt_dresp);

  Dispatcher dispatcher(poller_type, FLAGS_rdma_dev, FLAGS_port,
                        std::move(cores), rankmt);
  dispatcher.Run();
}
