#include <gflags/gflags.h>
#include <glog/logging.h>

#include "nexus/common/config.h"
#include "nexus/common/util.h"
#include "nexus/dispatcher/dispatcher.h"

DEFINE_string(port, "7001", "Server port");
DEFINE_string(rpc_port, "7002", "RPC port");
DEFINE_int32(udp_port, DISPATCHER_RPC_DEFAULT_PORT, "UDP RPC server port");
DEFINE_int32(udp_threads, 1, "Number of threads for UDP RPC servers");
DEFINE_string(udp_threads_pin_cpus, "",
              "Pin udp threads to the CPUs specified by the list. The list "
              "should contain exactly twice elements as the `-udp_thread`. "
              "Example: `0,2-4,7-16`.");

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
  nexus::dispatcher::Dispatcher dispatcher(FLAGS_rpc_port, FLAGS_udp_port,
                                           FLAGS_udp_threads, std::move(cores));
  dispatcher.Run();
}
