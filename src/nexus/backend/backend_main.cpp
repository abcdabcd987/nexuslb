#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "nexus/backend/backend_server.h"
#include "nexus/common/config.h"
#include "nexus/common/image.h"
#include "nexus/common/util.h"
#include "nexus/proto/nnquery.pb.h"

using namespace nexus;
using namespace nexus::backend;

DEFINE_string(rdma_dev, "", "RDMA device name");
DEFINE_uint32(port, BACKEND_DEFAULT_PORT,
              "TCP port used to setup RDMA connection.");
DEFINE_string(sch_addr, "127.0.0.1",
              "scheduler IP address "
              "(use default port 10001 if no port specified)");
DEFINE_int32(gpu, 0, "gpu device ID (default: 0)");
DEFINE_uint64(num_workers, 0, "number of workers (default: 0)");
DEFINE_string(cores, "", "Specify cores to use, e.g., \"0-4\", or \"0-3,5\"");

std::vector<int> ParseCores(std::string s) {
  std::vector<int> cores;
  std::vector<std::string> segs;
  SplitString(s, ',', &segs);
  for (auto seg : segs) {
    if (seg.find('-') == std::string::npos) {
      cores.push_back(std::stoi(seg));
    } else {
      std::vector<std::string> range;
      SplitString(seg, '-', &range);
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

BackendServer *server_ptr;

void sigint_handler(int _sig) {
  if (server_ptr) {
    server_ptr->Stop();
  }
  std::exit(0);
}

int main(int argc, char **argv) {
  struct sigaction sig_handle;
  sig_handle.sa_handler = sigint_handler;
  sigemptyset(&sig_handle.sa_mask);
  sig_handle.sa_flags = 0;
  sigaction(SIGINT, &sig_handle, NULL);

  // Init glog
  google::InitGoogleLogging(argv[0]);
  // Parse command line flags
  google::ParseCommandLineFlags(&argc, &argv, true);
  // Setup backtrace on segfault
  google::InstallFailureSignalHandler();
  // Decide server IP address
  LOG(INFO) << "Backend server: rdma_dev " << FLAGS_rdma_dev << ", port "
            << FLAGS_port << ", workers " << FLAGS_num_workers << ", gpu "
            << FLAGS_gpu;
  // Initialize _Hack_Images
  {
    ImageProto image;
    image.set_hack_filename("__init_Hack_Images");
    (void)_Hack_DecodeImageByFilename(image, ChannelOrder::CO_BGR);
  }
  // Create the backend server
  std::vector<int> cores = ParseCores(FLAGS_cores);
  BackendServer server(FLAGS_rdma_dev, FLAGS_port, FLAGS_sch_addr, FLAGS_gpu,
                       FLAGS_num_workers, cores);
  server_ptr = &server;
  server.Run();
  return 0;
}
