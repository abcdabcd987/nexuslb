#include <gflags/gflags.h>

#include "nexus/dispatcher/dispatcher.h"

DEFINE_string(port, "7001", "Server port");
DEFINE_string(rpc_port, "7002", "RPC port");
DEFINE_int32(udp_port, 7003, "UDP RPC server port");
DEFINE_string(sch_addr, "127.0.0.1", "Scheduler address");

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InstallFailureSignalHandler();
  nexus::dispatcher::Dispatcher dispatcher(FLAGS_port, FLAGS_rpc_port,
                                           FLAGS_sch_addr, FLAGS_udp_port);
  dispatcher.Run();
}
