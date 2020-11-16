#ifndef NEXUS_DISPATCHER_FRONTEND_DELEGATE_H_
#define NEXUS_DISPATCHER_FRONTEND_DELEGATE_H_

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>

#include "nexus/proto/control.grpc.pb.h"

namespace nexus {
namespace dispatcher {
class FrontendDelegate {
 public:
  FrontendDelegate(uint32_t node_id, const std::string& ip,
                   const std::string& server_port, const std::string& rpc_addr,
                   int beacon_sec);

  uint32_t node_id() const { return node_id_; }

  void Tick();
  bool IsAlive();

 private:
  uint32_t node_id_;
  std::string ip_;
  std::string server_port_;
  std::string rpc_port_;
  int beacon_sec_;
  long timeout_ms_;
  std::unique_ptr<FrontendCtrl::Stub> stub_;
  std::chrono::time_point<std::chrono::system_clock> last_time_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
