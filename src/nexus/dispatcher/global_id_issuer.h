#ifndef NEXUS_DISPATCHER_GLOBAL_ID_ISSUER_H_
#define NEXUS_DISPATCHER_GLOBAL_ID_ISSUER_H_

#include <atomic>

#include "nexus/common/typedef.h"

namespace nexus {
namespace dispatcher {

class GlobalIdIssuer {
 public:
  GlobalIdIssuer() = default;
  GlobalIdIssuer(const GlobalIdIssuer&) = delete;
  GlobalIdIssuer& operator=(const GlobalIdIssuer&) = delete;
  GlobalIdIssuer(GlobalIdIssuer&&) = delete;
  GlobalIdIssuer& operator=(GlobalIdIssuer&&) = delete;

  GlobalId Next() { return GlobalId(next_global_id_.fetch_add(1)); }

 private:
  std::atomic<uint64_t> next_global_id_{1};
};

}  // namespace dispatcher
}  // namespace nexus

#endif
