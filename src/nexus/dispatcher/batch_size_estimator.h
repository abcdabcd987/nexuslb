#ifndef NEXUS_DISPATCHER_BATCH_SIZE_ESTIMATOR_H_
#define NEXUS_DISPATCHER_BATCH_SIZE_ESTIMATOR_H_

#include <chrono>
#include <cstdint>

#include "nexus/common/model_db.h"

namespace nexus {
namespace dispatcher {

class BatchSizeEstimator {
 public:
  BatchSizeEstimator(double rps_multiplier, double std_multiplier);
  uint32_t Estimate(const ModelProfile& profile,
                    std::chrono::nanoseconds time_budget,
                    std::chrono::nanoseconds ddata, double rps,
                    double std) const;

 private:
  double xrps_;
  double xstd_;
};

}  // namespace dispatcher
}  // namespace nexus

#endif
