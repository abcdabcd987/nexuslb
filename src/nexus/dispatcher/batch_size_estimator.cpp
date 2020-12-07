#include "nexus/dispatcher/batch_size_estimator.h"

namespace nexus {
namespace dispatcher {

BatchSizeEstimator::BatchSizeEstimator(double rps_multiplier,
                                       double std_multiplier)
    : xrps_(rps_multiplier), xstd_(std_multiplier) {}

uint32_t BatchSizeEstimator::Estimate(const ModelProfile& profile,
                                      double time_budget, double rps,
                                      double std) const {
  // (0)  l(b) = alpha * b + beta
  // (1)  n * (b / l(b)) == xrps * r
  // (2)  l(b) * (1 + 1 / n) < SLO
  double r = rps + std * xstd_;
  for (uint32_t bs = 2;; ++bs) {
    double l = profile.GetForwardLatency(bs) * 1e-6;
    double t = bs / l;
    double n = xrps_ * r / t;
    if (l * (1 + 1 / n) > time_budget) {
      return bs - 1;
    }
  }
  return 1;
}

}  // namespace dispatcher
}  // namespace nexus
