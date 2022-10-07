#include "nexus/dispatcher/batch_size_estimator.h"

namespace nexus {
namespace dispatcher {
namespace {

struct Result {
  uint32_t batch_size;
  double num_gpus;
};

Result EstimateBatchSize(const ModelProfile& profile, double time_budget,
                         double ddata, double rps) {
  // (0)  l(b) = alpha * b + beta
  // (1)  n * (b / l(b)) == r
  // (2)  l(b) * (1 + 1 / n) < SLO
  for (uint32_t bs = 1;; ++bs) {
    double l = profile.GetForwardLatency(bs) * 1e-6;
    double t = bs / l;
    double n = rps / t;
    double n1 = n > 1 ? n : 1;
    if (l * (1 + 1 / n1) > time_budget - ddata * bs) {
      return {std::max(1U, bs - 1), n};
    }
  }
}

}  // namespace

BatchSizeEstimator::BatchSizeEstimator(double rps_multiplier,
                                       double std_multiplier)
    : xrps_(rps_multiplier), xstd_(std_multiplier) {}

uint32_t BatchSizeEstimator::Estimate(const ModelProfile& profile,
                                      std::chrono::nanoseconds time_budget,
                                      std::chrono::nanoseconds ddata,
                                      double rps, double std) const {
  double r = rps * xrps_ + std * xstd_;
  double budget_sec = time_budget.count() * 1e-9;
  double ddata_sec = ddata.count() * 1e-9;
  auto res = EstimateBatchSize(profile, budget_sec, ddata_sec, r);
  if (res.num_gpus > 1) {
    return res.batch_size;
  }

  // l(b) + b/r <= SLO, for n < 1, from Nexus paper, Line 13
  for (uint32_t bs = 2;; ++bs) {
    double l = profile.GetForwardLatency(bs) * 1e-6;
    if (l + bs / r + ddata_sec * bs > budget_sec) {
      return bs - 1;
    }
  }
  return 1;
}

}  // namespace dispatcher
}  // namespace nexus
