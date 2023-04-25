#ifndef NEXUS_COMMON_RPS_METER_H_
#define NEXUS_COMMON_RPS_METER_H_

#include <cstddef>
#include <optional>
#include <vector>

#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"

namespace nexus {

class RpsMeter {
 public:
  explicit RpsMeter(size_t window_size);
  void Hit();
  void SealCounter(TimePoint now);
  std::optional<AvgStd> Get() const;

 private:
  size_t size_;
  size_t counter_ = 0;
  TimePoint counter_start_at_;

  // Circular buffer
  std::vector<double> window_;
  size_t idx_ = 0;

  // Mean and stddev
  double avg_ = 0;
  double nvar_ = 0;
  double std_ = 0;
};

}  // namespace nexus

#endif
