#ifndef NEXUS_COMMON_RPS_METER_H_
#define NEXUS_COMMON_RPS_METER_H_

#include <cstddef>
#include <optional>

#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"

namespace nexus {

class RpsMeter {
 public:
  RpsMeter(double window_span_second, size_t history_length);
  void Hit(TimePoint time);
  std::optional<AvgStd> Get(TimePoint now);

 private:
};

}  // namespace nexus

#endif
