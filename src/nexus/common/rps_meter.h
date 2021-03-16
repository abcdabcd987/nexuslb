#ifndef NEXUS_COMMON_RPS_METER_H_
#define NEXUS_COMMON_RPS_METER_H_

#include <cstddef>
#include <deque>
#include <optional>

#include "nexus/common/time_util.h"
#include "nexus/common/typedef.h"

namespace nexus {

class RpsMeter {
 public:
  RpsMeter(double window_span_second, size_t history_length,
           TimePoint start_time);
  void Hit(TimePoint time);
  std::optional<AvgStd> Get(TimePoint now);

 private:
  void PopLeft(int64_t time_ns);
  size_t Index(int64_t time_ns);

  int64_t window_span_ns_;
  size_t history_length_;
  int64_t earliest_time_ns_;

  // TODO: use a circular buffer
  std::deque<size_t> counters_;
};

}  // namespace nexus

#endif
