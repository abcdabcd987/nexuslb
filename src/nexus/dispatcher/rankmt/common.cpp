#include "nexus/dispatcher/rankmt/common.h"

namespace nexus {
namespace dispatcher {
namespace rankmt {

std::chrono::nanoseconds EstimateExecElapse(const ModelProfile& profile,
                                            uint32_t batch_size) {
  double micros = 0;
  micros += profile.GetPreprocessLatency();
  micros += profile.GetPostprocessLatency();
  micros += profile.GetForwardLatency(batch_size);
  return std::chrono::nanoseconds(static_cast<long>(micros * 1e3));
}

}  // namespace rankmt
}  // namespace dispatcher
}  // namespace nexus
