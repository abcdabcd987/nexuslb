#include "nexus/dispatcher/rankmt/common.h"

#include <gflags/gflags.h>

using nexus::dispatcher::rankmt::RankmtConfig;

DEFINE_uint32(rankmt_dctrl, RankmtConfig::Default().ctrl_latency.count() / 1000,
              "Rankmt: control plane latency in microseconds.");
DEFINE_uint32(rankmt_ddata, RankmtConfig::Default().data_latency.count() / 1000,
              "Rankmt: data plane latency in microseconds.");
DEFINE_uint32(rankmt_dresp, RankmtConfig::Default().resp_latency.count() / 1000,
              "Rankmt: result latency in microseconds.");
DEFINE_uint32(rankmt_rpsmeter_rate,
              RankmtConfig::Default().rpsmeter_rate.count() / 1000000,
              "Rankmt: RpsMeter sample rate in milliseconds.");
DEFINE_uint64(rankmt_rpsmeter_window, RankmtConfig::Default().rpsmeter_window,
              "Rankmt: RpsMeter window length.");

namespace nexus {
namespace dispatcher {
namespace rankmt {

RankmtConfig RankmtConfig::FromFlags() {
  RankmtConfig rankmt;
  rankmt.ctrl_latency = std::chrono::microseconds(FLAGS_rankmt_dctrl);
  rankmt.data_latency = std::chrono::microseconds(FLAGS_rankmt_ddata);
  rankmt.resp_latency = std::chrono::microseconds(FLAGS_rankmt_dresp);
  rankmt.rpsmeter_rate = std::chrono::milliseconds(FLAGS_rankmt_rpsmeter_rate);
  rankmt.rpsmeter_window = FLAGS_rankmt_rpsmeter_window;
  return rankmt;
}

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
