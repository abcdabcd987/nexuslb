#include "shepherd/common.h"

#include <gflags/gflags.h>

using nexus::shepherd::ShepherdConfig;

DEFINE_uint32(shepherd_dctrl,
              ShepherdConfig::Default().ctrl_latency.count() / 1000,
              "Shepherd: control plane latency in microseconds.");
DEFINE_uint32(shepherd_ddata,
              ShepherdConfig::Default().data_latency.count() / 1000,
              "Shepherd: data plane latency in microseconds.");
DEFINE_double(shepherd_preempt_lambda, ShepherdConfig::Default().preempt_lambda,
              "Shepherd: lambda for preemptive scheduling.");

namespace nexus::shepherd {

ShepherdConfig ShepherdConfig::FromFlags() {
  ShepherdConfig config;
  config.ctrl_latency = std::chrono::microseconds(FLAGS_shepherd_dctrl);
  config.data_latency = std::chrono::microseconds(FLAGS_shepherd_ddata);
  config.preempt_lambda = FLAGS_shepherd_preempt_lambda;
  return config;
}

}  // namespace nexus::shepherd
