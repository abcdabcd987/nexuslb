#include "nexus/common/sleep_profile.h"

#include <glog/logging.h>

#include <cstring>
#include <optional>
#include <vector>

#include "nexus/common/util.h"

namespace nexus {

SleepProfile::SleepProfile(int slope_us, int intercept_us, int preprocess_us,
                           int postprocess_us)
    : slope_us_(slope_us),
      intercept_us_(intercept_us),
      preprocess_us_(preprocess_us),
      postprocess_us_(postprocess_us) {}

std::optional<SleepProfile> SleepProfile::Parse(const std::string& framework) {
  if (MatchPrefix(framework)) {
    auto param_str = framework.substr(strlen(kPrefix));
    std::vector<std::string> params;
    SplitString(param_str, ',', &params);
    std::vector<int> p;
    for (const auto& param : params) {
      try {
        int num = std::stoi(param);
        p.push_back(num);
      } catch (...) {
        LOG(ERROR)
            << "Bad parameter for SleepProfile. Cannot parse int from string \""
            << param << "\".";
      }
    }
    if (p.size() != 4) {
      LOG(ERROR) << "Bad parameter for SleepModel. Got " << p.size()
                 << " parameters. Expected format: " << kPrefix
                 << "slope_us,intercept_us,preprocess_us,postprocess_us";
    }
    return SleepProfile(p[0], p[1], p[2], p[3]);
  }
  return std::nullopt;
}

bool SleepProfile::MatchPrefix(const std::string& framework) {
  return framework.rfind(kPrefix, 0) == 0;
}

}  // namespace nexus
