#ifndef NEXUS_COMMON_SLEEP_PROFILE_H_
#define NEXUS_COMMON_SLEEP_PROFILE_H_

#include <optional>
#include <string>

namespace nexus {

class SleepProfile {
 public:
  SleepProfile(int slope_us, int intercept_us, int preprocess_us,
               int postprocess_us);
  static std::optional<SleepProfile> Parse(const std::string& framework);
  static bool MatchPrefix(const std::string& framework);

  static constexpr const char* kPrefix = "sleep#";
  static constexpr const char* kGpuDeviceName = "Sleep";

  int slope_us() const { return slope_us_; }
  int intercept_us() const { return intercept_us_; }
  int preprocess_us() const { return preprocess_us_; }
  int postprocess_us() const { return postprocess_us_; }
  int forward_us(uint32_t batch_size) const {
    return slope_us_ * batch_size + intercept_us_;
  }

 private:
  int slope_us_;
  int intercept_us_;
  int preprocess_us_;
  int postprocess_us_;
};

}  // namespace nexus

#endif
