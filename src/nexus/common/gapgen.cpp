#include "nexus/common/gapgen.h"

#include <glog/logging.h>

#include <cstring>
#include <string>

namespace nexus {

GapGeneratorBuilder::GapGeneratorBuilder() = default;

GapGeneratorBuilder::GapGeneratorBuilder(std::string name, BuildFn build_fn)
    : name_(std::move(name)), build_fn_(std::move(build_fn)){};

const std::string& GapGeneratorBuilder::Name() const { return name_; }

std::unique_ptr<GapGenerator> GapGeneratorBuilder::Build(int64_t seed,
                                                         double rps) const {
  return build_fn_(seed, rps);
}

GapGeneratorBuilder::operator bool() const {
  return static_cast<bool>(build_fn_);
}

GapGeneratorBuilder GapGeneratorBuilder::Parse(const std::string str) {
  if (str == "const") {
    return GapGeneratorBuilder(str, [](int64_t seed, double rps) {
      return std::make_unique<ConstGapGenerator>(rps);
    });
  }
  if (str == "exp") {
    return GapGeneratorBuilder(str, [](int64_t seed, double rps) {
      return std::make_unique<ExpGapGenerator>(seed, rps);
    });
  }
  if (str.rfind("gamma:", 0) == 0) {
    auto shape_str = str.substr(std::strlen("gamma:"));
    double shape;
    try {
      shape = std::stod(shape_str);
    } catch (const std::exception& ex) {
      LOG(ERROR) << "GapGeneratorBuilder::Parse: Failed to parse gamma shape. "
                 << "shape_str: " << shape_str;
      return GapGeneratorBuilder();
    }
    return GapGeneratorBuilder(str, [shape](int64_t seed, double rps) {
      return std::make_unique<GammaGapGenerator>(seed, rps, shape);
    });
  }
  LOG(ERROR) << "GapGeneratorBuilder::Parse: Unknown gap type. str: " << str;
  return GapGeneratorBuilder();
}

}  // namespace nexus
