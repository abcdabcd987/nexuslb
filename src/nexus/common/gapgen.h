#ifndef NEXUS_COMMON_GAPGEN_H_
#define NEXUS_COMMON_GAPGEN_H_

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <random>
#include <string>

namespace nexus {

class GapGenerator {
 public:
  GapGenerator(double rps) : rps_(rps) {}
  double rps() const { return rps_; }
  virtual std::string Name() const = 0;
  virtual double Next() = 0;

 private:
  double rps_;
};

class GapGeneratorBuilder {
 public:
  GapGeneratorBuilder();
  static GapGeneratorBuilder Parse(const std::string str);
  std::unique_ptr<GapGenerator> Build(int64_t seed, double rps) const;
  operator bool() const;
  const std::string& Name() const;

 private:
  using BuildFn =
      std::function<std::unique_ptr<GapGenerator>(int64_t seed, double rps)>;
  GapGeneratorBuilder(std::string name, BuildFn build_fn);

  std::string name_;
  BuildFn build_fn_;
};

class ConstGapGenerator : public GapGenerator {
 public:
  ConstGapGenerator(double rps) : GapGenerator(rps) {}

  double Next() override { return 1 / rps(); }

  std::string Name() const override { return "const"; }
};

class ExpGapGenerator : public GapGenerator {
 public:
  ExpGapGenerator(int64_t seed, double rps)
      : GapGenerator(rps), rand_engine_(seed) {}

  double Next() override {
    auto rand = uniform_(rand_engine_);
    return -std::log(1.0 - rand) / rps();
  }

  std::string Name() const override { return "exp"; }

 private:
  std::mt19937 rand_engine_;
  std::uniform_real_distribution<double> uniform_;
};

class GammaGapGenerator : public GapGenerator {
 public:
  GammaGapGenerator(int64_t seed, double rps, double shape)
      : GapGenerator(rps),
        rand_engine_(seed),
        dist_(shape, 1.0 / (shape * rps)) {}

  double Next() override { return dist_(rand_engine_); }

  std::string Name() const override { return "exp"; }

 private:
  std::mt19937 rand_engine_;
  std::uniform_real_distribution<double> uniform_;
  std::gamma_distribution<double> dist_;
};

}  // namespace nexus

#endif
