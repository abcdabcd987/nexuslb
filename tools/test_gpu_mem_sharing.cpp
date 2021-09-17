#include <glog/logging.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <numeric>
#include <tuple>
#include <utility>
#include <vector>

#include "nexus/backend/tensorflow_wrapper.h"
#include "nexus/common/device.h"
#include "nexus/common/time_util.h"

namespace tf = nexus::backend::tf;
using nexus::Clock;
using nexus::TimePoint;

constexpr size_t kBatchSize = 64;
constexpr size_t kResolution = 224;
constexpr size_t kWarmup = 10;
constexpr size_t kRepeats = 20;

std::pair<double, double> GetAvgStd(const std::vector<double>& v) {
  double sum = std::accumulate(v.begin(), v.end(), 0.0);
  double mean = sum / v.size();

  std::vector<double> diff(v.size());
  std::transform(v.begin(), v.end(), diff.begin(),
                 [mean](double x) { return x - mean; });
  double sq_sum =
      std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
  double stdev = std::sqrt(sq_sum / v.size());

  return {mean, stdev};
}

size_t GetGpuMemoryUsage() {
  size_t free_mem, total_mem;
  NEXUS_CUDA_CHECK(cudaSetDevice(0));
  NEXUS_CUDA_CHECK(cudaMemGetInfo(&free_mem, &total_mem));
  return total_mem - free_mem;
}

std::string LookupModelInputName(const std::string& pb_path) { return "input"; }

std::string LookupModelOutputName(const std::string& pb_path) {
  if (pb_path.find("inception_v3") != std::string::npos) {
    return "InceptionV3/Predictions/Softmax";
  }
  return "output";
}

long RunSession(const std::string& pb_path, tf::Session& session,
                tf::Tensor& input) {
  const auto& input_name = LookupModelInputName(pb_path);
  const auto& output_name = LookupModelOutputName(pb_path);
  auto st = Clock::now();
  (void)session.Run({{input_name, input}}, {output_name});
  auto ed = Clock::now();
  auto elapse_ns = (ed - st).count();
  return elapse_ns;
}

struct SingleResult {
  double first;
  double avg;
  double std;
  size_t param;
  size_t peak;
  size_t usage;

  void Write(FILE* f) const {
    int n = fprintf(f, "%.16e %.16e %.16e %zu %zu %zu\n", first, avg, std,
                    param, peak, usage);
    CHECK_GT(n, 0);
  }

  void Parse(FILE* f) {
    int n = fscanf(f, "%le %le %le %zu %zu %zu", &first, &avg, &std, &param,
                   &peak, &usage);
    CHECK_GT(n, 0);
  }

  void Aggregate(const SingleResult& other) {
    first += other.first;
    avg += other.avg;
    std += other.std;
    param += other.param;
    peak = std::max(peak, other.peak);
    usage = std::max(usage, other.usage);
  }

  void Print(const char* header) const {
    printf("\"%s\",%.16e,%.16e,%.16e,%.16e,%.16e,%.16e\n", header, first * 1e3,
           avg * 1e3, std * 1e3, param * 1e-6, peak * 1e-6, usage * 1e-6);
  }
};

void RunSingleModel(const std::string& pb_path,
                    const std::string& result_filename) {
  SingleResult result;
  size_t begin_usage = GetGpuMemoryUsage();
  std::string visible_device_list = "0";
  double per_process_gpu_memory_fraction = 0;
  LOG(INFO) << "Create Session " << pb_path;
  tf::Session session(visible_device_list, per_process_gpu_memory_fraction,
                      pb_path);
  auto input = session.NewTensor(tf::DataType::DT_FLOAT,
                                 {kBatchSize, kResolution, kResolution, 3});

  // First run
  {
    auto elapse_ns = RunSession(pb_path, session, input);
    result.first = elapse_ns * 1e-9;
  }

  // Warmup
  for (size_t i = 0; i < kWarmup; ++i) {
    LOG(INFO) << "Warmup " << (i + 1) << "/" << kWarmup << " " << pb_path;
    RunSession(pb_path, session, input);
  }

  // Repeat trials
  std::vector<double> elapses;
  long total_elapse_ns = 0;
  for (size_t repeat = 0; repeat < kRepeats; ++repeat) {
    auto elapse_ns = RunSession(pb_path, session, input);
    elapses.push_back(elapse_ns * 1e-9);
    LOG(INFO) << "Trial " << (repeat + 1) << "/" << kRepeats << " "
              << (elapse_ns * 1e-6) << "ms " << pb_path;
  }

  size_t final_usage = GetGpuMemoryUsage();
  std::tie(result.avg, result.std) = GetAvgStd(elapses);
  result.param = session.GetBytesInUse();
  result.peak = session.GetPeakBytesInUse();
  result.usage = final_usage - begin_usage;

  FILE* f = fopen(result_filename.c_str(), "w");
  CHECK(f != nullptr);
  result.Write(f);
  fclose(f);
}

struct MultiResult {
  std::vector<SingleResult> results;

  void Write(FILE* f) const {
    CHECK_GT(fprintf(f, "%zu\n", results.size()), 0);
    for (const auto& result : results) {
      result.Write(f);
    }
  }

  void Parse(FILE* f) {
    size_t sz;
    CHECK_GT(fscanf(f, "%zu", &sz), 0);
    results.clear();
    results.resize(sz);
    for (size_t i = 0; i < sz; ++i) {
      results[i].Parse(f);
    }
  }
};

void RunMultipleModels(std::vector<std::string> pb_paths,
                       const std::string& result_filename) {
  MultiResult result;
  size_t begin_usage = GetGpuMemoryUsage();
  std::vector<std::vector<double>> elapses;
  std::vector<std::unique_ptr<tf::Session>> sessions;
  std::vector<tf::Tensor> inputs;
  sessions.reserve(pb_paths.size());
  result.results.resize(pb_paths.size());
  elapses.resize(pb_paths.size());
  for (size_t idx = 0; idx < pb_paths.size(); ++idx) {
    std::string visible_device_list = "0";
    double per_process_gpu_memory_fraction = 0;
    LOG(INFO) << "Create Session " << pb_paths[idx];
    auto session = std::make_unique<tf::Session>(
        visible_device_list, per_process_gpu_memory_fraction, pb_paths[idx]);
    auto input = session->NewTensor(tf::DataType::DT_FLOAT,
                                    {kBatchSize, kResolution, kResolution, 3});

    // First run
    auto elapse_ns = RunSession(pb_paths[idx], *session, input);
    result.results[idx].first = elapse_ns * 1e-9;

    sessions.push_back(std::move(session));
    inputs.push_back(input);
    elapses[idx].reserve(kRepeats);
  }

  // Shuffle repeat trials
  std::vector<size_t> idxs;
  idxs.reserve(pb_paths.size() * kRepeats);
  for (size_t i = 0; i < pb_paths.size(); ++i) {
    for (size_t repeat = 0; repeat < kRepeats; ++repeat) {
      idxs.push_back(i);
    }
  }
  std::random_shuffle(idxs.begin(), idxs.end());

  // Warmup
  for (size_t warmup = 0; warmup < kWarmup; ++warmup) {
    for (size_t idx = 0; idx < pb_paths.size(); ++idx) {
      RunSession(pb_paths[idx], *sessions[idx], inputs[idx]);
      LOG(INFO) << "Warmup " << (warmup * pb_paths.size() + idx + 1) << "/"
                << (kWarmup * pb_paths.size()) << " " << pb_paths[idx];
    }
  }

  // Run repeat trials
  for (size_t i = 0; i < idxs.size(); ++i) {
    auto idx = idxs[i];
    auto elapse_ns = RunSession(pb_paths[idx], *sessions[idx], inputs[idx]);
    elapses[idx].push_back(elapse_ns * 1e-9);
    LOG(INFO) << "Trial " << (i + 1) << "/" << idxs.size() << " "
              << (elapse_ns * 1e-6) << "ms " << pb_paths[idx];
  }

  size_t final_usage = GetGpuMemoryUsage();
  for (size_t idx = 0; idx < pb_paths.size(); ++idx) {
    auto& r = result.results[idx];
    std::tie(r.avg, r.std) = GetAvgStd(elapses[idx]);
    r.param = sessions[idx]->GetBytesInUse();
    r.peak = sessions[idx]->GetPeakBytesInUse();
    r.usage = final_usage - begin_usage;
  }

  FILE* f = fopen(result_filename.c_str(), "w");
  CHECK(f != nullptr);
  result.Write(f);
  fclose(f);
}

std::string MakeTempFilename() {
  char filename[] = "/tmp/test_gpu_mem_sharing-XXXXXX";
  int result_fd = mkstemp(filename);
  close(result_fd);
  return filename;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    fprintf(stderr, "usage: %s <model1.pb> [model2.pb] [model3.pb] ...\n",
            argv[0]);
    return 1;
  }
  FLAGS_alsologtostderr = 1;
  FLAGS_colorlogtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  auto temp = MakeTempFilename();

  printf(
      "Model Path,"
      "First Run Elapse (ms),"
      "avg(Forward Elapse) (ms),"
      "std(Forward Elapse) (ms),"
      "Parameter Memory Usage (MB),"
      "Peak Memory Usage (MB),"
      "Memory Usage Reported by CUDA (MB)\n");

  // Run separately
  printf("Run Separately\n");
  std::vector<std::string> pb_paths;
  SingleResult aggregate = {};
  for (int i = 1; i < argc; ++i) {
    pb_paths.push_back(argv[i]);
    fflush(stdout);
    auto child_pid = fork();
    if (child_pid == 0) {
      RunSingleModel(argv[i], temp);
      std::exit(0);
    }

    int wstatus;
    waitpid(child_pid, &wstatus, 0);
    CHECK(WIFEXITED(wstatus));
    CHECK_EQ(WEXITSTATUS(wstatus), 0);
    SingleResult r;
    FILE* f = fopen(temp.c_str(), "r");
    CHECK(f != nullptr);
    r.Parse(f);
    fclose(f);
    r.Print(argv[i]);
    aggregate.Aggregate(r);
  }
  aggregate.Print("AGGREGATE");

  // Mix run
  printf("Mix Run\n");
  aggregate = SingleResult{};
  {
    fflush(stdout);
    auto child_pid = fork();
    if (child_pid == 0) {
      RunMultipleModels(pb_paths, temp);
      std::exit(0);
    }

    int wstatus;
    waitpid(child_pid, &wstatus, 0);
    CHECK(WIFEXITED(wstatus));
    CHECK_EQ(WEXITSTATUS(wstatus), 0);
    MultiResult result;
    FILE* f = fopen(temp.c_str(), "r");
    CHECK(f != nullptr);
    result.Parse(f);
    fclose(f);
    for (int i = 1; i < argc; ++i) {
      auto& r = result.results.at(i - 1);
      r.Print(argv[i]);
      aggregate.Aggregate(r);
    }
  }
  aggregate.param /= pb_paths.size();
  aggregate.Print("AGGREGATE");

  unlink(temp.c_str());
}
