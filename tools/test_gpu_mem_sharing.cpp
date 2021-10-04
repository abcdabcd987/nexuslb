#include <glog/logging.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <boost/container_hash/hash.hpp>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <ios>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "nexus/backend/tensorflow_wrapper.h"
#include "nexus/common/device.h"
#include "nexus/common/time_util.h"
#include "tensorflow/core/framework/graph.pb.h"

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

bool StringEndsWith(const std::string& str, const std::string& end) {
  if (str.size() < end.size()) return false;
  return 0 == str.compare(str.size() - end.size(), end.size(), end);
}

void RandomizeWeights(tensorflow::GraphDef& graph_def, uint64_t seed) {
  std::vector<float> buf;
  std::mt19937 gen(seed);
  std::uniform_real_distribution<float> dis;
  size_t generated_bytes = 0;
  for (auto& node : *graph_def.mutable_node()) {
    if (!StringEndsWith(node.name(), "weights")) continue;
    auto iter = node.mutable_attr()->find("value");
    if (iter == node.mutable_attr()->end()) continue;
    if (!iter->second.has_tensor()) continue;
    auto& tensor = *iter->second.mutable_tensor();
    LOG_IF(FATAL, tensor.dtype() != tensorflow::DT_FLOAT)
        << "Unsupported dtype: " << tensorflow::DataType_Name(tensor.dtype());

    size_t element_size = 4;
    size_t len = tensor.tensor_content().size();
    size_t num_elements = len / element_size;
    CHECK_EQ(element_size * num_elements, len);
    buf.resize(num_elements);
    std::generate(buf.begin(), buf.end(), [&gen, &dis] { return dis(gen); });
    generated_bytes += len;

    auto* beg = reinterpret_cast<char*>(buf.data());
    tensor.mutable_tensor_content()->assign(beg, beg + len);
  }
  LOG(INFO) << "Replaced " << (generated_bytes / 1e6) << "MB parameters out of "
            << (graph_def.ByteSizeLong() / 1e6) << "MB";
}

std::string MakeTempFilename() {
  return "/tmp/test_gpu_mem_sharing" + std::to_string(getpid());
}

std::string LookupModelInputName(const std::string& pb_path) { return "input"; }

std::string LookupModelOutputName(const std::string& pb_path) {
  if (pb_path.find("inception_v3") != std::string::npos) {
    return "InceptionV3/Predictions/Softmax";
  }
  return "output";
}

struct ModelPbSpec {
  std::string pb_path;
  size_t replica;

  ModelPbSpec(std::string path, size_t copy)
      : pb_path(std::move(path)), replica(copy) {}

  std::string ToString() const {
    std::ostringstream ss;
    ss << pb_path << '#' << replica;
    return ss.str();
  }

  std::string GenerateGraphDef() const {
    tensorflow::GraphDef graph_def;
    {
      std::ifstream istream(pb_path, std::ios::in | std::ios::binary);
      CHECK(graph_def.ParseFromIstream(&istream));
    }

    size_t seed = 0;
    boost::hash_combine(seed, pb_path);
    boost::hash_combine(seed, replica);
    RandomizeWeights(graph_def, seed);

    std::ostringstream ss;
    ss << MakeTempFilename() << "-" << seed << ".pb";
    auto filename = ss.str();

    std::ofstream ostream(filename, std::ios::out | std::ios::binary);
    CHECK(graph_def.SerializeToOstream(&ostream));
    return filename;
  }
};

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
  double load;
  double first;
  double avg;
  double std;
  size_t param;
  size_t tmp;
  size_t usage;

  void Write(FILE* f) const {
    int n = fprintf(f, "%.16e %.16e %.16e %.16e %zu %zu %zu\n", load, first,
                    avg, std, param, tmp, usage);
    CHECK_GT(n, 0);
  }

  void Parse(FILE* f) {
    int n = fscanf(f, "%le %le %le %le %zu %zu %zu", &load, &first, &avg, &std,
                   &param, &tmp, &usage);
    CHECK_GT(n, 0);
  }

  void Aggregate(const SingleResult& other) {
    load += other.load;
    first += other.first;
    avg += other.avg;
    std += other.std;
    param += other.param;
    tmp = std::max(tmp, other.tmp);
    usage = std::max(usage, other.usage);
  }

  void Print(const std::string& header) const {
    printf("\"%s\",%.16e,%.16e,%.16e,%.16e,%.16e,%.16e,%.16e\n", header.c_str(),
           load * 1e3, first * 1e3, avg * 1e3, std * 1e3, param * 1e-6,
           tmp * 1e-6, usage * 1e-6);
  }
};

void RunSingleModel(const ModelPbSpec& model_pb,
                    const std::string& result_filename) {
  SingleResult result;
  size_t begin_usage = GetGpuMemoryUsage();
  std::string visible_device_list = "0";
  double per_process_gpu_memory_fraction = 0;
  LOG(INFO) << "Create Session " << model_pb.ToString();
  auto generated_pb = model_pb.GenerateGraphDef();
  auto st = Clock::now();
  tf::Session session(visible_device_list, per_process_gpu_memory_fraction,
                      generated_pb);
  auto ed = Clock::now();
  result.load = (ed - st).count() * 1e-9;
  unlink(generated_pb.c_str());
  auto input = session.NewTensor(tf::DataType::DT_FLOAT,
                                 {kBatchSize, kResolution, kResolution, 3});

  // First run
  {
    auto elapse_ns = RunSession(model_pb.pb_path, session, input);
    result.first = elapse_ns * 1e-9;
  }

  // Warmup
  for (size_t i = 0; i < kWarmup; ++i) {
    LOG(INFO) << "Warmup " << (i + 1) << "/" << kWarmup << " "
              << model_pb.ToString();
    RunSession(model_pb.pb_path, session, input);
  }

  // Repeat trials
  std::vector<double> elapses;
  long total_elapse_ns = 0;
  for (size_t repeat = 0; repeat < kRepeats; ++repeat) {
    auto elapse_ns = RunSession(model_pb.pb_path, session, input);
    elapses.push_back(elapse_ns * 1e-9);
    LOG(INFO) << "Trial " << (repeat + 1) << "/" << kRepeats << " "
              << (elapse_ns * 1e-6) << "ms " << model_pb.ToString();
  }

  size_t final_usage = GetGpuMemoryUsage();
  std::tie(result.avg, result.std) = GetAvgStd(elapses);
  result.param = session.GetBytesInUse();
  result.tmp = session.GetPeakBytesInUse() - result.param;
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

void RunMultipleModels(std::vector<ModelPbSpec> model_pbs,
                       const std::string& result_filename) {
  MultiResult result;
  size_t begin_usage = GetGpuMemoryUsage();
  std::vector<std::vector<double>> elapses;
  std::vector<std::unique_ptr<tf::Session>> sessions;
  std::vector<tf::Tensor> inputs;
  sessions.reserve(model_pbs.size());
  result.results.resize(model_pbs.size());
  elapses.resize(model_pbs.size());
  for (size_t idx = 0; idx < model_pbs.size(); ++idx) {
    std::string visible_device_list = "0";
    double per_process_gpu_memory_fraction = 0;
    LOG(INFO) << "Create Session " << model_pbs[idx].ToString();
    auto generated_pb = model_pbs[idx].GenerateGraphDef();
    auto st = Clock::now();
    auto session = std::make_unique<tf::Session>(
        visible_device_list, per_process_gpu_memory_fraction, generated_pb);
    auto ed = Clock::now();
    result.results[idx].load = (ed - st).count() * 1e-9;
    unlink(generated_pb.c_str());
    auto input = session->NewTensor(tf::DataType::DT_FLOAT,
                                    {kBatchSize, kResolution, kResolution, 3});

    // First run
    auto elapse_ns = RunSession(model_pbs[idx].pb_path, *session, input);
    result.results[idx].first = elapse_ns * 1e-9;

    sessions.push_back(std::move(session));
    inputs.push_back(input);
    elapses[idx].reserve(kRepeats);
  }

  // Shuffle repeat trials
  std::vector<size_t> idxs;
  idxs.reserve(model_pbs.size() * kRepeats);
  for (size_t i = 0; i < model_pbs.size(); ++i) {
    for (size_t repeat = 0; repeat < kRepeats; ++repeat) {
      idxs.push_back(i);
    }
  }
  std::random_shuffle(idxs.begin(), idxs.end());

  // Warmup
  for (size_t warmup = 0; warmup < kWarmup; ++warmup) {
    for (size_t idx = 0; idx < model_pbs.size(); ++idx) {
      RunSession(model_pbs[idx].pb_path, *sessions[idx], inputs[idx]);
      LOG(INFO) << "Warmup " << (warmup * model_pbs.size() + idx + 1) << "/"
                << (kWarmup * model_pbs.size()) << " "
                << model_pbs[idx].ToString();
    }
  }

  // Run repeat trials
  for (size_t i = 0; i < idxs.size(); ++i) {
    auto idx = idxs[i];
    auto elapse_ns =
        RunSession(model_pbs[idx].pb_path, *sessions[idx], inputs[idx]);
    elapses[idx].push_back(elapse_ns * 1e-9);
    LOG(INFO) << "Trial " << (i + 1) << "/" << idxs.size() << " "
              << (elapse_ns * 1e-6) << "ms " << model_pbs[idx].ToString();
  }

  size_t final_usage = GetGpuMemoryUsage();
  for (size_t idx = 0; idx < model_pbs.size(); ++idx) {
    auto& r = result.results[idx];
    std::tie(r.avg, r.std) = GetAvgStd(elapses[idx]);
    r.param = sessions[idx]->GetBytesInUse();
    r.tmp = sessions[idx]->GetPeakBytesInUse() - r.param;
    r.usage = final_usage - begin_usage;
  }

  FILE* f = fopen(result_filename.c_str(), "w");
  CHECK(f != nullptr);
  result.Write(f);
  fclose(f);
}

int main(int argc, char** argv) {
  if (argc < 3) {
    fprintf(stderr,
            "usage: %s <num_model_replicas> <model1.pb> [model2.pb] "
            "[model3.pb] ...\n",
            argv[0]);
    return 1;
  }
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  size_t num_model_replicas = std::stoul(argv[1]);
  auto temp = MakeTempFilename() + ".txt";

  std::vector<ModelPbSpec> model_pbs;
  for (int i = 2; i < argc; ++i) {
    for (size_t replica = 0; replica < num_model_replicas; ++replica) {
      model_pbs.emplace_back(argv[i], replica);
    }
  }

  printf(
      "Model Path,"
      "Load Elapse (ms),"
      "First Run Elapse (ms),"
      "avg(Forward Elapse) (ms),"
      "std(Forward Elapse) (ms),"
      "Parameter Memory Usage (MB),"
      "Temporary Memory Usage (MB),"
      "Memory Usage Reported by CUDA (MB)\n");

  // Mix run
  printf("Mix Run\n");
  SingleResult aggregate = {};
  {
    fflush(stdout);
    auto child_pid = fork();
    if (child_pid == 0) {
      RunMultipleModels(model_pbs, temp);
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
    CHECK_EQ(result.results.size(), model_pbs.size());
    for (size_t i = 0; i < model_pbs.size(); ++i) {
      const auto& r = result.results[i];
      r.Print(model_pbs[i].ToString());
      aggregate.Aggregate(r);
    }
  }
  aggregate.param /= model_pbs.size();
  aggregate.Print("AGGREGATE");

  // Run separately
  printf("Run Separately\n");
  aggregate = SingleResult{};
  for (const auto& model_pb : model_pbs) {
    fflush(stdout);
    auto child_pid = fork();
    if (child_pid == 0) {
      RunSingleModel(model_pb, temp);
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
    r.Print(model_pb.ToString());
    aggregate.Aggregate(r);
  }
  aggregate.Print("AGGREGATE");

  unlink(temp.c_str());
}
