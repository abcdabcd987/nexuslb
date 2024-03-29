#include <gflags/gflags.h>
#include <glog/logging.h>
#include <time.h>
#include <yaml-cpp/yaml.h>

#include <boost/filesystem.hpp>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "nexus/backend/model_exec.h"
#include "nexus/backend/model_ins.h"
#include "nexus/common/block_queue.h"
#include "nexus/common/device.h"
#include "nexus/common/model_db.h"
#include "nexus/common/typedef.h"
#include "nexus/proto/nnquery.pb.h"

DEFINE_int32(gpu, 0, "GPU device id");
DEFINE_string(framework, "", "Framework");
DEFINE_string(model, "", "Model name");
DEFINE_int32(model_version, 1, "Version");
DEFINE_int32(min_batch, 1, "Minimum batch size");
DEFINE_int32(max_batch, 256, "Maximum batch size");
DEFINE_string(output, "", "Output file");
DEFINE_int32(height, 0, "Image height");
DEFINE_int32(width, 0, "Image width");
DEFINE_bool(share_prefix, false, "Enable share prefix");
DEFINE_int32(repeat, 10, "Repeat times for profiling");

namespace nexus {
namespace backend {

using duration = std::chrono::microseconds;
namespace fs = boost::filesystem;

class ModelProfiler {
 public:
  ModelProfiler(int gpu, const std::string& framework,
                const std::string& model_name, int model_version, int height,
                int width)
      : gpu_(gpu) {
    model_info_ = ModelDatabase::Singleton().GetModelInfo(framework, model_name,
                                                          model_version);
    CHECK(model_info_ != nullptr) << "Cannot find model info for " << framework
                                  << ":" << model_name << ":" << model_version;
    // Init model session
    model_sess_.set_framework(framework);
    model_sess_.set_model_name(model_name);
    model_sess_.set_version(model_version);
    model_sess_.set_latency_sla(50000);
    if (height > 0) {
      CHECK_GT(width, 0) << "Height and width must be set together";
      model_sess_.set_image_height(height);
      model_sess_.set_image_width(width);
    } else {
      if ((*model_info_)["resizable"] &&
          (*model_info_)["resizable"].as<bool>()) {
        // Set default image size for resizable CNN
        model_sess_.set_image_height(
            (*model_info_)["image_height"].as<uint32_t>());
        model_sess_.set_image_width(
            (*model_info_)["image_width"].as<uint32_t>());
      }
    }
    LOG(INFO) << model_sess_.DebugString();
    model_sessions_.push_back(ModelSessionToString(model_sess_));
    LOG(INFO) << "Profile model " << ModelSessionToProfileID(model_sess_);
    cpu_device_ = DeviceManager::Singleton().GetCPUDevice();
#ifdef USE_GPU
    // Init GPU device
    NEXUS_CUDA_CHECK(cudaSetDevice(gpu_));
    gpu_device_ = DeviceManager::Singleton().GetGPUDevice(gpu_);
#else
    if (gpu_ != -1) {
      LOG(FATAL) << "The code is compiled without USE_GPU. Please set "
                    "`-gpu=-1` to profile on CPU.";
    }
#endif
  }

  void Profile(int min_batch, int max_batch, const std::string output = "",
               int repeat = 10) {
    std::vector<uint64_t> preprocess_lats;
    std::vector<uint64_t> postprocess_lats;
    ModelInstanceConfig config;
    config.add_model_session()->CopyFrom(model_sess_);
    if (FLAGS_share_prefix) {
      std::vector<std::string> share_models =
          ModelDatabase::Singleton().GetPrefixShareModels(
              ModelSessionToModelID(model_sess_));
      for (auto model_id : share_models) {
        LOG(INFO) << model_id;
        auto share_sess = config.add_model_session();
        ParseModelID(model_id, share_sess);
        share_sess->set_latency_sla(50000);
        if (model_sess_.image_height() > 0) {
          share_sess->set_image_height(model_sess_.image_height());
          share_sess->set_image_width(model_sess_.image_width());
        }
        model_sessions_.push_back(ModelSessionToString(*share_sess));
      }
    }
    LOG(INFO) << config.DebugString();
    BlockPriorityQueue<Task> task_queue;
    const size_t input_len =
        model_sess_.image_width() * model_sess_.image_height() * 3;

    // skip preprocess
    // TODO: refactor preprocessing. also see
    // BackendServer::HandleFetchImageReply
    std::this_thread::sleep_for(std::chrono::microseconds(200));
    preprocess_lats.push_back(200);
    preprocess_lats.push_back(200);
    LOG(INFO) << "Preprocess skipped";

    // output to file
    std::ostream* fout;
    if (output.length() == 0) {
      fout = &std::cout;
    } else {
      fout = new std::ofstream(output, std::ofstream::out);
    }

    if (FLAGS_share_prefix) {
      *fout << ModelSessionToProfileID(model_sess_) << "-prefix\n";
    } else {
      *fout << ModelSessionToProfileID(model_sess_) << "\n";
    }
#ifdef USE_GPU
    *fout << gpu_device_->device_name() << "\n";
    *fout << gpu_device_->uuid() << "\n";
#else
    *fout << cpu_device_->name() << "\n";
    *fout << "GenericCPU"
          << "\n";
#endif
    *fout << "Forward latency\n";
    *fout
        << "batch,latency(us),std(us),static memory(B),peak memory(B),repeat\n";
    *fout << std::flush;

    // forward and postprocess
    int dryrun = 4;
    for (int batch = min_batch; batch <= max_batch; ++batch) {
      config.set_batch(batch);
      config.set_max_batch(batch);
      auto model = std::make_shared<ModelExecutor>(gpu_, config, ModelIndex(0),
                                                   task_queue);
      std::vector<uint64_t> forward_lats;
      std::vector<std::shared_ptr<BatchPlanContext>> batchplans;
      batchplans.reserve(repeat + dryrun);
      for (int i = 0; i < repeat + dryrun; ++i) {
        BatchPlanProto batchplan_proto;
        batchplan_proto.set_model_index(0);
        batchplan_proto.set_plan_id(i);
        for (int j = 0; j < batch; ++j) {
          auto* query =
              batchplan_proto.add_queries()->mutable_query_without_input();
          query->set_global_id(i * batch + j);
          query->set_model_index(0);
        }
        batchplans.push_back(
            std::make_shared<BatchPlanContext>(batchplan_proto));
      }

      // start meansuring forward latency
      for (int i = 0; i < dryrun + repeat; ++i) {
        // Assign pre-allocated GPU memory
        auto plan = batchplans[i];
        plan->SetPinnedMemory(model->AcquirePinnedMemory());
        plan->SetInputArray(model->AcquireInputArray());

        // Create tasks
        for (int j = 0; j < batch; ++j) {
          // Fill junk inputs
          auto input = plan->pinned_memory()->Slice(input_len * j, input_len);
          std::mt19937 gen(i * batch + j);
          std::uniform_real_distribution<float> dis;
          float* beg = input->Data<float>();
          float* end = beg + input_len;
          std::generate(beg, end, [&gen, &dis] { return dis(gen); });

          // Create task
          auto task = std::make_shared<Task>();
          task->SetDeadline(std::chrono::milliseconds(1000000));
          task->query.set_global_id(i * batch + j);
          task->query.set_model_index(0);
          task->AppendInput(input);

          // Host2Device Memcpy
          plan->AddPreprocessedTask(task);
        }

        // Execute. Pre-allocated memory will be released in ExecuteBatchPlan.
        auto beg = std::chrono::high_resolution_clock::now();
        model->ExecuteBatchPlan(plan);
        auto end = std::chrono::high_resolution_clock::now();

        if (i < dryrun) continue;
        forward_lats.push_back(
            std::chrono::duration_cast<duration>(end - beg).count());
      }
      auto static_memory_usage = model->GetStaticMemoryUsage();
      auto peak_memory_usage = model->GetPeakMemoryUsage();
      for (int i = 0; i < batch * (repeat + dryrun); ++i) {
        auto task = task_queue.pop();
        CHECK_EQ(task->result.status(), CTRL_OK)
            << "Error detected: " << task->result.status();
        auto beg = std::chrono::high_resolution_clock::now();
        model->Postprocess(task);
        auto end = std::chrono::high_resolution_clock::now();
        if (i > 0 && postprocess_lats.size() < 2000) {
          postprocess_lats.push_back(
              std::chrono::duration_cast<duration>(end - beg).count());
        }
      }
      float mean, std;
      std::tie(mean, std) = GetStats<uint64_t>(forward_lats);
      CHECK_EQ(task_queue.size(), 0) << "Task queue is not empty";

      // output to file
      *fout << batch << "," << static_cast<int64_t>(mean) << ","
            << static_cast<int64_t>(std) << "," << static_memory_usage << ","
            << peak_memory_usage << "," << repeat << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(200));
    }

#ifdef USE_GPU
    LOG(INFO) << "Final free memory: " << gpu_device_->FreeMemory();
#endif

    // output to file
    float mean, std;
    *fout << "Preprocess latency (mean,std,repeat)\n";
    std::tie(mean, std) = GetStats<uint64_t>(preprocess_lats);
    *fout << mean << "," << std << "," << preprocess_lats.size() << "\n";
    *fout << "Postprocess latency (mean,std,repeat)\n";
    std::tie(mean, std) = GetStats<uint64_t>(postprocess_lats);
    *fout << mean << "," << std << "," << postprocess_lats.size() << "\n";
    *fout << std::flush;
    if (fout != &std::cout) {
      delete fout;
    }
  }

 private:
  template <class T>
  std::pair<float, float> GetStats(const std::vector<T>& lats) {
    float mean = 0.;
    float std = 0.;
    for (uint i = 0; i < lats.size(); ++i) {
      mean += lats[i];
    }
    mean /= lats.size();
    for (uint i = 0; i < lats.size(); ++i) {
      std += (lats[i] - mean) * (lats[i] - mean);
    }
    std = sqrt(std / (lats.size() - 1));
    return {mean, std};
  }

 private:
  int gpu_;
  ModelSession model_sess_;
  const YAML::Node* model_info_;
  std::string framework_;
  std::string model_name_;
  std::vector<std::string> model_sessions_;
  CPUDevice* cpu_device_;
#ifdef USE_GPU
  GPUDevice* gpu_device_;
#endif
};  // namespace backend

}  // namespace backend
}  // namespace nexus

int main(int argc, char** argv) {
  using namespace nexus;
  using namespace nexus::backend;

  // log to stderr
  FLAGS_logtostderr = 1;
  // Init glog
  google::InitGoogleLogging(argv[0]);
  // Parse command line flags
  google::ParseCommandLineFlags(&argc, &argv, true);
  // Setup backtrace on segfault
  google::InstallFailureSignalHandler();
  // Check flags
  CHECK_GT(FLAGS_framework.length(), 0) << "Missing framework";
  CHECK_GT(FLAGS_model.length(), 0) << "Missing model";
  if (FLAGS_framework == "tf_share" && FLAGS_share_prefix)
    LOG(FATAL) << "Cannot use --share_prefix on TFShare models";
  srand(time(NULL));
  ModelProfiler profiler(FLAGS_gpu, FLAGS_framework, FLAGS_model,
                         FLAGS_model_version, FLAGS_height, FLAGS_width);
  profiler.Profile(FLAGS_min_batch, FLAGS_max_batch, FLAGS_output,
                   FLAGS_repeat);
}
