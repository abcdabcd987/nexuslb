#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <numeric>
#include <opencv2/opencv.hpp>
#include <random>
#include <thread>
#include <utility>

#include "nexus/common/image.h"
#include "nexus/proto/nnquery.pb.h"

std::vector<unsigned char> GenerateRandomJPG(int rows, int cols) {
  std::default_random_engine gen(0xabcdabcd987LL);
  std::uniform_int_distribution<unsigned char> dist;
  cv::Mat mat(rows, cols, CV_8UC3);
  for (int i = 0; i < rows; ++i) {
    for (int j = 0; j < cols; ++j) {
      for (int k = 0; k < 3; ++k) {
        mat.at<unsigned char>(i, j, k) = dist(gen);
      }
    }
  }
  std::vector<unsigned char> buf;
  bool ok = cv::imencode(".jpg", mat, buf);
  CHECK(ok);
  return buf;
}

void Preprocess(const nexus::ValueProto& value, int resize,
                std::vector<float>* out) {
  cv::Mat image = nexus::DecodeImage(value.image(), nexus::CO_RGB);
  cv::Mat fimg;
  image.convertTo(fimg, CV_32FC3);
  cv::Mat resized(resize, resize, CV_32FC3, out->data());
  cv::resize(fimg, resized, cv::Size(resize, resize));
}

std::pair<double, double> CalcAvgStd(std::vector<long> xs) {
  double sum = std::accumulate(xs.begin(), xs.end(), 0.0);
  double avg = sum / xs.size();
  double sqr_sum = std::inner_product(xs.begin(), xs.end(), xs.begin(), 0.0);
  double std = std::sqrt(sqr_sum / xs.size() - avg * avg);
  return {avg, std};
}

class Benchmark {
 public:
  Benchmark(std::vector<unsigned char> encoded, int num_workers, int resize,
            int repeats)
      : encoded_(std::move(encoded)),
        num_workers_(num_workers),
        resize_(resize),
        repeats_(repeats) {}

  std::pair<double, double> Run() {
    warmup_repeats_ = static_cast<int>(repeats_ * 0.2);
    bench_repeats_ = static_cast<int>(repeats_ * 0.7);
    cooldown_repeats_ = static_cast<int>(repeats_ * 0.1);
    nano_elapses_.clear();
    start_time_ =
        std::chrono::system_clock::now() + std::chrono::milliseconds(10);
    std::vector<std::thread> workers;
    for (int i = 0; i < num_workers_; ++i) {
      workers.emplace_back(&Benchmark::DoAsWorker, this);
    }
    for (auto& t : workers) {
      t.join();
    }

    return CalcAvgStd(nano_elapses_);
  }

 private:
  void DoAsWorker() {
    using namespace std::chrono;

    nexus::ValueProto value_proto;
    auto* image_proto = value_proto.mutable_image();
    image_proto->set_data(encoded_.data(), encoded_.size());
    image_proto->set_format(nexus::ImageProto_ImageFormat_JPEG);
    image_proto->set_color(true);

    int target_size = resize_;
    std::vector<float> preprocessed(target_size * target_size * 3);
    std::vector<long> es;
    es.reserve(bench_repeats_);
    std::this_thread::sleep_until(start_time_);

    for (int i = 0; i < warmup_repeats_; ++i) {
      Preprocess(value_proto, target_size, &preprocessed);
    }
    for (int i = 0; i < bench_repeats_; ++i) {
      auto st = high_resolution_clock::now();
      Preprocess(value_proto, target_size, &preprocessed);
      auto ed = high_resolution_clock::now();
      auto nanos = duration_cast<nanoseconds>(ed - st).count();
      es.push_back(nanos);
    }
    for (int i = 0; i < cooldown_repeats_; ++i) {
      Preprocess(value_proto, target_size, &preprocessed);
    }
    {
      std::lock_guard lock(nano_elapses_mu_);
      nano_elapses_.reserve(nano_elapses_.size() + es.size());
      nano_elapses_.insert(nano_elapses_.end(), es.begin(), es.end());
    }
  }

  std::vector<unsigned char> encoded_;
  int num_workers_;
  int resize_;
  int repeats_;
  int warmup_repeats_;
  int bench_repeats_;
  int cooldown_repeats_;
  std::chrono::system_clock::time_point start_time_;
  std::mutex nano_elapses_mu_;
  std::vector<long> nano_elapses_;
};

DEFINE_int32(original_size, 224, "Client-side input image size");
DEFINE_int32(target_size, 224, "Neural-network input image size");
DEFINE_int32(repeats, 10000, "Repeat trials of each thread");

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InstallFailureSignalHandler();

  auto jpg = GenerateRandomJPG(FLAGS_original_size, FLAGS_original_size);
  int ncores = std::thread::hardware_concurrency();
  for (int num_workers = 1; num_workers <= ncores; ++num_workers) {
    Benchmark b(jpg, num_workers, FLAGS_target_size, FLAGS_repeats);
    auto [avg_ns, std_ns] = b.Run();
    auto avg_us = avg_ns / 1e3;
    auto std_us = std_ns / 1e3;
    printf(
        "num_workers:%3d    "
        "avg:%9.3fus    "
        "std:%9.3fus\n",
        num_workers, avg_us, std_us);
  }
}
