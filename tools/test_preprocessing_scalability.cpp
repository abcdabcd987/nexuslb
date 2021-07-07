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
#include <tuple>
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

std::pair<double, double> CalcAvgStd(std::vector<double> xs) {
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

  void Run() {
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

    std::vector<double> decode_micros, convert_micros, resize_micros;
    for (const auto& [d1, d2, d3] : nano_elapses_) {
      decode_micros.push_back(d1 / 1e3);
      convert_micros.push_back(d2 / 1e3);
      resize_micros.push_back(d3 / 1e3);
    }
    auto [decode_avg, decode_std] = CalcAvgStd(decode_micros);
    auto [convert_avg, convert_std] = CalcAvgStd(convert_micros);
    auto [resize_avg, resize_std] = CalcAvgStd(resize_micros);
    auto total_avg = decode_avg + convert_avg + resize_avg;
    auto total_std = decode_std + convert_std + resize_std;
    printf(
        "num_workers: %3d      "
        "decode: %9.3f+/-%7.3fus      "
        "convert: %9.3f+/-%7.3fus      "
        "resize: %9.3f+/-%7.3fus      "
        "total: %9.3f+/-%7.3fus      "
        "\n",
        num_workers_, decode_avg, decode_std, convert_avg, convert_std,
        resize_avg, resize_std, total_avg, total_std);

    return;
  }

 private:
  void DoAsWorker() {
    using namespace std::chrono;

    nexus::ValueProto value_proto;
    auto* image_proto = value_proto.mutable_image();
    image_proto->set_data(encoded_.data(), encoded_.size());
    image_proto->set_format(nexus::ImageProto_ImageFormat_JPEG);
    image_proto->set_color(true);

    std::vector<float> preprocessed(resize_ * resize_ * 3);
    std::vector<std::tuple<long, long, long>> es;
    es.reserve(bench_repeats_);
    std::this_thread::sleep_until(start_time_);

    for (int i = -warmup_repeats_, end = bench_repeats_ + cooldown_repeats_;
         i < end; ++i) {
      auto t0 = high_resolution_clock::now();
      cv::Mat image = nexus::DecodeImage(value_proto.image(), nexus::CO_RGB);
      auto t1 = high_resolution_clock::now();
      cv::Mat fimg;
      image.convertTo(fimg, CV_32FC3);
      auto t2 = high_resolution_clock::now();
      cv::Mat resized(resize_, resize_, CV_32FC3, preprocessed.data());
      cv::resize(fimg, resized, cv::Size(resize_, resize_));
      auto t3 = high_resolution_clock::now();

      if (0 <= i && i < bench_repeats_) {
        auto d1 = duration_cast<nanoseconds>(t1 - t0).count();
        auto d2 = duration_cast<nanoseconds>(t2 - t1).count();
        auto d3 = duration_cast<nanoseconds>(t3 - t2).count();
        es.emplace_back(d1, d2, d3);
      }
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
  std::vector<std::tuple<long, long, long>> nano_elapses_;
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
    b.Run();
  }
}
