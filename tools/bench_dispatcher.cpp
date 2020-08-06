#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <boost/asio.hpp>
#include <chrono>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "nexus/common/model_def.h"
#include "nexus/proto/control.pb.h"

using boost::asio::ip::udp;
using nexus::DispatchReply;
using nexus::DispatchRequest;
using nexus::ModelSession;

DEFINE_string(host, "127.0.0.1", "Host of the dispatcher");
DEFINE_int32(port, 7003, "Port of the dispatcher");
DEFINE_string(model_session_id, "tensorflow:resnet_0:1:224x224:50",
              "ModelSession ID");
DEFINE_int32(bench_seconds, 60, "Duration of the benchmark.");
DEFINE_int32(num_workers, 1, "Number of concurrent workers.");
DEFINE_int32(max_flying, 5, "Max number of flying requests for each worker.");
DEFINE_int32(timeout_us, 2000, "Timeout in microseconds for a request.");
DEFINE_string(output, "", "Path to save the data.");

class DispatcherRpcBencher {
 public:
  DispatcherRpcBencher(udp::endpoint dispatcher_endpoint,
                       ModelSession model_session, int bench_seconds,
                       int max_flying, uint64_t timeout_us,
                       bool report_progress)
      : dispatcher_endpoint_(dispatcher_endpoint),
        model_session_(model_session),
        bench_seconds_(bench_seconds),
        max_flying_(max_flying),
        timeout_ns_(timeout_us * 1000),
        report_progress_(report_progress),
        tx_socket_(io_context_),
        rx_socket_(io_context_),
        timeout_timer_(io_context_),
        report_timer_(io_context_),
        stop_timer_(io_context_) {}

  void Run() {
    // Start tx/rx socket on the client sdie
    tx_socket_.open(udp::v4());
    rx_socket_.open(udp::v4());
    tx_socket_.bind(udp::endpoint(udp::v4(), 0));
    rx_socket_.bind(udp::endpoint(udp::v4(), 0));
    rx_port_ = rx_socket_.local_endpoint().port();

    // Set up bookkeeping
    requests_per_second_.assign(bench_seconds_ + 2, 0);
    start_time_ = std::chrono::high_resolution_clock::now();
    status_ = Status::Running;

    // Set up async operations
    DoReceive();
    MaintainQuery();
    TimeoutTimer();
    ReportTimer();
    StopTimer();

    // Block until done
    io_context_.run();
    status_ = Status::Done;
  }

  const std::vector<uint32_t>& latencies_ns() const { return latencies_ns_; }

  const std::vector<uint32_t>& requests_per_second() const {
    return requests_per_second_;
  }

  int cnt_sent() const { return cnt_sent_; }

 private:
  void DoReceive() {
    if (status_ == Status::Stopping) {
      return;
    }
    rx_socket_.async_receive_from(
        boost::asio::buffer(rx_buf_, 1400), rx_endpoint_,
        [this](boost::system::error_code ec, std::size_t len) {
          if (ec == boost::asio::error::operation_aborted) {
            return;
          } else if (ec) {
            LOG(ERROR) << "Error when receiving: " << ec;
            DoReceive();
            return;
          }

          DispatchReply reply;
          bool ok = reply.ParseFromString(std::string(rx_buf_, rx_buf_ + len));
          if (!ok) {
            LOG(ERROR)
                << "Bad response. Failed to ParseFromString. Total length = "
                << len;
            DoReceive();
            return;
          }

          auto iter = pending_responses_.find(reply.query_id());
          if (iter == pending_responses_.end()) {
            LOG(ERROR) << "Got unexpected response. query_id: "
                       << reply.query_id();
            DoReceive();
            return;
          }
          auto& pending_response = iter->second;
          const auto now = std::chrono::high_resolution_clock::now();
          uint32_t latency_ns =
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  now - pending_response.start_time)
                  .count();
          latencies_ns_.push_back(latency_ns);
          const size_t bucket =
              std::chrono::duration_cast<std::chrono::seconds>(now -
                                                               start_time_)
                  .count();
          ++requests_per_second_[bucket];
          flying_requests_.erase(reply.query_id());
          pending_responses_.erase(iter);

          DoReceive();
          MaintainQuery();
        });
  }

  void MaintainQuery() {
    if (status_ != Status::Running) {
      return;
    }

    // Remove flying requests that are timed out
    const auto now = std::chrono::high_resolution_clock::now();
    while (!not_yet_timeout_requests_.empty()) {
      auto req_id = not_yet_timeout_requests_.front();
      auto iter = pending_responses_.find(req_id);
      if (iter == pending_responses_.end()) {
        // Already received
        not_yet_timeout_requests_.pop_front();
      } else {
        uint64_t elapsed_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                now - iter->second.start_time)
                .count();
        if (elapsed_ns >= timeout_ns_) {
          // Timed out
          const size_t bucket =
              std::chrono::duration_cast<std::chrono::seconds>(now -
                                                               start_time_)
                  .count();
          ++cnt_timeout_;
          flying_requests_.erase(iter->first);
          not_yet_timeout_requests_.pop_front();
        } else {
          // The earliest request in the queue is not timed out yet.
          // Don't bother check the rest of them.
          break;
        }
      }
    }

    // Send requests
    while (flying_requests_.size() < max_flying_) {
      DispatchRequest request;
      auto req_id = ++cnt_sent_;
      *request.mutable_model_session() = model_session_;
      request.set_udp_rpc_port(rx_port_);
      request.set_query_id(req_id);

      pending_responses_[req_id] = PendingResponse{
          .start_time = now,
      };
      not_yet_timeout_requests_.push_back(req_id);
      flying_requests_.insert(req_id);

      auto msg = request.SerializeAsString();
      auto msg_len = msg.size();
      tx_socket_.async_send_to(
          boost::asio::buffer(std::move(msg)), dispatcher_endpoint_,
          [this, msg_len](boost::system::error_code ec, std::size_t len) {
            if (ec && ec != boost::asio::error::operation_aborted) {
              LOG(ERROR) << "Error when sending: " << ec;
            }
            if (len != msg_len) {
              LOG(ERROR) << "Sent " << len << " bytes, expecting " << msg_len
                         << " bytes";
            }
          });
    }
  }

  void TimeoutTimer() {
    if (status_ == Status::Stopping) {
      return;
    }
    timeout_timer_.expires_after(std::chrono::nanoseconds(timeout_ns_));
    timeout_timer_.async_wait([this](boost::system::error_code) {
      MaintainQuery();
      TimeoutTimer();
    });
  }

  void ReportTimer() {
    if (status_ == Status::Stopping) {
      return;
    }
    report_timer_.expires_after(std::chrono::seconds(1));
    report_timer_.async_wait([this](boost::system::error_code) {
      const auto now = std::chrono::high_resolution_clock::now();
      const size_t elapsed =
          std::chrono::duration_cast<std::chrono::seconds>(now - start_time_)
              .count();

      while (report_progress_ && next_report_second_ < elapsed) {
        LOG(INFO) << "cnt_sent: " << latencies_ns_.size() << ", rps["
                  << next_report_second_
                  << "]: " << requests_per_second_[next_report_second_]
                  << ", cnt_timeout: " << cnt_timeout_
                  << ", cnt_flying: " << flying_requests_.size();
        ++next_report_second_;
      }

      ReportTimer();
    });
  }

  void StopTimer() {
    if (status_ == Status::Running) {
      stop_timer_.expires_at(start_time_ +
                             std::chrono::seconds(bench_seconds_));
      stop_timer_.async_wait([this](boost::system::error_code) {
        status_ = Status::Finishing;
        tx_socket_.cancel();
        StopTimer();
      });
    } else if (status_ == Status::Finishing) {
      stop_timer_.expires_after(std::chrono::seconds(1));
      stop_timer_.async_wait([this](boost::system::error_code) {
        status_ = Status::Stopping;
        rx_socket_.cancel();
      });
    }
  }

  enum class Status { Idle, Running, Finishing, Stopping, Done };

  const udp::endpoint dispatcher_endpoint_;
  const ModelSession model_session_;
  const int bench_seconds_;
  const int max_flying_;
  const uint64_t timeout_ns_;
  const bool report_progress_;

  Status status_ = Status::Idle;
  boost::asio::io_context io_context_;
  udp::socket tx_socket_;
  udp::socket rx_socket_;
  uint32_t rx_port_ = 0;
  udp::endpoint rx_endpoint_;
  uint8_t rx_buf_[1400];
  boost::asio::high_resolution_timer timeout_timer_;
  boost::asio::high_resolution_timer report_timer_;
  boost::asio::high_resolution_timer stop_timer_;

  struct PendingResponse {
    std::chrono::time_point<std::chrono::high_resolution_clock> start_time;
  };
  std::unordered_map<uint32_t, PendingResponse> pending_responses_;
  std::unordered_set<uint32_t> flying_requests_;
  std::deque<uint32_t> not_yet_timeout_requests_;
  std::vector<uint32_t> latencies_ns_;
  uint32_t cnt_sent_ = 0;
  uint32_t cnt_timeout_ = 0;
  std::chrono::time_point<std::chrono::high_resolution_clock> start_time_;
  std::vector<uint32_t> requests_per_second_;
  int next_report_second_ = 0;
};

udp::endpoint Resolve(const std::string& host, int port) {
  boost::asio::io_context io_context;
  udp::resolver resolver(io_context);
  auto resolve_result = resolver.resolve(udp::v4(), host, std::to_string(port));
  CHECK(!resolve_result.empty())
      << "Failed to resolve address " << host << ":" << port;
  return *resolve_result.begin();
}

struct Worker {
  std::unique_ptr<DispatcherRpcBencher> bencher;
  std::thread thread;
};

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  FLAGS_colorlogtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InstallFailureSignalHandler();

  auto dispatcher_endpoint = Resolve(FLAGS_host, FLAGS_port);
  ModelSession model_session;
  nexus::ParseModelSession(FLAGS_model_session_id, &model_session);

  // Open file in advance
  FILE* file = nullptr;
  if (!FLAGS_output.empty()) {
    file = fopen(FLAGS_output.c_str(), "wb");
    CHECK(file != nullptr) << "Failed to open " << FLAGS_output;
  } else {
    LOG(INFO) << "Not saving data to file.";
  }

  // Run bench
  std::vector<Worker> workers;
  for (int i = 0; i < FLAGS_num_workers; ++i) {
    bool report_progress = i == 0;
    auto* bencher = new DispatcherRpcBencher(
        dispatcher_endpoint, model_session, FLAGS_bench_seconds,
        FLAGS_max_flying, FLAGS_timeout_us, report_progress);
    auto worker = Worker{
        .bencher = std::unique_ptr<DispatcherRpcBencher>(bencher),
        .thread = std::thread(&DispatcherRpcBencher::Run, bencher),
    };
    workers.emplace_back(std::move(worker));
  }
  LOG(INFO) << "Benching " << dispatcher_endpoint << " using " << workers.size()
            << " threads. Only the first thread reports.";
  for (auto& worker : workers) {
    worker.thread.join();
  }
  LOG(INFO) << "Stopped";

  // Collect data
  uint32_t cnt_sent = 0;
  std::vector<uint32_t> latencies_us;
  std::vector<uint32_t> requests_per_second(FLAGS_bench_seconds, 0);
  for (const auto& worker : workers) {
    const auto& bencher = *worker.bencher;
    latencies_us.insert(latencies_us.end(), bencher.latencies_ns().begin(),
                        bencher.latencies_ns().end());
    cnt_sent += bencher.cnt_sent();
    for (size_t i = 0; i < FLAGS_bench_seconds; ++i) {
      requests_per_second[i] += bencher.requests_per_second()[i];
    }
  }

  // Report
  LOG(INFO) << "Sent: " << cnt_sent;
  LOG(INFO) << "Recv: " << latencies_us.size();
  int sum_rps = 0, cnt = 0;
  for (int i = 2; i + 2 < FLAGS_bench_seconds; ++i) {
    sum_rps += requests_per_second[i];
    ++cnt;
  }
  LOG(INFO) << "Avg : " << static_cast<double>(sum_rps) / cnt << " rps";

  // Write to file
  if (file) {
    uint32_t cnt_recv = latencies_us.size();
    uint32_t bench_seconds = FLAGS_bench_seconds;
    CHECK_EQ(fwrite(&cnt_sent, sizeof(cnt_sent), 1, file), 1);
    CHECK_EQ(fwrite(&cnt_recv, sizeof(cnt_recv), 1, file), 1);
    CHECK_EQ(fwrite(&bench_seconds, sizeof(bench_seconds), 1, file), 1);
    CHECK_EQ(fwrite(requests_per_second.data(),
                    sizeof(requests_per_second.front()), bench_seconds, file),
             bench_seconds);
    CHECK_EQ(fwrite(latencies_us.data(), sizeof(latencies_us.front()),
                    latencies_us.size(), file),
             latencies_us.size());
    fclose(file);
    LOG(INFO) << "The data is written to " << FLAGS_output;
  }
}
