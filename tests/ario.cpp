#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>

#include "ario/chrono.h"
#include "ario/epoll.h"
#include "ario/error.h"
#include "ario/memory.h"
#include "ario/rdma.h"
#include "ario/timer.h"
#include "ario/utils.h"

using namespace ario;

constexpr size_t kRdmaBufPoolBits = __builtin_ctzl(1 << 30);
constexpr size_t kRdmaBufBlockBits = __builtin_ctzl(128 << 10);

#pragma pack(push, 1)
struct RpcMessage {
  static constexpr size_t kDefaultMsgLen = 1000;
  size_t seqnum;
  size_t read_offset;
  size_t read_length;
  char msg[1];
};
#pragma pack(pop)

class TestHandler : public RdmaEventHandler {
 public:
  void OnRemoteMemoryRegionReceived(RdmaQueuePair *conn, uint64_t addr,
                                    size_t size) override {
    fprintf(stderr, "got memory region: addr=0x%016lx, size=%lu\n", addr, size);
  }

  void OnRecv(RdmaQueuePair *conn, OwnedMemoryBlock buf) override {
    auto view = buf.AsMessageView();
    auto *req = reinterpret_cast<RpcMessage *>(view.bytes());
    if (print_message_) {
      fprintf(stderr,
              "Recv message. view.bytes_length()=%u. seqnum=%lu msg=\"%s\"\n",
              view.bytes_length(), req->seqnum, req->msg);
    }
    if (reply_allocator_) {
      auto reply_buf = reply_allocator_->Allocate();
      auto reply_view = reply_buf.AsMessageView();
      auto *reply = reinterpret_cast<RpcMessage *>(reply_view.bytes());
      reply->seqnum = req->seqnum;
      reply->read_offset = req->read_offset;
      reply->read_length = req->read_length;
      auto maxlen = reply_view.max_bytes_length() - sizeof(RpcMessage);

      if (req->read_length) {
        if (!exposed_memory_addr_) die("exposed_memory_addr_ not set");
        if (req->read_length > maxlen) die("req->reply_msglen > maxlen");
        if (req->read_offset + req->read_length > exposed_memory_size_)
          die("req->read_offset + req->reply_msglen > exposed_memory_size_");
        memcpy(&reply->msg, exposed_memory_addr_ + reply->read_offset,
               req->read_length);
        reply_view.set_bytes_length(sizeof(RpcMessage) - sizeof(req->msg) +
                                    req->read_length);
      } else {
        snprintf(reply->msg, maxlen,
                 "THIS IS A REPLY FROM THE SERVER. SEQNUM=%lu", req->seqnum);
        reply_view.set_bytes_length(sizeof(RpcMessage) -
                                    sizeof(RpcMessage::msg) +
                                    RpcMessage::kDefaultMsgLen);
      }

      conn->AsyncSend(std::move(reply_buf));
    }
  }

  void OnSent(RdmaQueuePair *conn, OwnedMemoryBlock buf) override {}

  void OnError(RdmaQueuePair *conn, RdmaError error) override {
    fprintf(stderr, "TestHandler::OnError. error=%d\n",
            static_cast<int>(error));
  }

  void SetPrintMessage(bool print_message) { print_message_ = print_message; }

  void SetReplyAllocator(MemoryBlockAllocator *reply_allocator) {
    reply_allocator_ = reply_allocator;
  }

  void SetExposedMemory(uint8_t *addr, size_t size) {
    exposed_memory_addr_ = addr;
    exposed_memory_size_ = size;
  }

 private:
  bool print_message_ = true;
  MemoryBlockAllocator *reply_allocator_ = nullptr;
  uint8_t *exposed_memory_addr_ = nullptr;
  size_t exposed_memory_size_ = 0;
};

class TestServerHandler : public TestHandler {
 public:
  void OnConnected(RdmaQueuePair *conn) override {
    fprintf(stderr, "New RDMA connection.\n");
  }

  void OnRdmaReadComplete(RdmaQueuePair *conn, WorkRequestID wrid,
                          OwnedMemoryBlock buf) override {}
};

class TestClientHandler : public TestHandler {
 public:
  void OnConnected(RdmaQueuePair *conn) override {
    if (conn_ != nullptr) die("TestHandler::OnConnected: conn_ != nullptr");
    conn_ = conn;
    cv_.notify_all();
  }

  void OnRemoteMemoryRegionReceived(RdmaQueuePair *conn, uint64_t addr,
                                    size_t size) override {
    TestHandler::OnRemoteMemoryRegionReceived(conn, addr, size);
    if (got_memory_region_) die("Already got memory region");
    got_memory_region_ = true;
    cv_.notify_all();
  }

  void OnRdmaReadComplete(RdmaQueuePair *conn, WorkRequestID wrid,
                          OwnedMemoryBlock buf) override {
    if (data_.has_value())
      die("TestHandler::OnRdmaReadComplete: data_.has_value()");
    data_ = std::move(buf);
    cv_.notify_all();
  }

  RdmaQueuePair *WaitConnection() {
    if (conn_) return conn_;
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return conn_ != nullptr; });
    return conn_;
  }

  void WaitMemoryRegion() {
    if (got_memory_region_) return;
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return got_memory_region_; });
  }

  OwnedMemoryBlock WaitRead() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return data_.has_value(); });
    auto bytes = std::move(*data_);
    data_ = std::nullopt;
    return bytes;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::optional<OwnedMemoryBlock> data_;
  bool got_memory_region_ = false;
  RdmaQueuePair *conn_ = nullptr;
};

[[noreturn]] void DieUsage(const char *program) {
  printf("usage:\n");
  printf("  %s tcpserver block|spin <listen_port>\n", program);
  printf("  %s tcpclient block|spin <server_host> <server_port>\n", program);
  printf(
      "  %s server block|spin <dev_name> <listen_port> "
      "print|noprint reply|noreply\n",
      program);
  printf("  %s client block|spin <dev_name> <server_host> <server_port>\n",
         program);
  printf(
      "  %s benchsend block|spin <dev_name> <server_host> <server_port> "
      "<max_flying> <num_packets> <read_size> <logfilename>\n",
      program);
  printf(
      "  %s benchread block|spin <dev_name> <server_host> <server_port> "
      "<max_flying> <num_packets> <read_size> <logfilename>\n",
      program);
  printf(
      "  %s benchincast block|spin <dev_name> "
      "<read_size> <concurrent_reads> <warmups> <tests> "
      "<ip:port>...\n",
      program);
  printf("  %s benchtimer block|spin <duration> <count>\n", program);
  std::exit(1);
}

EpollExecutor BuildExecutor(int argc, char **argv) {
  if (std::string(argv[2]) == "block") {
    return EpollExecutor(PollerType::kBlocking);
  } else if (std::string(argv[2]) == "spin") {
    return EpollExecutor(PollerType::kSpinning);
  };
  DieUsage(argv[0]);
}

class SimpleTcpConnection {
 public:
  explicit SimpleTcpConnection(TcpSocket &&peer) : peer_(std::move(peer)) {}

  void RecvMessage() {
    MutableBuffer len_buf(&recv_len_, sizeof(recv_len_));
    peer_.AsyncRead(len_buf, [this](int err, size_t) {
      if (err) {
        fprintf(stderr, "AsyncRead header err=%d\n", err);
        delete this;
        return;
      }
      MutableBuffer msg_buf(recv_data_, recv_len_);
      peer_.AsyncRead(msg_buf, [this](int err, size_t len) {
        if (err) {
          fprintf(stderr, "AsyncRead message err=%d\n", err);
          delete this;
          return;
        }
        fprintf(stderr, "got message. len=%lu. msg: %s\n", len, recv_data_);
        RecvMessage();
      });
    });
  }

  void SendMessage(std::vector<uint8_t> &&data,
                   std::function<void(int error)> &&callback) {
    send_data_ = std::move(data);
    send_callback_ = std::move(callback);
    send_len_ = static_cast<uint16_t>(send_data_.size());

    ConstBuffer len_buf(&send_len_, sizeof(send_len_));
    peer_.AsyncWrite(len_buf, [this](int error, size_t) {
      if (error) {
        auto callback = std::move(send_callback_);
        delete this;
        callback(error);
        return;
      }
      ConstBuffer msg_buf(send_data_.data(), send_len_);
      peer_.AsyncWrite(msg_buf, [this](int error, size_t) {
        auto callback = std::move(send_callback_);
        if (error) {
          delete this;
        }
        callback(error);
      });
    });
  }

 private:
  ~SimpleTcpConnection() {
    fprintf(stderr, "SimpleTcpConnection destructor\n");
  }

  TcpSocket peer_;
  uint16_t recv_len_;
  uint8_t recv_data_[1024];
  uint16_t send_len_;
  std::vector<uint8_t> send_data_;
  std::function<void(int error)> send_callback_;
};

void DoAccept(TcpAcceptor &acceptor) {
  acceptor.AsyncAccept([&acceptor](int err, TcpSocket peer) {
    if (err) return;
    auto *conn = new SimpleTcpConnection(std::move(peer));
    conn->RecvMessage();
    DoAccept(acceptor);
  });
}

void TcpServerMain(int argc, char **argv) {
  if (argc != 4) DieUsage(argv[0]);
  uint16_t listen_port = std::stoi(argv[3]);

  auto executor = BuildExecutor(argc, argv);
  TcpAcceptor acceptor(executor);
  acceptor.BindAndListen(listen_port);
  fprintf(stderr, "Listening on port %d\n", listen_port);
  DoAccept(acceptor);
  executor.RunEventLoop();
}

void TcpClientMain(int argc, char **argv) {
  if (argc != 5) DieUsage(argv[0]);
  std::string server_host = argv[3];
  uint16_t server_port = std::stoi(argv[4]);

  auto executor = BuildExecutor(argc, argv);
  TcpSocket socket;
  socket.Connect(executor, server_host, server_port);
  fprintf(stderr, "connected.\n");
  auto conn = new SimpleTcpConnection(std::move(socket));
  std::string msg = "This is a message from the client.";
  std::vector<uint8_t> data(msg.data(), msg.data() + msg.size());
  data.push_back('\0');
  conn->SendMessage(std::move(data), [&executor](int error) {
    if (error) {
      fprintf(stderr, "error=%d\n", error);
    } else {
      fprintf(stderr, "message sent.\n");
    }
    fprintf(stderr, "stopping event loop\n");
    executor.StopEventLoop();
  });
  executor.RunEventLoop();
}

void FillMemoryPool(std::vector<uint8_t> &memory_pool) {
  auto *mem = memory_pool.data();
  auto pid = getpid();
  char timebuf[100];
  std::time_t t = std::time(nullptr);
  std::strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S %Z",
                std::localtime(&t));
  char buf[100];
  int len = snprintf(buf, sizeof(buf), "MESSAGE FROM PID %d. CREATED AT %s.",
                     pid, timebuf);
  memcpy(mem + 4, buf, len);
  *reinterpret_cast<uint32_t *>(mem) = static_cast<uint32_t>(len);
  fprintf(stderr, "FillMemoryPool: mem[0]=%d. mem[4]=\"%s\"\n", len, buf);

  std::mt19937 gen(123);
  std::uniform_int_distribution<> distrib(0, 255);
  size_t offset = 42 << 20;
  size_t rand_len = 64 << 10;
  uint64_t sum = 0;
  for (size_t i = 0; i < rand_len; ++i) {
    auto x = distrib(gen);
    mem[offset + i] = x;
    sum += x;
  }
  fprintf(stderr, "FillMemoryPool: mem[%lu:%lu].sum()=%lu\n", offset,
          offset + rand_len, sum);
}

void ServerMain(int argc, char **argv) {
  if (argc != 7) DieUsage(argv[0]);
  std::string dev_name = argv[3];
  uint16_t listen_port = std::stoul(argv[4]);

  auto test = std::make_unique<TestServerHandler>();
  MemoryBlockAllocator buf(kRdmaBufPoolBits, kRdmaBufBlockBits);
  for (size_t i = 5; i < 7; ++i) {
    std::string option = argv[i];
    if (option == "print") {
      test->SetPrintMessage(true);
    } else if (option == "noprint") {
      test->SetPrintMessage(false);
    } else if (option == "reply") {
      test->SetReplyAllocator(&buf);
    } else if (option == "noreply") {
      test->SetReplyAllocator(nullptr);
    } else {
      fprintf(stderr, "Unknown option: %s\n", argv[i]);
      DieUsage(argv[0]);
    }
  }
  auto executor = BuildExecutor(argc, argv);

  std::vector<uint8_t> memory_pool(100 << 20);
  FillMemoryPool(memory_pool);
  test->SetExposedMemory(memory_pool.data(), memory_pool.size());

  RdmaManager manager(dev_name, &executor, test.get(), &buf);
  manager.ExposeMemory(memory_pool.data(), memory_pool.size());
  manager.ListenTcp(listen_port);
  executor.RunEventLoop();

  // Unreachable
  executor.StopEventLoop();
  manager.Stop();
}

void ClientMain(int argc, char **argv) {
  if (argc != 6) DieUsage(argv[0]);
  std::string dev_name = argv[3];
  std::string server_host = argv[4];
  uint16_t server_port = std::stoi(argv[5]);
  auto executor = BuildExecutor(argc, argv);

  auto test = std::make_unique<TestClientHandler>();
  MemoryBlockAllocator buf(kRdmaBufPoolBits, kRdmaBufBlockBits);
  RdmaManager manager(dev_name, &executor, test.get(), &buf);
  MemoryBlockAllocator read_buf(kRdmaBufPoolBits, kRdmaBufBlockBits);
  manager.RegisterLocalMemory(&read_buf);
  manager.ConnectTcp(server_host, server_port);
  std::thread event_loop_thread(&EpollExecutor::RunEventLoop, &executor);

  auto *conn = test->WaitConnection();
  fprintf(stderr, "ClientMain: connected.\n");
  test->WaitMemoryRegion();

  conn->AsyncRead(read_buf.Allocate(), 0, 1024);
  auto read1_data = test->WaitRead();
  if (read1_data.empty()) die("read_data.empty()");
  auto read1_view = read1_data.AsMessageView();
  auto msg_len = *reinterpret_cast<uint32_t *>(read1_view.bytes());
  std::string msg(read1_view.bytes() + 4, read1_view.bytes() + 4 + msg_len);
  fprintf(stderr,
          "ClientMain: Read(mem[0:1024]). read1_view.bytes_length()=%u. "
          "msg_len=%u. msg: %s\n",
          read1_view.bytes_length(), msg_len, msg.c_str());

  size_t offset = 42 << 20;
  size_t rand_len = 64 << 10;
  conn->AsyncRead(read_buf.Allocate(), offset, rand_len);
  auto read2_data = test->WaitRead();
  auto read2_view = read2_data.AsMessageView();
  uint64_t sum = 0;
  for (size_t i = 0; i < read2_view.bytes_length(); ++i) {
    sum += read2_view.bytes()[i];
  }
  fprintf(stderr, "ClientMain: mem[%lu:%lu].sum()=%lu\n", offset,
          offset + rand_len, sum);

  auto send_buf = buf.Allocate();
  auto send_view = send_buf.AsMessageView();
  auto *req = reinterpret_cast<RpcMessage *>(send_view.bytes());
  req->seqnum = 2333;
  strcpy(req->msg, "THIS IS A MESSAGE FROM THE CLIENT.");
  send_view.set_bytes_length(100);
  conn->AsyncSend(std::move(send_buf));
  fprintf(stderr, "ClientMain: AsyncSend.\n");

  executor.StopEventLoop();
  manager.Stop();
  fprintf(stderr, "ClientMain: Joining event loop.\n");
  event_loop_thread.join();
  fprintf(stderr, "ClientMain: event loop joined.\n");
}

class BenchHandler : public TestClientHandler {
 public:
  explicit BenchHandler(EpollExecutor *executor) : executor_(*executor) {}

  void SetAllocator(MemoryBlockAllocator &allocator) {
    allocator_ = &allocator;
  }

  void OnSent(RdmaQueuePair *conn, OwnedMemoryBlock buf) override {
    sent_bytes_ += buf.AsMessageView().total_length();
  }

  void OnRecv(RdmaQueuePair *conn, OwnedMemoryBlock buf) override {
    auto now = Clock::now();
    auto view = buf.AsMessageView();
    auto *reply = reinterpret_cast<RpcMessage *>(view.bytes());
    rpc_recv_time_[reply->seqnum] = now;

    --cnt_flying_;
    recv_bytes_ += view.total_length();
    if (cnt_sent_ < num_packets_) {
      SendMore();
    }
    ++cnt_recv_;
    if (cnt_recv_ == num_packets_) {
      finish_time_ = now;
      cv_.notify_all();
    }
  }

  void BenchSend(size_t max_flying, size_t num_packets, size_t read_size) {
    mode_ = Mode::kSendRecv;
    max_flying_ = max_flying;
    num_packets_ = num_packets;
    read_size_ = read_size;
    cnt_sent_ = 0;
    cnt_recv_ = 0;
    sent_bytes_ = 0;
    recv_bytes_ = 0;
    start_time_ = Clock::now();
    last_report_time_ = start_time_;
    rpc_send_time_.reset(new TimePoint[num_packets]);
    rpc_recv_time_.reset(new TimePoint[num_packets]);
    distrib_ = std::uniform_int_distribution<size_t>(
        0, remote_memory_size_ - read_size_ - 1);

    executor_.PostOk([this](ario::ErrorCode) { SendMore(); });
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return cnt_recv_ == num_packets_; });
    ReportProgress(true);
  }

  void WaitMemoryRegion() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return remote_memory_size_ != 0; });
  }

  void OnRemoteMemoryRegionReceived(RdmaQueuePair *conn, uint64_t addr,
                                    size_t size) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      remote_memory_size_ = size;
      conn_ = conn;
    }
    cv_.notify_all();
  }

  void OnRdmaReadComplete(RdmaQueuePair *conn, WorkRequestID wrid,
                          OwnedMemoryBlock buf) override {
    auto now = Clock::now();
    auto idx = wrid_to_idx_[wrid];
    rpc_recv_time_[idx] = now;

    ++cnt_recv_;
    recv_bytes_ += buf.AsMessageView().total_length();
    if (cnt_recv_ == num_packets_) {
      finish_time_ = now;
      cv_.notify_all();
    }
    ReadOneMore();
  }

  void BenchRead(size_t max_flying, size_t num_packets, size_t read_size) {
    mode_ = Mode::kRead;
    max_flying_ = max_flying;
    num_packets_ = num_packets;
    read_size_ = read_size;
    cnt_sent_ = 0;
    cnt_recv_ = 0;
    recv_bytes_ = 0;
    start_time_ = Clock::now();
    last_report_time_ = start_time_;
    rpc_send_time_.reset(new TimePoint[num_packets]);
    rpc_recv_time_.reset(new TimePoint[num_packets]);
    wrid_to_idx_.clear();
    wrid_to_idx_.reserve(num_packets);
    distrib_ = std::uniform_int_distribution<size_t>(
        0, remote_memory_size_ - read_size_ - 1);

    executor_.PostOk([this](ario::ErrorCode) {
      for (size_t i = 0; i < max_flying_; ++i) {
        ReadOneMore();
      }
    });
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return cnt_recv_ == num_packets_; });
    ReportProgress(true);
  }

  void SaveAnalysis(const char *filename) {
    FILE *f = nullptr;
    if (filename) {
      f = fopen(filename, "w");
      if (!f) {
        die("Cannot open file to write: " + std::string(filename));
      }
    }

    std::vector<int64_t> rtt;
    rtt.reserve(num_packets_);
    for (size_t i = 0; i < num_packets_; ++i) {
      auto send_time_ns = rpc_recv_time_[i].time_since_epoch().count();
      auto rtt_ns = (rpc_recv_time_[i] - rpc_send_time_[i]).count();
      rtt.push_back(rtt_ns);
      if (f) {
        fprintf(f, "%ld %ld\n", send_time_ns, rtt_ns);
      }
    }

    double elapse_s = (finish_time_ - start_time_).count() / 1e9;
    double recv_packet_size = recv_bytes_ * 1.0 / num_packets_;
    double recv_bandwidth_gbps = recv_bytes_ * 8 / 1e9 / elapse_s;
    printf("max_flying: %lu\n", max_flying_);
    printf("num_packets: %lu\n", num_packets_);
    printf("read_size: %lu\n", read_size_);
    printf("remote_memory_size: %lu\n", remote_memory_size_);
    if (mode_ == Mode::kRead) {
      printf("mode: READ\n");
      printf("avg READ size: %.0f\n", recv_packet_size);
      printf("avg READ bandwidth: %.3f Gbps\n", recv_bandwidth_gbps);
    } else {
      double send_packet_size = sent_bytes_ * 1.0 / num_packets_;
      double send_bandwidth_gbps = sent_bytes_ * 8 / 1e9 / elapse_s;
      printf("mode: SEND/RECV\n");
      printf("avg SEND size: %.0f\n", send_packet_size);
      printf("avg SEND bandwidth: %.3f Gbps\n", send_bandwidth_gbps);
      printf("avg RECV size: %.0f\n", recv_packet_size);
      printf("avg RECV bandwidth: %.3f Gbps\n", recv_bandwidth_gbps);
    }

    double pps = num_packets_ / elapse_s;
    printf("avg rate: %.3f kpps\n", pps / 1e3);

    std::sort(rtt.begin(), rtt.end());
    auto pp = [&rtt, n = num_packets_](double p) {
      auto idx = static_cast<size_t>(std::floor(n * p / 100.));
      printf("p%-5.2f: %-4.0f us\n", p, rtt[idx] / 1e3);
    };
    pp(50);
    pp(75);
    pp(90);
    pp(95);
    pp(99);
    pp(99.5);
    pp(99.9);
    pp(99.95);
    pp(99.99);
  }

 private:
  void SendMore() {
    auto last_send = cnt_sent_;
    while (cnt_flying_ < max_flying_ && cnt_sent_ < num_packets_) {
      auto send_buf = allocator_->Allocate();
      auto send_view = send_buf.AsMessageView();
      auto *req = reinterpret_cast<RpcMessage *>(send_view.bytes());
      req->seqnum = cnt_sent_;
      if (read_size_) {
        req->read_offset = distrib_(gen_);
        req->read_length = read_size_;
        send_view.set_bytes_length(sizeof(*req));
      } else {
        req->read_offset = 0;
        req->read_length = 0;
        auto maxlen = send_view.max_bytes_length() - sizeof(RpcMessage);
        snprintf(req->msg, maxlen, "THIS IS REQUEST SEQNUM=%lu", req->seqnum);
        send_view.set_bytes_length(sizeof(*req) - sizeof(req->msg) +
                                   RpcMessage::kDefaultMsgLen);
      }
      auto now = Clock::now();
      conn_->AsyncSend(std::move(send_buf));
      rpc_send_time_[req->seqnum] = now;
      ++cnt_flying_;
      ++cnt_sent_;
    }
    if (last_send != cnt_sent_) {
      ReportProgress(false);
    }
  }

  void ReadOneMore() {
    if (cnt_sent_ == num_packets_) {
      return;
    }
    size_t idx = cnt_sent_;
    auto offset = distrib_(gen_);
    auto wrid = conn_->AsyncRead(allocator_->Allocate(), offset, read_size_);
    auto now = Clock::now();
    wrid_to_idx_[wrid] = idx;
    rpc_send_time_[idx] = now;
    ++cnt_sent_;
    ReportProgress(false);
  }

  void ReportProgress(bool force) {
    auto now = Clock::now();
    auto last_second = std::chrono::duration_cast<std::chrono::seconds>(
                           last_report_time_ - start_time_)
                           .count();
    auto now_second =
        std::chrono::duration_cast<std::chrono::seconds>(now - start_time_)
            .count();
    if (now_second == last_second && !force) {
      return;
    }
    auto nanos = (now - start_time_).count();
    auto seconds = nanos / 1e9;
    auto cnt_sent = cnt_sent_;
    fprintf(stderr, "[%3lu%%] Sent %lu/%lu requests in %.6fs. (avg %.3f rps)\n",
            cnt_sent * 100 / num_packets_, cnt_sent, num_packets_, seconds,
            cnt_sent / seconds);
    last_report_time_ = now;
  }

  enum class Mode {
    kSendRecv,
    kRead,
  };

  using Clock = std::chrono::high_resolution_clock;
  using TimePoint = std::chrono::time_point<Clock, std::chrono::nanoseconds>;

  EpollExecutor &executor_;
  MemoryBlockAllocator *allocator_;
  std::mutex mutex_;
  std::condition_variable cv_;
  RdmaQueuePair *conn_ = nullptr;
  Mode mode_;
  size_t max_flying_ = 0;
  size_t num_packets_ = 0;
  size_t remote_memory_size_ = 0;
  size_t read_size_ = 0;
  size_t cnt_flying_ = 0;
  size_t cnt_sent_ = 0;
  size_t cnt_recv_ = 0;
  size_t sent_bytes_ = 0;
  size_t recv_bytes_ = 0;
  TimePoint start_time_;
  TimePoint finish_time_;
  TimePoint last_report_time_;
  std::unique_ptr<TimePoint[]> rpc_send_time_;
  std::unique_ptr<TimePoint[]> rpc_recv_time_;
  std::unordered_map<WorkRequestID, size_t> wrid_to_idx_;
  std::mt19937 gen_{0xabcdabcd987LL};
  std::uniform_int_distribution<size_t> distrib_;
};

void BenchSendMain(int argc, char **argv) {
  if (argc != 10) DieUsage(argv[0]);
  std::string dev_name = argv[3];
  std::string server_host = argv[4];
  uint16_t server_port = std::stoi(argv[5]);
  size_t max_flying = std::stoul(argv[6]);
  size_t num_packets = std::stoul(argv[7]);
  size_t read_size = std::stoul(argv[8]);
  std::string logfilename = argv[9];
  auto executor = BuildExecutor(argc, argv);

  auto handler = std::make_unique<BenchHandler>(&executor);
  MemoryBlockAllocator buf(kRdmaBufPoolBits, kRdmaBufBlockBits);
  RdmaManager manager(dev_name, &executor, handler.get(), &buf);
  handler->SetAllocator(buf);
  manager.ConnectTcp(server_host, server_port);
  std::thread event_loop_thread(&EpollExecutor::RunEventLoop, &executor);

  handler->WaitMemoryRegion();
  fprintf(stderr, "BenchSendMain: got memory region.\n");

  fprintf(stderr, "sleep 1 second\n");
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  fprintf(stderr, "start bench\n");
  handler->BenchSend(max_flying, num_packets, read_size);
  handler->SaveAnalysis(logfilename.c_str());

  executor.StopEventLoop();
  manager.Stop();
  event_loop_thread.join();
}

void BenchReadMain(int argc, char **argv) {
  if (argc != 10) DieUsage(argv[0]);
  std::string dev_name = argv[3];
  std::string server_host = argv[4];
  uint16_t server_port = std::stoi(argv[5]);
  size_t max_flying = std::stoul(argv[6]);
  size_t num_packets = std::stoul(argv[7]);
  size_t read_size = std::stoul(argv[8]);
  std::string logfilename = argv[9];
  auto executor = BuildExecutor(argc, argv);

  auto handler = std::make_unique<BenchHandler>(&executor);
  MemoryBlockAllocator buf(kRdmaBufPoolBits, kRdmaBufBlockBits);
  RdmaManager manager(dev_name, &executor, handler.get(), &buf);
  handler->SetAllocator(buf);
  manager.ConnectTcp(server_host, server_port);
  std::thread event_loop_thread(&EpollExecutor::RunEventLoop, &executor);

  handler->WaitMemoryRegion();
  fprintf(stderr, "BenchReadMain: got memory region.\n");

  fprintf(stderr, "sleep 1 second\n");
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  fprintf(stderr, "start bench\n");
  handler->BenchRead(max_flying, num_packets, read_size);
  handler->SaveAnalysis(logfilename.c_str());

  executor.StopEventLoop();
  manager.Stop();
  event_loop_thread.join();
}

class IncastHandler : public ario::RdmaEventHandler {
 public:
  struct Options {
    size_t read_size;
    size_t concurrent_reads;
    size_t warmups;
    size_t tests;
  };

  IncastHandler(Options options, ario::EpollExecutor &executor,
                ario::MemoryBlockAllocator &allocator, size_t num_servers)
      : options_(std::move(options)),
        executor_(executor),
        allocator_(allocator),
        num_servers_(num_servers),
        gen_(0xabcdabcd987LL),
        test_elapse_ns_(options_.warmups + options_.tests) {}

  void OnConnected(RdmaQueuePair *conn) override {}
  void OnRecv(RdmaQueuePair *conn, OwnedMemoryBlock buf) override {}
  void OnSent(RdmaQueuePair *conn, OwnedMemoryBlock buf) override {}
  void OnError(RdmaQueuePair *conn, RdmaError error) override {
    fprintf(stderr, "IncastHandler::OnError. error=%d\n",
            static_cast<int>(error));
  }

  void OnRemoteMemoryRegionReceived(RdmaQueuePair *conn, uint64_t addr,
                                    size_t size) override {
    fprintf(stderr, "Got MemoryRegion from %s:%d size %zu\n",
            conn->peer_ip().c_str(), conn->peer_tcp_port(), size);
    if (servers_.size() == num_servers_) die("Unexpected connection.");
    if (size < options_.read_size)
      die("MemoryRegion too small (" + std::to_string(size) +
          "), expecting at least" + std::to_string(options_.read_size));
    size_t rand_upper = size - options_.read_size + 1;
    servers_.push_back({conn, size, rand_upper});
    if (servers_.size() == num_servers_) {
      executor_.PostOk([this](ario::ErrorCode) { StartBench(); });
    }
  }

  void OnRdmaReadComplete(RdmaQueuePair *conn, WorkRequestID wrid,
                          OwnedMemoryBlock buf) override {
    ++cnt_complete_;
    if (cnt_complete_ == target_complete_) {
      executor_.PostOk(
          [this, now = Clock::now()](ario::ErrorCode) { TestDone(now); });
    }
  }

 private:
  struct ServerInfo {
    RdmaQueuePair *conn;
    size_t remote_memory_size;
    size_t rand_upper;
  };

  void StartBench() {
    start_time_ = Clock::now();
    cnt_done_test_ = 0;
    target_complete_ = options_.concurrent_reads * num_servers_;
    BenchMore();
  }

  void BenchMore() {
    if (cnt_done_test_ == options_.warmups + options_.tests) {
      fprintf(stderr, "Benchmark done.\n");
      BenchmarkDone();
      return;
    } else if (cnt_done_test_ == options_.warmups) {
      fprintf(stderr, "Start to benchmark.\n");
    } else if (cnt_done_test_ == 0) {
      fprintf(stderr, "Start to warmup.\n");
    }

    cnt_complete_ = 0;
    test_start_time_ = Clock::now();
    for (const auto &server : servers_) {
      for (size_t i = 0; i < options_.concurrent_reads; ++i) {
        size_t offset = gen_() % server.rand_upper;
        server.conn->AsyncRead(allocator_.Allocate(), offset,
                               options_.read_size);
      }
    }
  }

  void TestDone(TimePoint test_done_time) {
    auto elapse_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         test_done_time - test_start_time_)
                         .count();
    test_elapse_ns_[cnt_done_test_] = elapse_ns;
    ++cnt_done_test_;
    ReportProgress(false);
    BenchMore();
  }

  void BenchmarkDone() {
    executor_.StopEventLoop();
    ReportProgress(true);

    std::sort(test_elapse_ns_.begin(), test_elapse_ns_.end());
    auto pp = [this](double p) {
      size_t idx = std::floor(test_elapse_ns_.size() * p / 100.);
      double latency_us = test_elapse_ns_[idx] / 1e3;
      double throughput_gbps = target_complete_ * options_.read_size /
                               (test_elapse_ns_[idx] / 1e9) / 1e9;
      printf("p%-5.2f: %4.0f us (%6.3f Gbps)\n", p, latency_us,
             throughput_gbps);
    };
    pp(50);
    pp(75);
    pp(90);
    pp(95);
    pp(99);
    pp(99.5);
    pp(99.9);
    pp(99.95);
    pp(99.99);
  }

  void ReportProgress(bool force) {
    auto now = Clock::now();
    auto last_second = std::chrono::duration_cast<std::chrono::seconds>(
                           last_report_time_ - start_time_)
                           .count();
    auto now_second =
        std::chrono::duration_cast<std::chrono::seconds>(now - start_time_)
            .count();
    if (now_second == last_second && !force) {
      return;
    }
    auto nanos = (now - start_time_).count();
    auto seconds = nanos / 1e9;
    auto total = options_.warmups + options_.tests;
    auto pct = cnt_done_test_ * 100 / total;
    auto rps = cnt_done_test_ * target_complete_ / seconds;
    fprintf(stderr, "[%3lu%%] Sent %lu/%lu requests in %.6fs. (avg %.3f rps)\n",
            pct, cnt_done_test_, total, seconds, rps);
    last_report_time_ = now;
  }

  Options options_;
  ario::EpollExecutor &executor_;
  ario::MemoryBlockAllocator &allocator_;
  size_t num_servers_;
  std::vector<ServerInfo> servers_;
  std::mt19937 gen_;
  std::vector<int64_t> test_elapse_ns_;
  TimePoint start_time_;
  TimePoint last_report_time_;

  size_t cnt_done_test_;
  size_t target_complete_;
  size_t cnt_complete_;
  TimePoint test_start_time_;
};

void BenchIncastMain(int argc, char **argv) {
  if (argc < 9) DieUsage(argv[0]);
  std::string dev_name = argv[3];
  IncastHandler::Options options;
  options.read_size = std::stoul(argv[4]);
  options.concurrent_reads = std::stoul(argv[5]);
  options.warmups = std::stoul(argv[6]);
  options.tests = std::stoul(argv[7]);
  std::vector<std::pair<std::string, uint16_t>> servers;
  for (int i = 8; i < argc; ++i) {
    std::string s(argv[i]);
    auto pos = s.find(':');
    if (pos == std::string::npos) die("Failed to parse server address: " + s);
    auto ip = s.substr(0, pos);
    uint16_t port = std::stoul(s.substr(pos + 1));
    servers.push_back({ip, port});
  }
  auto executor = BuildExecutor(argc, argv);

  MemoryBlockAllocator buf(kRdmaBufPoolBits, kRdmaBufBlockBits);
  IncastHandler handler(options, executor, buf, servers.size());
  RdmaManager manager(dev_name, &executor, &handler, &buf);
  for (const auto &p : servers) {
    fprintf(stderr, "Connecting to %s:%u\n", p.first.c_str(), p.second);
    manager.ConnectTcp(p.first, p.second);
  }
  fprintf(stderr, "Waiting for memory regions\n");
  executor.RunEventLoop();
}

class TimerBencher {
 public:
  TimerBencher(EpollExecutor *executor, int duration_sec, int count)
      : executor_(*executor), duration_sec_(duration_sec), count_(count) {}

  void Bench() {
    executor_thread_ = std::thread(&EpollExecutor::RunEventLoop, &executor_);

    fprintf(stderr, "BenchTimerMain: warming up...\n");
    TimePoint now = Clock::now();
    constexpr int kWarmupCount = 1000;
    std::vector<Timer> warmup_timers;
    for (int i = 0; i < kWarmupCount; ++i) {
      auto timeout = now + std::chrono::milliseconds(i);
      warmup_timers.emplace_back(executor_, timeout,
                                 [this](ErrorCode) { ++cnt_warmup_; });
    }

    std::vector<Timer> bench_timers;
    bench_timers.reserve(count_);
    timers_.reserve(count_);
    offsets_.reserve(count_);
    auto begin = now + std::chrono::seconds(3);
    auto timeout_interval_ns = static_cast<int>(duration_sec_ * 1e9 / count_);
    for (int i = 0; i < count_; ++i) {
      auto timeout =
          begin + std::chrono::nanoseconds((i + 1) * timeout_interval_ns);
      timers_.push_back(timeout);
      bench_timers.emplace_back(executor_, timeout, [this, i](ErrorCode) {
        ++cnt_bench_;
        TimePoint now = Clock::now();
        auto offset_ns = (now - timers_[i]).count();
        offsets_.push_back(offset_ns);
      });
    }
    while (cnt_warmup_ != kWarmupCount)
      ;
    now = Clock::now();
    auto wait_ms = (begin - now).count() / 1e6;
    if (wait_ms < 0) die("Too late");
    fprintf(stderr, "BenchTimerMain: wait for %.0f ms before benching...\n",
            wait_ms);

    Timer wait_timer(executor_, begin);
    wait_timer.AsyncWaitBigCallback([this, timeout_interval_ns](ErrorCode) {
      fprintf(stderr, "BenchTimerMain: benching... timeout_interval: %f us\n",
              timeout_interval_ns / 1e3);
    });
    while (cnt_bench_ != count_)
      ;

    fprintf(stderr, "Stats:\n");
    std::sort(offsets_.begin(), offsets_.end());
    auto pp = [this](double p) {
      auto idx = static_cast<size_t>(std::floor(count_ * p / 100.));
      printf("p%-5.2f: %-6.0f us\n", p, offsets_[idx] / 1e3);
    };
    pp(0);
    pp(10);
    pp(25);
    pp(50);
    pp(90);
    pp(95);
    pp(99);
    pp(99.9);
    pp(99.99);

    executor_.StopEventLoop();
    executor_thread_.join();
  }

 private:
  int duration_sec_;
  int count_;

  EpollExecutor &executor_;
  std::thread executor_thread_;

  std::atomic<int> cnt_warmup_ = 0;
  std::atomic<int> cnt_bench_ = 0;
  std::vector<TimePoint> timers_;
  std::vector<long> offsets_;
};

void BenchTimerMain(int argc, char **argv) {
  if (argc != 5) DieUsage(argv[0]);
  int duration_sec = std::stoi(argv[3]);
  int count = std::stoi(argv[4]);
  auto executor = BuildExecutor(argc, argv);

  TimerBencher bencher(&executor, duration_sec, count);
  bencher.Bench();
}

int main(int argc, char **argv) {
  if (argc < 2) DieUsage(argv[0]);
  if (std::string("server") == argv[1]) {
    ServerMain(argc, argv);
  } else if (std::string("client") == argv[1]) {
    ClientMain(argc, argv);
  } else if (std::string("benchsend") == argv[1]) {
    BenchSendMain(argc, argv);
  } else if (std::string("benchread") == argv[1]) {
    BenchReadMain(argc, argv);
  } else if (std::string("benchincast") == argv[1]) {
    BenchIncastMain(argc, argv);
  } else if (std::string("benchtimer") == argv[1]) {
    BenchTimerMain(argc, argv);
  } else if (std::string("tcpserver") == argv[1]) {
    TcpServerMain(argc, argv);
  } else if (std::string("tcpclient") == argv[1]) {
    TcpClientMain(argc, argv);
  } else {
    DieUsage(argv[0]);
  }
}
