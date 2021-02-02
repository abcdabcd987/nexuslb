#include "ario/ario.h"

#include <unistd.h>

#include <condition_variable>
#include <random>

#include "ario/memory.h"
#include "ario/utils.h"

using namespace ario;

constexpr size_t kRdmaBufPoolBits = __builtin_ctzl(128 << 20);
constexpr size_t kRdmaBufBlockBits = __builtin_ctzl(2 << 20);

struct RpcRequest {
  char msg[1000];
};

class TestHandler : public RdmaEventHandler {
 public:
  void OnRemoteMemoryRegionReceived(RdmaQueuePair *conn, uint64_t addr,
                                    size_t size) override {
    fprintf(stderr, "got memory region: addr=0x%016lx, size=%lu\n", addr, size);
  }

  void OnRecv(RdmaQueuePair *conn, OwnedMemoryBlock buf) override {
    auto view = buf.AsMessageView();
    auto *req = reinterpret_cast<RpcRequest *>(view.bytes());
    fprintf(stderr, "Recv message. view.bytes_length()=%u. msg=\"%s\"\n",
            view.bytes_length(), req->msg);
  }

  void OnSent(RdmaQueuePair *conn, OwnedMemoryBlock buf) override {}
};

class TestServerHandler : public TestHandler {
 public:
  void OnConnected(RdmaQueuePair *conn) override {
    fprintf(stderr, "New RDMA connection.\n");
  }

  void OnRdmaReadComplete(RdmaQueuePair *conn, OwnedMemoryBlock buf) override {}
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

  void OnRdmaReadComplete(RdmaQueuePair *conn, OwnedMemoryBlock buf) override {
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

void DieUsage(const char *program) {
  printf("usage:\n");
  printf("  %s tcpserver <listen_port>\n", program);
  printf("  %s tcpclient <server_host> <server_port>\n", program);
  printf("  %s server <dev_name> <listen_port>\n", program);
  printf("  %s client <dev_name> <server_host> <server_port>\n", program);
  printf(
      "  %s benchsend <dev_name> <server_host> <server_port> <num_packets>\n",
      program);
  std::exit(1);
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
  if (argc != 3) DieUsage(argv[0]);
  uint16_t listen_port = std::stoi(argv[2]);

  EpollExecutor executor;
  TcpAcceptor acceptor(executor);
  acceptor.BindAndListen(listen_port);
  fprintf(stderr, "Listening on port %d\n", listen_port);
  DoAccept(acceptor);
  executor.RunEventLoop();
}

void TcpClientMain(int argc, char **argv) {
  if (argc != 4) DieUsage(argv[0]);
  std::string server_host = argv[2];
  uint16_t server_port = std::stoi(argv[3]);

  EpollExecutor executor;
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
  size_t rand_len = 1 << 20;
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
  if (argc != 4) DieUsage(argv[0]);
  std::string dev_name = argv[2];
  uint16_t listen_port = std::stoul(argv[3]);

  std::vector<uint8_t> memory_pool(100 << 20);
  FillMemoryPool(memory_pool);

  auto test = std::make_shared<TestServerHandler>();
  MemoryBlockAllocator buf(kRdmaBufPoolBits, kRdmaBufBlockBits);
  RdmaManager manager(dev_name, test, &buf);
  manager.ExposeMemory(memory_pool.data(), memory_pool.size());
  manager.ListenTcp(listen_port);
  // std::thread event_loop_thread(&RdmaManager::RunEventLoop, &manager);
  manager.RunEventLoop();
  manager.StopEventLoop();
  // event_loop_thread.join();
}

void ClientMain(int argc, char **argv) {
  if (argc != 5) DieUsage(argv[0]);
  std::string dev_name = argv[2];
  std::string server_host = argv[3];
  uint16_t server_port = std::stoi(argv[4]);

  auto test = std::make_shared<TestClientHandler>();
  MemoryBlockAllocator buf(kRdmaBufPoolBits, kRdmaBufBlockBits);
  RdmaManager manager(dev_name, test, &buf);
  MemoryBlockAllocator read_buf(kRdmaBufPoolBits, kRdmaBufBlockBits);
  manager.RegisterLocalMemory(&read_buf);
  manager.ConnectTcp(server_host, server_port);
  std::thread event_loop_thread(&RdmaManager::RunEventLoop, &manager);

  auto *conn = test->WaitConnection();
  fprintf(stderr, "ClientMain: connected.\n");
  test->WaitMemoryRegion();

  conn->AsyncRead(read_buf.Allocate(), 0, 1024);
  auto read_data = test->WaitRead();
  if (read_data.empty()) die("read_data.empty()");
  auto read_view = read_data.AsMessageView();
  auto msg_len = *reinterpret_cast<uint32_t *>(read_view.bytes());
  std::string msg(read_view.bytes() + 4, read_view.bytes() + 4 + msg_len);
  fprintf(stderr,
          "ClientMain: Read(mem[0:1024]). read_view.bytes_length()=%u. "
          "msg_len=%u. msg: %s\n",
          read_view.bytes_length(), msg_len, msg.c_str());

  size_t offset = 42 << 20;
  size_t rand_len = 1 << 20;
  conn->AsyncRead(read_buf.Allocate(), offset, rand_len);
  read_data = test->WaitRead();
  read_view = read_data.AsMessageView();
  uint64_t sum = 0;
  for (size_t i = 0; i < read_view.bytes_length(); ++i) {
    sum += read_view.bytes()[i];
  }
  fprintf(stderr, "ClientMain: mem[%lu:%lu].sum()=%lu\n", offset,
          offset + rand_len, sum);

  auto send_buf = buf.Allocate();
  auto send_view = send_buf.AsMessageView();
  auto *req = reinterpret_cast<RpcRequest *>(send_view.bytes());
  strcpy(req->msg, "THIS IS A MESSAGE FROM THE CLIENT.");
  send_view.set_bytes_length(sizeof(*req));
  conn->AsyncSend(std::move(send_buf));
  fprintf(stderr, "ClientMain: AsyncSend.\n");

  manager.StopEventLoop();
  fprintf(stderr, "ClientMain: Joining event loop.\n");
  event_loop_thread.join();
  fprintf(stderr, "ClientMain: event loop joined.\n");
}

class BenchSendHandler : public TestClientHandler {
 public:
  void SetAllocator(MemoryBlockAllocator &allocator) {
    allocator_ = &allocator;
  }

  void OnSent(RdmaQueuePair *conn, OwnedMemoryBlock buf) override {
    --cnt_flying_;
    ++cnt_sent_;
    if (cnt_sent_ == num_packets_) {
      cv_.notify_all();
    } else {
      SendMore();
    }
  }

  void BenchSend(size_t num_packets, RdmaQueuePair *conn) {
    num_packets_ = num_packets;
    conn_ = conn;
    cnt_sent_ = 0;
    cnt_send_ = 0;
    start_ = std::chrono::high_resolution_clock::now();
    last_report_time_ = start_;

    SendMore();
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return cnt_sent_.load() == num_packets_; });
    ReportProgress(true);
  }

 private:
  void SendMore() {
    constexpr size_t kMaxFlying = 10;

    auto last_send = cnt_send_;
    while (cnt_flying_ < kMaxFlying && cnt_send_ < num_packets_) {
      auto send_buf = allocator_->Allocate();
      auto send_view = send_buf.AsMessageView();
      auto *req = reinterpret_cast<RpcRequest *>(send_view.bytes());
      snprintf(req->msg, sizeof(req->msg), "THIS IS PACKET #%08lu", cnt_send_);
      send_view.set_bytes_length(sizeof(*req));
      conn_->AsyncSend(std::move(send_buf));
      ++cnt_flying_;
      ++cnt_send_;
      // std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
    if (last_send != cnt_send_) {
      ReportProgress(false);
    }
  }

  void ReportProgress(bool force) {
    auto now = std::chrono::high_resolution_clock::now();
    auto last_second = std::chrono::duration_cast<std::chrono::seconds>(
                           last_report_time_ - start_)
                           .count();
    auto now_second =
        std::chrono::duration_cast<std::chrono::seconds>(now - start_).count();
    if (now_second == last_second && !force) {
      return;
    }
    auto nanos =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_)
            .count();
    auto seconds = nanos / 1e9;
    auto cnt_sent = cnt_sent_.load();
    fprintf(stderr, "[%3lu%%] Sent %lu/%lu requests in %.6fs. (avg %.3f rps)\n",
            cnt_sent * 100 / num_packets_, cnt_sent, num_packets_, seconds,
            cnt_sent / seconds);
    last_report_time_ = now;
  }

  MemoryBlockAllocator *allocator_;
  std::mutex mutex_;
  std::condition_variable cv_;
  RdmaQueuePair *conn_ = nullptr;
  size_t num_packets_ = 0;
  size_t cnt_send_;
  std::atomic<size_t> cnt_sent_;
  std::atomic<size_t> cnt_flying_;
  std::chrono::high_resolution_clock::time_point start_;
  std::chrono::high_resolution_clock::time_point last_report_time_;
};

void BenchSendMain(int argc, char **argv) {
  if (argc != 6) DieUsage(argv[0]);
  std::string dev_name = argv[2];
  std::string server_host = argv[3];
  uint16_t server_port = std::stoi(argv[4]);
  size_t num_packets = std::stoul(argv[5]);

  auto handler = std::make_shared<BenchSendHandler>();
  MemoryBlockAllocator buf(kRdmaBufPoolBits, kRdmaBufBlockBits);
  RdmaManager manager(dev_name, handler, &buf);
  handler->SetAllocator(buf);
  manager.ConnectTcp(server_host, server_port);
  std::thread event_loop_thread(&RdmaManager::RunEventLoop, &manager);

  auto *conn = handler->WaitConnection();
  fprintf(stderr, "BenchSendMain: connected.\n");

  fprintf(stderr, "sleep 1 second\n");
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  fprintf(stderr, "start bench\n");
  handler->BenchSend(num_packets, conn);

  manager.StopEventLoop();
  event_loop_thread.join();
}

int main(int argc, char **argv) {
  if (argc < 2) DieUsage(argv[0]);
  if (std::string("server") == argv[1]) {
    ServerMain(argc, argv);
  } else if (std::string("client") == argv[1]) {
    ClientMain(argc, argv);
  } else if (std::string("benchsend") == argv[1]) {
    BenchSendMain(argc, argv);
  } else if (std::string("tcpserver") == argv[1]) {
    TcpServerMain(argc, argv);
  } else if (std::string("tcpclient") == argv[1]) {
    TcpClientMain(argc, argv);
  } else {
    DieUsage(argv[0]);
  }
}
