#pragma once
#include <infiniband/verbs.h>
#include <poll.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ario/epoll.h"
#include "ario/memory.h"
#include "ario/tcp.h"

namespace ario {

#pragma pack(push, 1)

struct RemoteMemoryRegion {
  uint64_t addr;
  uint64_t size;
  uint32_t rkey;
};

struct RdmaConnectorMessage {
  enum class Type : uint8_t {
    kConnInfo,
    kMemoryRegion,
  };

  struct ConnInfo {
    uint16_t lid;
    ibv_gid gid;
    uint32_t qp_num;
  };

  Type type;
  union {
    ConnInfo conn;
    RemoteMemoryRegion mr;
  } payload;
};

#pragma pack(pop)

class Connection;

class EventHandler {
 public:
  virtual void OnConnected(Connection *conn) = 0;
  virtual void OnRdmaReadComplete(OwnedMemoryBlock buf) = 0;
  virtual void OnRecv(OwnedMemoryBlock buf) = 0;
  virtual void OnSent(OwnedMemoryBlock buf) = 0;
};

enum class PollerType {
  kBlocking,
  kSpinning,
};

struct WorkRequestContext {
  OwnedMemoryBlock buf;

  WorkRequestContext() : buf() {}
  explicit WorkRequestContext(OwnedMemoryBlock buf) : buf(std::move(buf)) {}
};

class Connection {
 public:
  ~Connection();
  void AsyncSend(OwnedMemoryBlock buf);
  void AsyncRead(/* OwnedMemoryBlock buf, */ size_t offset, size_t length);
  MemoryBlockAllocator &allocator() { return local_buf_; }

 private:
  friend class RdmaConnector;
  static constexpr size_t kRecvBacklog = 20;

  Connection(std::string dev_name, int dev_port, TcpSocket tcp,
             ibv_context *ctx, std::vector<uint8_t> *memory_region,
             std::shared_ptr<EventHandler> handler);
  void MarkConnected();
  void SendMemoryRegions();
  void BuildProtectionDomain();
  void BuildCompletionQueue();
  void BuildQueuePair();
  void SendConnInfo();
  void RecvConnInfo();
  void SendMemoryRegion();
  void RecvMemoryRegion();
  void TransitQueuePairToInit();
  void TransitQueuePairToRTR(const RdmaConnectorMessage::ConnInfo &msg);
  void TransitQueuePairToRTS();
  void RegisterMemory();

  void PostRead();
  void PostReceive();
  void PollCompletionQueueBlocking();
  void PollCompletionQueueSpinning();
  void HandleWorkCompletion(ibv_wc *wc);

  std::string dev_name_;
  int dev_port_;
  PollerType poller_type_;
  std::shared_ptr<EventHandler> handler_;

  MemoryBlockAllocator local_buf_;
  ibv_mr *local_mr_ = nullptr;

  std::mutex mutex_;
  std::unordered_map<uint64_t, WorkRequestContext>
      wr_ctx_ /* GUARDED_BY(mutex_) */;

  ibv_mr *rdma_remote_mr_ = nullptr;
  RemoteMemoryRegion remote_mr_{};
  std::vector<uint8_t> *memory_region_;

  bool is_connected_ = false;

  std::atomic<uint64_t> next_wr_id_{1};

  TcpSocket tcp_;
  ibv_context *ctx_ = nullptr;
  ibv_qp *qp_ = nullptr;
  ibv_pd *pd_ = nullptr;
  ibv_cq *cq_ = nullptr;
  ibv_comp_channel *comp_channel_ = nullptr;
  pollfd comp_channel_pollfd_;

  std::atomic<bool> poller_stop_{false};
  std::thread cq_poller_thread_;
};

class RdmaConnector {
 public:
  explicit RdmaConnector(std::string dev_name,
                         std::shared_ptr<EventHandler> handler);
  ~RdmaConnector();
  void ListenTcp(uint16_t port, std::vector<uint8_t> &memory_region);
  void ConnectTcp(const std::string &addr, uint16_t port);
  void RunEventLoop();
  void StopEventLoop();
  Connection *GetConnection();

 private:
  void CreateContext();
  void AddConnection(TcpSocket tcp);
  void TcpAccept();

  std::string dev_name_;
  int dev_port_ = 0;
  std::shared_ptr<EventHandler> handler_;
  std::vector<uint8_t> *memory_region_ = nullptr;
  ibv_context *ctx_ = nullptr;
  std::vector<std::unique_ptr<Connection>> connections_;

  EpollExecutor executor_;
  TcpAcceptor tcp_acceptor_;
  TcpSocket tcp_socket_;
};

}  // namespace ario
