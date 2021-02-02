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
#include <unordered_map>
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

struct RdmaManagerMessage {
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

class RdmaQueuePair;

class EventHandler {
 public:
  virtual void OnConnected(RdmaQueuePair *conn) = 0;
  virtual void OnRemoteMemoryRegionReceived(RdmaQueuePair *conn, uint64_t addr,
                                            size_t size) = 0;
  virtual void OnRdmaReadComplete(OwnedMemoryBlock buf) = 0;
  virtual void OnRecv(OwnedMemoryBlock buf) = 0;
  virtual void OnSent(OwnedMemoryBlock buf) = 0;
};

enum class PollerType {
  kBlocking,
  kSpinning,
};

struct WorkRequestContext {
  RdmaQueuePair &conn;
  OwnedMemoryBlock buf;

  WorkRequestContext(RdmaQueuePair &conn, OwnedMemoryBlock buf)
      : conn(conn), buf(std::move(buf)) {}
};

class RdmaManager {
 public:
  RdmaManager(std::string dev_name, std::shared_ptr<EventHandler> handler,
              MemoryBlockAllocator *recv_buf);
  ~RdmaManager();
  void RegisterLocalMemory(MemoryBlockAllocator *buf);
  void ListenTcp(uint16_t port, std::vector<uint8_t> &memory_region);
  void ConnectTcp(const std::string &addr, uint16_t port);
  void RunEventLoop();
  void StopEventLoop();

 private:
  friend class RdmaManagerAccessor;
  void CreateContext();
  void BuildProtectionDomain();
  void BuildCompletionQueue();
  void ExposeMemory(void *addr, size_t size);
  void StartPoller();
  void PollCompletionQueueBlocking();
  void PollCompletionQueueSpinning();
  void HandleWorkCompletion(ibv_wc *wc);

  void AddConnection(TcpSocket tcp);
  void TcpAccept();

  void AsyncSend(RdmaQueuePair &conn, OwnedMemoryBlock buf);
  void AsyncRead(RdmaQueuePair &conn, OwnedMemoryBlock buf, size_t offset,
                 size_t length);
  void PostReceive(RdmaQueuePair &conn);

  std::string dev_name_;
  int dev_port_ = 0;
  std::shared_ptr<EventHandler> handler_;

  MemoryBlockAllocator *recv_buf_;

  ibv_context *ctx_ = nullptr;
  ibv_pd *pd_ = nullptr;
  ibv_cq *cq_ = nullptr;
  ibv_comp_channel *comp_channel_ = nullptr;
  pollfd comp_channel_pollfd_;

  std::mutex mr_mutex_;
  std::unordered_map<void *, ibv_mr *> mr_ /* GUARDED_BY(mr_mutex_) */;
  ibv_mr *exposed_mr_;

  PollerType poller_type_;
  std::atomic<bool> poller_stop_{false};
  std::thread cq_poller_thread_;

  std::atomic<uint64_t> next_wr_id_{1};
  std::mutex wr_ctx_mutex_;
  std::unordered_map<uint64_t, std::unique_ptr<WorkRequestContext>>
      wr_ctx_ /* GUARDED_BY(wr_ctx_mutex_) */;

  std::vector<std::unique_ptr<RdmaQueuePair>> connections_;

  EpollExecutor executor_;
  TcpAcceptor tcp_acceptor_;
  TcpSocket tcp_socket_;
};

class RdmaManagerAccessor {
 public:
  RdmaManagerAccessor() : m_(nullptr) {}

  const std::string &dev_name() const { return m_->dev_name_; }
  int dev_port() const { return m_->dev_port_; }
  ibv_context *ctx() const { return m_->ctx_; }
  ibv_pd *pd() const { return m_->pd_; }
  ibv_cq *cq() const { return m_->cq_; }
  ibv_mr *explosed_mr() const { return m_->exposed_mr_; }
  EventHandler *handler() const { return m_->handler_.get(); }

  void AsyncSend(RdmaQueuePair &conn, OwnedMemoryBlock buf) {
    m_->AsyncSend(conn, std::move(buf));
  }

  void AsyncRead(RdmaQueuePair &conn, OwnedMemoryBlock buf, size_t offset,
                 size_t length) {
    m_->AsyncRead(conn, std::move(buf), offset, length);
  }

  void PostReceive(RdmaQueuePair &conn) { m_->PostReceive(conn); }

 private:
  friend class RdmaManager;
  explicit RdmaManagerAccessor(RdmaManager &manager) : m_(&manager) {}
  RdmaManager *m_;
};

class RdmaQueuePair {
 public:
  ~RdmaQueuePair();
  void AsyncSend(OwnedMemoryBlock buf);
  void AsyncRead(OwnedMemoryBlock buf, size_t offset, size_t length);

 private:
  friend class RdmaManager;

  RdmaQueuePair(RdmaManagerAccessor manager, TcpSocket tcp);
  void MarkConnected();
  void SendMemoryRegions();
  void BuildQueuePair();
  void SendConnInfo();
  void RecvConnInfo();
  void SendMemoryRegion();
  void RecvMemoryRegion();
  void TransitQueuePairToInit();
  void TransitQueuePairToRTR(const RdmaManagerMessage::ConnInfo &msg);
  void TransitQueuePairToRTS();

  void PostReceive();

  RdmaManagerAccessor manager_;
  TcpSocket tcp_;
  bool is_connected_ = false;

  // Owned by this RdmaQueuePair
  ibv_qp *qp_ = nullptr;

  RemoteMemoryRegion remote_mr_{};
};

}  // namespace ario
