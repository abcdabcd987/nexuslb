#pragma once
#include <infiniband/verbs.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ario/circular_completion_queue.h"
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

class WorkRequestID {
 public:
  friend class std::hash<WorkRequestID>;
  friend bool operator==(WorkRequestID lhs, WorkRequestID rhs) {
    return lhs.wrid_ == rhs.wrid_;
  }
  friend bool operator!=(WorkRequestID lhs, WorkRequestID rhs) {
    return lhs.wrid_ != rhs.wrid_;
  }
  std::string DebugString() const {
    return "ario::WorkRequestID(" + std::to_string(wrid_) + ")";
  }

 private:
  friend class RdmaManager;
  explicit WorkRequestID(uint64_t wrid) : wrid_(wrid) {}
  uint64_t wrid_;
};

class RdmaQueuePair;

enum class RdmaError {
  kDisconnect,
};

class RdmaEventHandler {
 public:
  virtual void OnConnected(RdmaQueuePair *conn) = 0;
  virtual void OnRemoteMemoryRegionReceived(RdmaQueuePair *conn, uint64_t addr,
                                            size_t size) = 0;
  virtual void OnRdmaReadComplete(RdmaQueuePair *conn, WorkRequestID wrid,
                                  OwnedMemoryBlock buf) = 0;
  virtual void OnRecv(RdmaQueuePair *conn, OwnedMemoryBlock buf) = 0;
  virtual void OnSent(RdmaQueuePair *conn, OwnedMemoryBlock buf) = 0;
  virtual void OnError(RdmaQueuePair *conn, RdmaError error) = 0;
};

class RdmaManager {
 public:
  RdmaManager(std::string dev_name, EpollExecutor *executor,
              RdmaEventHandler *handler, MemoryBlockAllocator *recv_buf);
  ~RdmaManager();
  void Stop();
  void RegisterLocalMemory(MemoryBlockAllocator *buf);
  void ExposeMemory(void *addr, size_t size);
  void ListenTcp(uint16_t port);
  void ConnectTcp(const std::string &addr, uint16_t port);

 private:
  friend class RdmaManagerAccessor;

  class CompletionChannelHandler : public EpollEventHandler {
   public:
    explicit CompletionChannelHandler(RdmaManager *outer);
    void HandleEpollEvent(uint32_t epoll_events) override;

   private:
    RdmaManager &outer_;
  };

  class CompletionQueuePoller : public EventPoller {
   public:
    explicit CompletionQueuePoller(RdmaManager *outer);
    void Poll() override;

   private:
    RdmaManager &outer_;
  };

  void CreateContext();
  void BuildProtectionDomain();
  void BuildCompletionQueue();
  void StartPoller();
  void PollCompletionQueueBlocking();
  void PollCompletionQueueSpinning();
  void PollCompletionQueue();
  void HandleWorkCompletion(ibv_wc *wc);

  void AddConnection(TcpSocket tcp, bool is_initiator);
  void TcpAccept();

  void AsyncSend(RdmaQueuePair &conn, OwnedMemoryBlock buf);
  WorkRequestID AsyncRead(RdmaQueuePair &conn, OwnedMemoryBlock buf,
                          size_t offset, size_t length);
  void PostReceive(RdmaQueuePair &conn);
  void ReserveCQ();

  std::string dev_name_;
  int dev_port_ = 0;
  EpollExecutor &executor_;
  RdmaEventHandler *handler_;
  MemoryBlockAllocator *recv_buf_;

  ibv_context *ctx_ = nullptr;
  ibv_pd *pd_ = nullptr;
  ibv_cq *cq_ = nullptr;
  ibv_comp_channel *comp_channel_ = nullptr;
  std::unique_ptr<CompletionChannelHandler> comp_channel_handler_;
  std::unique_ptr<CompletionQueuePoller> cq_poller_;
  std::atomic<int> cnt_cq_pending_{0};

  std::mutex mr_mutex_;
  std::unordered_map<void *, ibv_mr *> mr_ /* GUARDED_BY(mr_mutex_) */;
  ibv_mr *exposed_mr_ = nullptr;

  std::atomic<bool> stop_{false};

  std::vector<std::unique_ptr<RdmaQueuePair>> connections_;

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
  RdmaEventHandler *handler() const { return m_->handler_; }

  void AsyncSend(RdmaQueuePair &conn, OwnedMemoryBlock buf) {
    m_->AsyncSend(conn, std::move(buf));
  }

  WorkRequestID AsyncRead(RdmaQueuePair &conn, OwnedMemoryBlock buf,
                          size_t offset, size_t length) {
    return m_->AsyncRead(conn, std::move(buf), offset, length);
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
  WorkRequestID AsyncRead(OwnedMemoryBlock buf, size_t offset, size_t length);
  void Shutdown();

  uint64_t tag() const { return tag_; }
  void set_tag(uint64_t tag) { tag_ = tag; }
  const std::string &peer_ip() const;
  uint16_t peer_tcp_port() const;

 private:
  friend class RdmaManager;

  RdmaQueuePair(RdmaManagerAccessor manager, TcpSocket tcp, size_t index,
                bool is_initiator);
  void MarkConnected();
  void SendMemoryRegions();
  void BuildQueuePair();
  void SendConnInfo();
  void RecvConnInfo();
  void SendMemoryRegion();
  void RecvMemoryRegion();
  void ShutdownTcp();
  void TransitQueuePairToInit();
  void TransitQueuePairToRTR(const RdmaManagerMessage::ConnInfo &msg);
  void TransitQueuePairToRTS();

  void PostReceive();

  RdmaManagerAccessor manager_;
  TcpSocket tcp_;
  size_t index_;
  bool is_initiator_;
  bool is_connected_ = false;
  ibv_qp *qp_ = nullptr;
  RemoteMemoryRegion remote_mr_{};
  uint64_t tag_ = 0;
  std::atomic<int> cnt_recv_pending_{0};
  std::atomic<int> cnt_send_pending_{0};

  // For shutdown tcp. Maybe replace by Promise?
  bool recv_memory_region_done_ = false;
  bool send_memory_region_done_ = false;

  std::mutex wr_ctx_mutex_;
  CircularCompletionQueue<OwnedMemoryBlock, 16384>
      recv_wr_ctx_ /* GUARDED_BY(wr_ctx_mutex_) */;
  CircularCompletionQueue<OwnedMemoryBlock, 16384>
      out_wr_ctx_ /* GUARDED_BY(wr_ctx_mutex_) */;
};

}  // namespace ario

namespace std {
template <>
struct hash<ario::WorkRequestID> {
  std::size_t operator()(ario::WorkRequestID w) const noexcept {
    return std::hash<uint64_t>{}(w.wrid_);
  }
};
}  // namespace std
