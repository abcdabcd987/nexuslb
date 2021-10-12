#include "ario/rdma.h"

#include <cstdio>
#include <cstring>
#include <memory>

#include "ario/utils.h"

namespace ario {

namespace {

constexpr int kQueuePairRecvBacklog = 1000;
constexpr int kQueuePairSendSize = 1000;
constexpr int kQueuePairRecvSize = kQueuePairRecvBacklog;
constexpr int kCompletionQueueEntries =
    (kQueuePairRecvSize + kQueuePairSendSize) * 100;

struct InternalWrid {
  uint64_t seqnum;
  size_t qp_idx;
  bool is_out;

  static InternalWrid Decode(uint64_t wrid) {
    InternalWrid e;
    e.is_out = wrid & 1;
    wrid >>= 1;
    e.qp_idx = wrid & ((1ULL << 20) - 1);
    wrid >>= 20;
    e.seqnum = wrid;
    return e;
  }

  uint64_t Encode() const { return (seqnum << 21) | (qp_idx << 1) | is_out; }
};

}  // namespace

RdmaManager::RdmaManager(std::string dev_name, EpollExecutor *executor,
                         RdmaEventHandler *handler,
                         MemoryBlockAllocator *recv_buf)
    : dev_name_(std::move(dev_name)),
      executor_(*executor),
      handler_(handler),
      recv_buf_(recv_buf),
      tcp_acceptor_(executor_) {
  // TODO: RAII
  int ret;
  CreateContext();
  BuildProtectionDomain();
  BuildCompletionQueue();
  RegisterLocalMemory(recv_buf_);
  StartPoller();
}

void RdmaManager::StartPoller() {
  switch (executor_.poller_type()) {
    case PollerType::kBlocking:
      comp_channel_handler_ = std::make_unique<CompletionChannelHandler>(this);
      executor_.WatchFD(comp_channel_->fd, *comp_channel_handler_);
      break;
    case PollerType::kSpinning:
      cq_poller_ = std::make_unique<CompletionQueuePoller>(this);
      executor_.AddPoller(*cq_poller_);
      break;
  }
}

void RdmaManager::Stop() {
  // TODO: unregister from the executor
  if (stop_) {
    return;
  }
  stop_ = true;
  connections_.clear();
  if (ctx_) ibv_close_device(ctx_);
}

RdmaManager::~RdmaManager() { Stop(); }

void RdmaManager::CreateContext() {
  int cnt, ret;
  ibv_context *ctx = nullptr;
  ibv_device **devs = ibv_get_device_list(&cnt);
  for (int i = 0; i < cnt; i++) {
    const char *name = ibv_get_device_name(devs[i]);
    ctx = ibv_open_device(devs[i]);
    ibv_device_attr device_attr;
    ret = ibv_query_device(ctx, &device_attr);
    if (ret) die("ibv_query_device");
    unsigned long long uuid = ibv_get_device_guid(devs[i]);
    fprintf(stderr,
            "Found ibv device: name=%s, guid=0x%016llx. Active ports:", name,
            uuid);
    int dev_port = 0;
    for (int p = 1; p <= device_attr.phys_port_cnt; ++p) {
      ibv_port_attr port_attr;
      ret = ibv_query_port(ctx, p, &port_attr);
      if (ret) die("ibv_query_port");
      if (port_attr.state != IBV_PORT_ACTIVE) {
        continue;
      }
      fprintf(stderr, " %d", p);
      if (!dev_port) {
        dev_port = p;
      }
    }
    fprintf(stderr, "\n");

    if (dev_name_ == name) {
      if (!dev_port) die("Could not find active port at device " + dev_name_);
      ctx_ = ctx;
      dev_port_ = dev_port;
    } else {
      ibv_close_device(ctx);
    }
  }
  ibv_free_device_list(devs);
  if (!ctx_) die("Could not open device: " + dev_name_);
  fprintf(stderr, "Opened ibv device %s at port %d\n", dev_name_.c_str(),
          dev_port_);
}

void RdmaManager::ListenTcp(uint16_t port) {
  tcp_acceptor_.BindAndListen(port);
  fprintf(stderr, "TCP server listening on port %d\n", port);
  TcpAccept();
}

void RdmaManager::TcpAccept() {
  tcp_acceptor_.AsyncAccept([this](int error, TcpSocket peer) {
    if (error) {
      fprintf(stderr, "TcpAccept error=%d\n", error);
      die("TcpAccept AsyncAccept");
    }

    AddConnection(std::move(peer), false);
    TcpAccept();
  });
}

void RdmaManager::ConnectTcp(const std::string &host, uint16_t port) {
  fprintf(stderr, "Connecting TCP to host %s port %u\n", host.c_str(), port);
  tcp_socket_.Connect(executor_, host, port);
  fprintf(stderr, "TCP socket connected to %s:%d\n",
          tcp_socket_.peer_ip().c_str(), tcp_socket_.peer_port());
  AddConnection(std::move(tcp_socket_), true);
}

void RdmaManager::AddConnection(TcpSocket tcp, bool is_initiator) {
  auto conn = new RdmaQueuePair(RdmaManagerAccessor(*this), std::move(tcp),
                                connections_.size(), is_initiator);
  connections_.emplace_back(conn);
}

RdmaQueuePair::RdmaQueuePair(RdmaManagerAccessor manager, TcpSocket tcp,
                             size_t index, bool is_initiator)
    : manager_(manager),
      tcp_(std::move(tcp)),
      index_(index),
      is_initiator_(is_initiator) {
  int ret;
  is_connected_ = false;

  BuildQueuePair();
  TransitQueuePairToInit();
  SendConnInfo();
}

RdmaQueuePair::~RdmaQueuePair() {
  if (qp_) ibv_destroy_qp(qp_);
}

void RdmaQueuePair::Shutdown() {
  fprintf(stderr, "TODO: RdmaQueuePair::Shutdown\n");
}

const std::string &RdmaQueuePair::peer_ip() const { return tcp_.peer_ip(); }

uint16_t RdmaQueuePair::peer_tcp_port() const { return tcp_.peer_port(); }

void RdmaManager::BuildProtectionDomain() {
  pd_ = ibv_alloc_pd(ctx_);
  if (!pd_) die_perror("ibv_alloc_pd");
}

void RdmaManager::BuildCompletionQueue() {
  if (executor_.poller_type() == PollerType::kBlocking) {
    comp_channel_ = ibv_create_comp_channel(ctx_);
    if (!comp_channel_) die_perror("ibv_create_comp_channel");
    SetNonBlocking(comp_channel_->fd);
  }

  cq_ = ibv_create_cq(ctx_, kCompletionQueueEntries, nullptr, comp_channel_, 0);
  if (!cq_) die_perror("ibv_create_cq");

  if (executor_.poller_type() == PollerType::kBlocking) {
    int ret = ibv_req_notify_cq(cq_, 0);
    if (ret) die("ibv_req_notify_cq");
  }
}

void RdmaQueuePair::BuildQueuePair() {
  constexpr uint32_t kMaxSendScatterGatherElements = 1;
  constexpr uint32_t kMaxRecvScatterGatherElements = 1;

  ibv_qp_init_attr attr;
  memset(&attr, 0, sizeof(attr));
  attr.send_cq = manager_.cq();
  attr.recv_cq = manager_.cq();
  attr.qp_type = IBV_QPT_RC;
  attr.cap.max_send_wr = kQueuePairSendSize;
  attr.cap.max_recv_wr = kQueuePairRecvSize;
  attr.cap.max_send_sge = kMaxSendScatterGatherElements;
  attr.cap.max_recv_sge = kMaxRecvScatterGatherElements;

  qp_ = ibv_create_qp(manager_.pd(), &attr);
  if (!qp_) die_perror("ibv_create_qp");
}

void RdmaQueuePair::SendConnInfo() {
  ibv_port_attr attr;
  int ret = ibv_query_port(manager_.ctx(), manager_.dev_port(), &attr);
  if (ret) die_perror("SendConnInfo: ibv_query_port");
  ibv_gid gid;
  memset(&gid, 0, sizeof(gid));
  if (!attr.lid) {
    // Only InfiniBand has Local ID. RoCE needs Global ID.
    ibv_query_gid(manager_.ctx(), manager_.dev_port(), 0, &gid);
    if (ret) die_perror("SendConnInfo: ibv_query_gid");
  }

  auto msg = std::make_shared<RdmaManagerMessage>();
  msg->type = RdmaManagerMessage::Type::kConnInfo;
  msg->payload.conn.lid = attr.lid;
  msg->payload.conn.gid = gid;
  msg->payload.conn.qp_num = qp_->qp_num;

  ConstBuffer buf(msg.get(), sizeof(*msg));
  tcp_.AsyncWrite(buf, [this, msg = std::move(msg)](int err, size_t) {
    if (err) {
      fprintf(stderr, "SendConnInfo: AsyncWrite err = %d\n", err);
      die("SendConnInfo AsyncWrite callback");
    }
    RecvConnInfo();
  });
}

void RdmaQueuePair::RecvConnInfo() {
  auto msg = std::make_shared<RdmaManagerMessage>();
  MutableBuffer buf(msg.get(), sizeof(*msg));
  tcp_.AsyncRead(buf, [msg = std::move(msg), this](int err, size_t) {
    if (err) {
      fprintf(stderr, "RecvConnInfo: AsyncRead err = %d (%s), peer=%s:%u.\n",
              err, std::strerror(err), peer_ip().c_str(), peer_tcp_port());
      die("RecvConnInfo AsyncRead callback");
    }
    if (msg->type != RdmaManagerMessage::Type::kConnInfo) {
      fprintf(stderr, "RecvConnInfo: AsyncRead msg->type = %d, peer=%s:%u.\n",
              static_cast<int>(msg->type), peer_ip().c_str(), peer_tcp_port());
      die("RecvConnInfo AsyncRead callback");
    }

    TransitQueuePairToRTR(msg->payload.conn);
    TransitQueuePairToRTS();
    MarkConnected();
    manager_.handler()->OnConnected(this);
    RecvMemoryRegion();
    SendMemoryRegion();
  });
}

void RdmaQueuePair::SendMemoryRegion() {
  auto msg = std::make_shared<RdmaManagerMessage>();
  msg->type = RdmaManagerMessage::Type::kMemoryRegion;
  const auto *mr = manager_.explosed_mr();
  if (mr) {
    msg->payload.mr.addr = reinterpret_cast<uint64_t>(mr->addr);
    msg->payload.mr.size = mr->length;
    msg->payload.mr.rkey = mr->rkey;
  } else {
    msg->payload.mr.addr = 0;
    msg->payload.mr.size = 0;
    msg->payload.mr.rkey = 0;
  }

  ConstBuffer buf(msg.get(), sizeof(*msg));
  tcp_.AsyncWrite(buf, [this, msg = std::move(msg)](int err, size_t) {
    if (err) {
      fprintf(stderr, "SendMemoryRegion: AsyncWrite err = %d\n", err);
      die("SendMemoryRegion AsyncWrite callback");
    }

    // Maybe replace by Promise?
    send_memory_region_done_ = true;
    ShutdownTcp();
  });
}

void RdmaQueuePair::RecvMemoryRegion() {
  auto msg = std::make_shared<RdmaManagerMessage>();
  MutableBuffer buf(msg.get(), sizeof(*msg));
  tcp_.AsyncRead(buf, [msg = std::move(msg), this](int err, size_t) {
    if (err) {
      fprintf(stderr,
              "RecvMemoryRegion: AsyncRead err = %d (%s), peer=%s:%u.\n", err,
              std::strerror(err), peer_ip().c_str(), peer_tcp_port());
      return;
    }
    if (msg->type != RdmaManagerMessage::Type::kMemoryRegion) {
      fprintf(stderr, "RecvMemoryRegion: AsyncRead msg->type = %d peer=%s:%u\n",
              static_cast<int>(msg->type), peer_ip().c_str(), peer_tcp_port());
      return;
    }

    remote_mr_ = msg->payload.mr;
    if (remote_mr_.size) {
      manager_.handler()->OnRemoteMemoryRegionReceived(this, remote_mr_.addr,
                                                       remote_mr_.size);
    }

    // Maybe replace by Promise?
    recv_memory_region_done_ = true;
    ShutdownTcp();
  });
}

void RdmaQueuePair::ShutdownTcp() {
  if (!recv_memory_region_done_) return;
  if (!send_memory_region_done_) return;
  fprintf(stderr, "Close TCP Connection to %s:%u\n", tcp_.peer_ip().c_str(),
          tcp_.peer_port());
  tcp_.Shutdown();
}

void RdmaQueuePair::TransitQueuePairToInit() {
  ibv_qp_attr attr;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = manager_.dev_port();
  attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                         IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

  int ret = ibv_modify_qp(
      qp_, &attr,
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
  if (ret) die_perror("TransitQueuePairToInit");
}

void RdmaQueuePair::TransitQueuePairToRTR(
    const RdmaManagerMessage::ConnInfo &msg) {
  ibv_qp_attr attr;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.ah_attr.port_num = manager_.dev_port();
  attr.ah_attr.dlid = msg.lid;
  attr.path_mtu = IBV_MTU_1024;
  attr.dest_qp_num = msg.qp_num;
  attr.rq_psn = 0;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 9;  // 0.24 ms

  if (msg.lid) {
    // Infiniband
    attr.ah_attr.dlid = msg.lid;
  } else {
    // RoCE
    attr.ah_attr.is_global = true;
    attr.ah_attr.grh.dgid = msg.gid;
    attr.ah_attr.grh.hop_limit = 1;
  }

  int ret = ibv_modify_qp(qp_, &attr,
                          IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                              IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                              IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
  if (ret) die_perror("TransitQueuePairToRTR");
}

void RdmaQueuePair::TransitQueuePairToRTS() {
  ibv_qp_attr attr;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 6;  // 262.144 us
  attr.retry_cnt = 1;
  attr.rnr_retry = 1;
  // attr.retry_cnt = 0;  // no retry
  // attr.rnr_retry = 0;  // no retry
  // attr.retry_cnt = 7;  // infinite retry
  // attr.rnr_retry = 7;  // infinite retry
  attr.max_rd_atomic = 1;

  int ret = ibv_modify_qp(qp_, &attr,
                          IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT |
                              IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                              IBV_QP_MAX_QP_RD_ATOMIC);
  if (ret) die_perror("TransitQueuePairToRTS");
}

void RdmaManager::RegisterLocalMemory(MemoryBlockAllocator *buf) {
  std::lock_guard<std::mutex> lock(mr_mutex_);
  auto *addr = buf->data();
  if (mr_.count(addr))
    die("RdmaManager::RegisterLocalMemory: Already registered.");
  auto *mr = ibv_reg_mr(pd_, addr, buf->pool_size(), IBV_ACCESS_LOCAL_WRITE);
  if (!mr) die("RdmaManager::RegisterLocalMemory: ibv_reg_mr");
  mr_[addr] = mr;
  buf->set_rdma_lkey(mr->lkey);
}

void RdmaManager::ExposeMemory(void *addr, size_t size) {
  if (exposed_mr_)
    die("Currently only one exposed memory region is supported.");
  auto *mr = ibv_reg_mr(pd_, addr, size,
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                            IBV_ACCESS_REMOTE_READ);
  if (!mr) die("ibv_reg_mr: RdmaManager::ExposeMemory");
  exposed_mr_ = mr;
  std::lock_guard<std::mutex> lock(mr_mutex_);
  mr_[addr] = mr;
}

void RdmaManager::ReserveCQ() {
  auto pending = ++cnt_cq_pending_;
  if (pending >= kCompletionQueueEntries) {
    die("cnt_cq_pending_=" + std::to_string(pending));
  }
}

void RdmaQueuePair::PostReceive() { manager_.PostReceive(*this); }

void RdmaManager::PostReceive(RdmaQueuePair &conn) {
  ibv_recv_wr wr, *bad_wr = nullptr;
  ibv_sge sge;

  wr.next = nullptr;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  auto buf = recv_buf_->Allocate();
  sge.addr = reinterpret_cast<uint64_t>(buf.data());
  sge.length = buf.size();
  sge.lkey = buf.rdma_lkey();
  uint64_t seqnum;
  {
    std::lock_guard<std::mutex> lock(conn.wr_ctx_mutex_);
    seqnum = conn.recv_wr_ctx_.Enqueue(std::move(buf));
  }
  wr.wr_id = InternalWrid{seqnum, conn.index_, false}.Encode();

  ReserveCQ();
  int ret = ibv_post_recv(conn.qp_, &wr, &bad_wr);
  if (ret) die("ibv_post_recv");
  if (++conn.cnt_recv_pending_ > kQueuePairRecvSize) {
    fprintf(stderr, "conn.cnt_recv_pending_ > kQueuePairRecvSize. peer=%s:%d\n",
            conn.peer_ip().c_str(), conn.peer_tcp_port());
    die("conn.cnt_recv_pending_ > kQueuePairRecvSize");
  }
}

void RdmaQueuePair::AsyncSend(OwnedMemoryBlock buf) {
  if (!is_connected_) die("Send: not connected.");
  manager_.AsyncSend(*this, std::move(buf));
}

void RdmaManager::AsyncSend(RdmaQueuePair &conn, OwnedMemoryBlock buf) {
  ibv_send_wr wr, *bad_wr = nullptr;
  ibv_sge sge;

  memset(&wr, 0, sizeof(wr));
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  auto msg = buf.AsMessageView();
  sge.addr = reinterpret_cast<uint64_t>(buf.data());
  sge.length = msg.total_length();
  sge.lkey = buf.rdma_lkey();

  uint64_t seqnum;
  {
    std::lock_guard<std::mutex> lock(conn.wr_ctx_mutex_);
    seqnum = conn.out_wr_ctx_.Enqueue(std::move(buf));
  }
  wr.wr_id = InternalWrid{seqnum, conn.index_, true}.Encode();

  ReserveCQ();
  int ret = ibv_post_send(conn.qp_, &wr, &bad_wr);
  if (ret) die_perror("Connection::Send: ibv_post_send");
  if (++conn.cnt_send_pending_ > kQueuePairSendSize) {
    fprintf(stderr,
            "conn.cnt_send_pending_=%d > kQueuePairSendSize. peer=%s:%d\n",
            conn.cnt_send_pending_.load(), conn.peer_ip().c_str(),
            conn.peer_tcp_port());
    die("conn.cnt_send_pending_ > kQueuePairSendSize");
  }
}

WorkRequestID RdmaQueuePair::AsyncRead(OwnedMemoryBlock buf, size_t offset,
                                       size_t length) {
  return manager_.AsyncRead(*this, std::move(buf), offset, length);
}

WorkRequestID RdmaManager::AsyncRead(RdmaQueuePair &conn, OwnedMemoryBlock buf,
                                     size_t offset, size_t length) {
  ibv_send_wr wr, *bad_wr = nullptr;
  ibv_sge sge;

  memset(&wr, 0, sizeof(wr));
  wr.opcode = IBV_WR_RDMA_READ;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = conn.remote_mr_.addr + offset;
  wr.wr.rdma.rkey = conn.remote_mr_.rkey;

  auto msg = buf.AsMessageView();
  sge.addr = reinterpret_cast<uintptr_t>(msg.bytes());
  sge.length = length;
  sge.lkey = buf.rdma_lkey();
  msg.set_bytes_length(length);

  uint64_t seqnum;
  {
    std::lock_guard<std::mutex> lock(conn.wr_ctx_mutex_);
    seqnum = conn.out_wr_ctx_.Enqueue(std::move(buf));
  }
  wr.wr_id = InternalWrid{seqnum, conn.index_, true}.Encode();

  ReserveCQ();
  int ret = ibv_post_send(conn.qp_, &wr, &bad_wr);
  if (ret) die("Connection::PostRead: ibv_post_send");
  if (++conn.cnt_send_pending_ > kQueuePairSendSize) {
    fprintf(stderr, "conn.cnt_send_pending_ > kQueuePairSendSize. peer=%s:%d\n",
            conn.peer_ip().c_str(), conn.peer_tcp_port());
    die("conn.cnt_send_pending_ > kQueuePairSendSize");
  }
  return WorkRequestID(wr.wr_id);
}

RdmaManager::CompletionChannelHandler::CompletionChannelHandler(
    RdmaManager *outer)
    : outer_(*outer) {}

void RdmaManager::CompletionChannelHandler::HandleEpollEvent(
    uint32_t epoll_events) {
  outer_.PollCompletionQueueBlocking();
}

RdmaManager::CompletionQueuePoller::CompletionQueuePoller(RdmaManager *outer)
    : outer_(*outer) {}

void RdmaManager::CompletionQueuePoller::Poll() {
  outer_.PollCompletionQueueSpinning();
}

void RdmaManager::PollCompletionQueueBlocking() {
  if (stop_) {
    return;
  }

  struct ibv_cq *cq;
  void *ev_ctx;
  int ret = ibv_get_cq_event(comp_channel_, &cq, &ev_ctx);
  if (ret < 0) {
    fprintf(stderr,
            "PollCompletionQueueEventLoop: ibv_get_cq_event returns %d\n", ret);
    return;
  }

  ibv_ack_cq_events(cq, 1);
  ret = ibv_req_notify_cq(cq, 0);
  if (ret) {
    fprintf(stderr, "ibv_req_notify_cq\n");
    return;
  }

  PollCompletionQueue();
}

void RdmaManager::PollCompletionQueueSpinning() {
  if (stop_) {
    return;
  }
  PollCompletionQueue();
}

void RdmaManager::PollCompletionQueue() {
  constexpr size_t kPollBatch = 16;
  struct ibv_wc wc[kPollBatch];

  int n;
  do {
    n = ibv_poll_cq(cq_, kPollBatch, wc);
    cnt_cq_pending_ -= n;
    for (int i = 0; i < n; ++i) {
      auto encoding = InternalWrid::Decode(wc[i].wr_id);
      auto *conn = connections_.at(encoding.qp_idx).get();
      if (!encoding.is_out) {
        if (--conn->cnt_recv_pending_ <= 0) {
          fprintf(stderr, "cnt_recv_pending_=%d peer=%s:%d\n",
                  conn->cnt_recv_pending_.load(), conn->peer_ip().c_str(),
                  conn->peer_tcp_port());
          die("cnt_recv_pending_==0");
        }

        PostReceive(*conn);
      } else {
        --conn->cnt_send_pending_;
      }
    }
    for (int i = 0; i < n; ++i) {
      HandleWorkCompletion(&wc[i]);
    }
  } while (n > 0);
}

void RdmaManager::HandleWorkCompletion(ibv_wc *wc) {
  auto encoding = InternalWrid::Decode(wc->wr_id);
  auto *conn = connections_.at(encoding.qp_idx).get();
  if (wc->status != IBV_WC_SUCCESS) {
    const char *op = encoding.is_out ? "SEND" : "RECV";
    const char *wc_status = ibv_wc_status_str(wc->status);
    fprintf(stderr, "COMPLETION FAILURE (%s WR #%lu) peer=%s:%d status=%d %s\n",
            op, encoding.seqnum, conn->peer_ip().c_str(), conn->peer_tcp_port(),
            wc->status, wc_status);
    die("wc->status != IBV_WC_SUCCESS");
  }
  OwnedMemoryBlock buf;
  {
    std::lock_guard<std::mutex> lock(conn->wr_ctx_mutex_);
    if (encoding.is_out) {
      buf = std::move(conn->out_wr_ctx_.Dequeue(encoding.seqnum));
    } else {
      buf = std::move(conn->recv_wr_ctx_.Dequeue(encoding.seqnum));
    }
  }
  if (wc->opcode & IBV_WC_RECV) {
    handler_->OnRecv(conn, std::move(buf));
    return;
  }
  switch (wc->opcode) {
    case IBV_WC_SEND: {
      handler_->OnSent(conn, std::move(buf));
      return;
    }
    case IBV_WC_RDMA_READ: {
      handler_->OnRdmaReadComplete(conn, WorkRequestID(wc->wr_id),
                                   std::move(buf));
      return;
    }
    // TODO: handle all opcode
    case IBV_WC_RDMA_WRITE:
    default:
      fprintf(stderr, "Unknown wc->opcode %d\n", wc->opcode);
      return;
  }
}

void RdmaQueuePair::MarkConnected() {
  for (int i = 0; i < kQueuePairRecvBacklog; ++i) {
    PostReceive();
  }

  is_connected_ = true;
}

}  // namespace ario
