#include "ario/rdma.h"

#include "ario/utils.h"

namespace ario {

RdmaManager::RdmaManager(std::string dev_name,
                         std::shared_ptr<EventHandler> handler,
                         MemoryBlockAllocator *recv_buf)
    : dev_name_(std::move(dev_name)),
      handler_(std::move(handler)),
      recv_buf_(recv_buf),
      poller_type_(PollerType::kBlocking),
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
  if (poller_type_ == PollerType::kBlocking) {
    cq_poller_thread_ =
        std::thread(&RdmaManager::PollCompletionQueueBlocking, this);
  } else {
    cq_poller_thread_ =
        std::thread(&RdmaManager::PollCompletionQueueSpinning, this);
  }
}

RdmaManager::~RdmaManager() {
  poller_stop_ = true;
  connections_.clear();
  cq_poller_thread_.join();
  if (ctx_) ibv_close_device(ctx_);
}

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
    fprintf(stderr,
            "Found ibv device: name=%s, guid=0x%016lx. Active ports:", name,
            ibv_get_device_guid(devs[i]));
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

    AddConnection(std::move(peer));
    TcpAccept();
  });
}

void RdmaManager::ConnectTcp(const std::string &host, uint16_t port) {
  fprintf(stderr, "Connecting TCP to host %s port %u\n", host.c_str(), port);
  tcp_socket_.Connect(executor_, host, port);
  fprintf(stderr, "TCP socket connected\n");
  AddConnection(std::move(tcp_socket_));
}

void RdmaManager::RunEventLoop() { executor_.RunEventLoop(); }

void RdmaManager::StopEventLoop() { executor_.StopEventLoop(); }

void RdmaManager::AddConnection(TcpSocket tcp) {
  auto conn = new RdmaQueuePair(RdmaManagerAccessor(*this), std::move(tcp));
  connections_.emplace_back(conn);
}

RdmaQueuePair::RdmaQueuePair(RdmaManagerAccessor manager, TcpSocket tcp)
    : manager_(manager), tcp_(std::move(tcp)) {
  int ret;
  is_connected_ = false;

  BuildQueuePair();
  TransitQueuePairToInit();
  SendConnInfo();
  RecvConnInfo();
}

RdmaQueuePair::~RdmaQueuePair() {
  if (qp_) ibv_destroy_qp(qp_);
}

void RdmaManager::BuildProtectionDomain() {
  pd_ = ibv_alloc_pd(ctx_);
  if (!pd_) die_perror("ibv_alloc_pd");
}

void RdmaManager::BuildCompletionQueue() {
  constexpr int kNumCompletionQueueEntries = 100;

  if (poller_type_ == PollerType::kBlocking) {
    comp_channel_ = ibv_create_comp_channel(ctx_);
    if (!comp_channel_) die_perror("ibv_create_comp_channel");
    SetNonBlocking(comp_channel_->fd);
    comp_channel_pollfd_.fd = comp_channel_->fd;
    comp_channel_pollfd_.events = POLLIN;
    comp_channel_pollfd_.revents = 0;
  } else {
    comp_channel_ = nullptr;
  }

  cq_ = ibv_create_cq(ctx_, kNumCompletionQueueEntries, nullptr, comp_channel_,
                      0);
  if (!cq_) die_perror("ibv_create_cq");

  if (poller_type_ == PollerType::kBlocking) {
    int ret = ibv_req_notify_cq(cq_, 0);
    if (ret) die("ibv_req_notify_cq");
  }
}

void RdmaQueuePair::BuildQueuePair() {
  constexpr uint32_t kMaxSendQueueSize = 1024;
  constexpr uint32_t kMaxRecvQueueSize = 1024;
  constexpr uint32_t kMaxSendScatterGatherElements = 16;
  constexpr uint32_t kMaxRecvScatterGatherElements = 16;

  ibv_qp_init_attr attr;
  memset(&attr, 0, sizeof(attr));
  attr.send_cq = manager_.cq();
  attr.recv_cq = manager_.cq();
  attr.qp_type = IBV_QPT_RC;
  attr.cap.max_send_wr = kMaxSendQueueSize;
  attr.cap.max_recv_wr = kMaxRecvQueueSize;
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
  fprintf(stderr, "local ConnInfo: qp_num=%d, lid=%d, gid[0]=%016lx:%016lx\n",
          qp_->qp_num, attr.lid, gid.global.subnet_prefix,
          gid.global.interface_id);

  fprintf(stderr, "Sending ConnInfo\n");
  ConstBuffer buf(msg.get(), sizeof(*msg));
  tcp_.AsyncWrite(buf, [msg = std::move(msg)](int err, size_t) {
    if (err) {
      fprintf(stderr, "SendConnInfo: AsyncWrite err = %d\n", err);
      die("SendConnInfo AsyncWrite callback");
    }
    fprintf(stderr, "ConnInfo sent\n");
  });
}

void RdmaQueuePair::RecvConnInfo() {
  fprintf(stderr, "Waiting for peer ConnInfo\n");
  auto msg = std::make_shared<RdmaManagerMessage>();
  MutableBuffer buf(msg.get(), sizeof(*msg));
  tcp_.AsyncRead(buf, [msg = std::move(msg), this](int err, size_t) {
    if (err) {
      fprintf(stderr, "RecvConnInfo: AsyncRead err = %d\n", err);
      die("RecvConnInfo AsyncRead callback");
    }
    if (msg->type != RdmaManagerMessage::Type::kConnInfo) {
      fprintf(stderr, "RecvConnInfo: AsyncRead msg->type = %d\n",
              static_cast<int>(msg->type));
      die("RecvConnInfo AsyncRead callback");
    }
    fprintf(stderr, "Received peer ConnInfo\n");

    TransitQueuePairToRTR(msg->payload.conn);
    TransitQueuePairToRTS();
    MarkConnected();
    manager_.handler()->OnConnected(this);
    RecvMemoryRegion();
    if (manager_.explosed_mr()) {
      SendMemoryRegion();
    }
  });
}

void RdmaQueuePair::SendMemoryRegion() {
  fprintf(stderr, "Sending MemoryRegion\n");
  auto msg = std::make_shared<RdmaManagerMessage>();
  msg->type = RdmaManagerMessage::Type::kMemoryRegion;
  msg->payload.mr.addr =
      reinterpret_cast<uint64_t>(manager_.explosed_mr()->addr);
  msg->payload.mr.size = manager_.explosed_mr()->length;
  msg->payload.mr.rkey = manager_.explosed_mr()->rkey;

  ConstBuffer buf(msg.get(), sizeof(*msg));
  tcp_.AsyncWrite(buf, [msg = std::move(msg)](int err, size_t) {
    if (err) {
      fprintf(stderr, "SendMemoryRegion: AsyncWrite err = %d\n", err);
      die("SendMemoryRegion AsyncWrite callback");
    }
    fprintf(stderr, "MemoryRegion sent\n");
  });
}

void RdmaQueuePair::RecvMemoryRegion() {
  fprintf(stderr, "Waiting for peer MemoryRegion\n");
  auto msg = std::make_shared<RdmaManagerMessage>();
  MutableBuffer buf(msg.get(), sizeof(*msg));
  tcp_.AsyncRead(buf, [msg = std::move(msg), this](int err, size_t) {
    if (err) {
      fprintf(stderr,
              "RecvMemoryRegion: AsyncRead err = %d. "
              "TODO: handler client disconnect.\n",
              err);
      return;
    }
    if (msg->type != RdmaManagerMessage::Type::kMemoryRegion) {
      fprintf(stderr, "RecvMemoryRegion: AsyncRead msg->type = %d\n",
              static_cast<int>(msg->type));
      return;
    }

    remote_mr_ = msg->payload.mr;
    manager_.handler()->OnRemoteMemoryRegionReceived(this, remote_mr_.addr,
                                                     remote_mr_.size);
  });
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
  fprintf(stderr, "remote ConnInfo: qp_num=%d, lid=%d, gid[0]=%016lx:%016lx\n",
          msg.qp_num, msg.lid, msg.gid.global.subnet_prefix,
          msg.gid.global.interface_id);
  ibv_qp_attr attr;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.ah_attr.port_num = manager_.dev_port();
  attr.ah_attr.dlid = msg.lid;
  attr.path_mtu = IBV_MTU_1024;
  attr.dest_qp_num = msg.qp_num;
  attr.rq_psn = 0;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 12;  // 0.640 ms

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
  attr.timeout = 8;  // 1.048 ms
  // attr.timeout = 19;   // 2147 ms
  // attr.retry_cnt = 0;  // no retry
  // attr.rnr_retry = 0;  // no retry
  attr.retry_cnt = 7;  // infinite retry
  attr.rnr_retry = 7;  // infinite retry
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

void RdmaQueuePair::PostReceive() { manager_.PostReceive(*this); }

void RdmaManager::PostReceive(RdmaQueuePair &conn) {
  ibv_recv_wr wr, *bad_wr = nullptr;
  ibv_sge sge;

  wr.wr_id = next_wr_id_.fetch_add(1);
  wr.next = nullptr;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  auto buf = recv_buf_->Allocate();
  sge.addr = reinterpret_cast<uint64_t>(buf.data());
  sge.length = buf.size();
  sge.lkey = buf.rdma_lkey();
  {
    std::lock_guard<std::mutex> lock(wr_ctx_mutex_);
    wr_ctx_[wr.wr_id] =
        std::make_unique<WorkRequestContext>(conn, std::move(buf));
  }

  fprintf(stderr, "POST --> (RECV WR #%lu) [addr %lx, len %u, qp_num %u]\n",
          wr.wr_id, sge.addr, sge.length, conn.qp_->qp_num);
  int ret = ibv_post_recv(conn.qp_, &wr, &bad_wr);
  if (ret) die("ibv_post_recv");
}

void RdmaQueuePair::AsyncSend(OwnedMemoryBlock buf) {
  if (!is_connected_) die("Send: not connected.");
  manager_.AsyncSend(*this, std::move(buf));
}

void RdmaManager::AsyncSend(RdmaQueuePair &conn, OwnedMemoryBlock buf) {
  ibv_send_wr wr, *bad_wr = nullptr;
  ibv_sge sge;

  memset(&wr, 0, sizeof(wr));
  wr.wr_id = next_wr_id_.fetch_add(1);
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  auto msg = buf.AsMessageView();
  sge.addr = reinterpret_cast<uint64_t>(buf.data());
  sge.length = msg.total_length();
  sge.lkey = buf.rdma_lkey();

  {
    std::lock_guard<std::mutex> lock(wr_ctx_mutex_);
    wr_ctx_[wr.wr_id] =
        std::make_unique<WorkRequestContext>(conn, std::move(buf));
  }

  int ret = ibv_post_send(conn.qp_, &wr, &bad_wr);
  if (ret) die("Connection::Send: ibv_post_send");
  fprintf(stderr, "POST --> (SEND WR #%lu) [addr %lx, len %u, qp_num %u]\n",
          wr.wr_id, sge.addr, sge.length, conn.qp_->qp_num);
}

void RdmaQueuePair::AsyncRead(OwnedMemoryBlock buf, size_t offset,
                              size_t length) {
  manager_.AsyncRead(*this, std::move(buf), offset, length);
}

void RdmaManager::AsyncRead(RdmaQueuePair &conn, OwnedMemoryBlock buf,
                            size_t offset, size_t length) {
  ibv_send_wr wr, *bad_wr = nullptr;
  ibv_sge sge;

  memset(&wr, 0, sizeof(wr));
  wr.wr_id = next_wr_id_.fetch_add(1);
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
  msg.bytes_length() = length;

  {
    std::lock_guard<std::mutex> lock(wr_ctx_mutex_);
    wr_ctx_[wr.wr_id] =
        std::make_unique<WorkRequestContext>(conn, std::move(buf));
  }

  int ret = ibv_post_send(conn.qp_, &wr, &bad_wr);
  if (ret) die("Connection::PostRead: ibv_post_send");
  fprintf(stderr, "POST --> (READ WR #%lu) [offset %lx, len %lu, qp_num %u]\n",
          wr.wr_id, offset, length, conn.qp_->qp_num);
}

void RdmaManager::PollCompletionQueueBlocking() {
  constexpr int kPollTimeoutMills = 1;
  struct ibv_cq *cq;
  struct ibv_wc wc;
  void *ev_ctx;
  int ret;

  while (!poller_stop_) {
    do {
      ret = poll(&comp_channel_pollfd_, 1, kPollTimeoutMills);
    } while (ret == 0 && !poller_stop_);
    if (ret < 0) die("PollCompletionQueueBlocking: pool failed");
    if (poller_stop_) {
      break;
    }

    ret = ibv_get_cq_event(comp_channel_, &cq, &ev_ctx);
    if (ret < 0) {
      fprintf(stderr,
              "PollCompletionQueueBlocking: ibv_get_cq_event returns %d\n",
              ret);
      continue;
    }

    ibv_ack_cq_events(cq, 1);
    ret = ibv_req_notify_cq(cq, 0);
    if (ret) {
      fprintf(stderr, "ibv_req_notify_cq\n");
      continue;
    }
    while (!poller_stop_ && ibv_poll_cq(cq, 1, &wc)) {
      HandleWorkCompletion(&wc);
    }
  }
}

void RdmaManager::PollCompletionQueueSpinning() {
  struct ibv_wc wc;
  while (!poller_stop_) {
    while (!poller_stop_ && ibv_poll_cq(cq_, 1, &wc)) {
      HandleWorkCompletion(&wc);
    }
    asm volatile("pause\n" : : : "memory");
  }
}

void RdmaManager::HandleWorkCompletion(ibv_wc *wc) {
  if (wc->status != IBV_WC_SUCCESS) {
    fprintf(stderr, "COMPLETION FAILURE (%s WR #%lu) status[%d] = %s\n",
            (wc->opcode & IBV_WC_RECV) ? "RECV" : "SEND", wc->wr_id, wc->status,
            ibv_wc_status_str(wc->status));
    die("wc->status != IBV_WC_SUCCESS");
  }
  std::unique_ptr<WorkRequestContext> wr_ctx;
  {
    std::lock_guard<std::mutex> lock(wr_ctx_mutex_);
    auto iter = wr_ctx_.find(wc->wr_id);
    if (iter == wr_ctx_.end()) {
      fprintf(stderr, "Cannot find context for wr_id #%lu\n", wc->wr_id);
      die("wc->wr_id not in wr_ctx_");
    }
    wr_ctx = std::move(iter->second);
  }
  if (wc->opcode & IBV_WC_RECV) {
    PostReceive(wr_ctx->conn);
    handler_->OnRecv(std::move(wr_ctx->buf));
    return;
  }
  switch (wc->opcode) {
    case IBV_WC_SEND: {
      handler_->OnSent(std::move(wr_ctx->buf));
      return;
    }
    case IBV_WC_RDMA_READ: {
      handler_->OnRdmaReadComplete(std::move(wr_ctx->buf));
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
  static constexpr size_t kRecvBacklog = 20;
  for (size_t i = 0; i < kRecvBacklog; ++i) {
    PostReceive();
  }

  is_connected_ = true;
}

}  // namespace ario
