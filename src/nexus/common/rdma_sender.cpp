#include "nexus/common/rdma_sender.h"

#include <glog/logging.h>

namespace nexus {

RdmaSender::RdmaSender(ario::MemoryBlockAllocator* send_buf)
    : send_buf_(send_buf) {}

void RdmaSender::SendMessage(ario::RdmaQueuePair* conn,
                             const google::protobuf::Message& message) {
  auto buf = send_buf_->Allocate();
  auto view = buf.AsMessageView();
  view.set_bytes_length(message.ByteSizeLong());
  bool ok = message.SerializeToArray(view.bytes(), view.bytes_length());
  CHECK(ok) << "RdmaSender::SendMessage: failed to SerializeToArray";
  conn->AsyncSend(std::move(buf));
}

}  // namespace nexus
