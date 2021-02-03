#ifndef NEXUS_COMMON_RDMA_SENDER_H
#define NEXUS_COMMON_RDMA_SENDER_H

#include <google/protobuf/message.h>

#include "ario/ario.h"

namespace nexus {

class RdmaSender {
 public:
  explicit RdmaSender(ario::MemoryBlockAllocator* send_buf);
  void SendMessage(ario::RdmaQueuePair* conn,
                   const google::protobuf::Message& message);

 private:
  ario::MemoryBlockAllocator* send_buf_;
};

}  // namespace nexus

#endif
