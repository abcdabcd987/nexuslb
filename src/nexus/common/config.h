#ifndef NEXUS_CONFIG_H_
#define NEXUS_CONFIG_H_

#define DISPATCHER_RPC_DEFAULT_PORT 7003
#define BACKEND_DEFAULT_PORT 8001
#define BACKEND_DEFAULT_RPC_PORT 8002
#define FRONTEND_DEFAULT_PORT 9001
#define FRONTEND_DEFAULT_RPC_PORT 9002
#define SCHEDULER_DEFAULT_PORT 7001
#define BEACON_INTERVAL_SEC 2
#define EPOCH_INTERVAL_SEC 10

constexpr int kSmallBufferBlockBits = __builtin_ctzl(8 << 10);
constexpr int kSmallBufferPoolBits = kSmallBufferBlockBits + 18;
constexpr int kLargeBufferBlockBits = __builtin_ctzl(256 << 10);
constexpr int kLargeBufferPoolBits = kLargeBufferBlockBits + 12;
static_assert((1ULL << kLargeBufferBlockBits) > 3 * 224 * 224 * sizeof(char),
              "kLargeBufferBlockBits too few");

#endif  // NEXUS_CONFIG_H_
