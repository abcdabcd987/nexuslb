#ifndef NEXUS_COMMON_UTIL_H_
#define NEXUS_COMMON_UTIL_H_

#include <gflags/gflags.h>

#include <string>

#include "nexus/common/device.h"

DECLARE_bool(hack_reply_omit_output);

namespace nexus {

void SplitString(const std::string &str, char delim,
                 std::vector<std::string> *tokens);

void Memcpy(void *dst, const Device *dst_device, const void *src,
            const Device *src_device, size_t nbytes);

// GetIpAddress returns the first IP addres that is not localhost (127.0.0.1)
std::string GetIpAddress(const std::string &prefix);

void PinCpu(int cpu);

}  // namespace nexus

#endif  // NEXUS_COMMON_UTIL_H_
