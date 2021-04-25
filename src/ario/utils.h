#pragma once
#include <string>

namespace ario {

[[noreturn]] void die_perror(std::string reason);
[[noreturn]] void die(std::string reason);
void SetNonBlocking(int fd);

}  // namespace ario
