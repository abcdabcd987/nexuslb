#pragma once
#include <string>

namespace ario {

void die_perror(std::string reason);
void die(std::string reason);
void SetNonBlocking(int fd);

}  // namespace ario
