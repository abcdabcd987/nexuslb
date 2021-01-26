#include "ario/utils.h"

#include <fcntl.h>

#include <cstdio>
#include <cstdlib>

namespace ario {

void die_perror(std::string reason) {
  perror(reason.c_str());
  abort();
}

void die(std::string reason) {
  fprintf(stderr, "die: %s\n", reason.c_str());
  fflush(stderr);
  abort();
}

void SetNonBlocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) die_perror("fcntl");

  flags |= O_NONBLOCK;
  int ret = fcntl(fd, F_SETFL, flags);
  if (ret < 0) die_perror("fcntl");
}

}  // namespace ario
