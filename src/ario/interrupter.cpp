#include "ario/interrupter.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include "ario/utils.h"

namespace ario {

Interrupter::Interrupter() : event_fd_(eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK)) {
  if (event_fd_ < 0) die_perror("eventfd");
}

Interrupter::~Interrupter() { close(event_fd_); }

int Interrupter::fd() const { return event_fd_; }

void Interrupter::Interrupt() {
  uint64_t counter = 1;
  int ret = write(event_fd_, &counter, sizeof(counter));
  (void)ret;
}

void Interrupter::Reset() {
  uint64_t counter = 0;
  int ret = read(event_fd_, &counter, sizeof(counter));
  (void)ret;
}

}  // namespace ario
