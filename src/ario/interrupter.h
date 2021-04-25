#pragma once

namespace ario {

// Design and implementation copied from Boost.Asio. See:
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/eventfd_select_interrupter.hpp
// https://github.com/boostorg/asio/blob/boost-1.75.0/include/boost/asio/detail/impl/eventfd_select_interrupter.ipp
class Interrupter {
 public:
  Interrupter();
  ~Interrupter();
  Interrupter(const Interrupter &other) = delete;
  Interrupter &operator=(const Interrupter &other) = delete;
  Interrupter(Interrupter &&other) = delete;
  Interrupter &operator=(Interrupter &&other) = delete;

  int fd() const;
  void Interrupt();
  void Reset();

 private:
  const int event_fd_;
};

}  // namespace ario
