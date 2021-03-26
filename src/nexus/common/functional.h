#ifndef NEXUS_COMMON_FUNCTIONAL_H_
#define NEXUS_COMMON_FUNCTIONAL_H_

#include <utility>

namespace nexus {

struct Identity {
  template <typename U>
  constexpr auto operator()(U&& v) const noexcept
      -> decltype(std::forward<U>(v)) {
    return std::forward<U>(v);
  }
};

}  // namespace nexus

#endif
