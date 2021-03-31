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

// For std::variant
// See: https://bitbashing.io/std-visit.html

namespace {

template <class... Fs>
struct overload;

template <class F0, class... Frest>
struct overload<F0, Frest...> : F0, overload<Frest...> {
  overload(F0 f0, Frest... rest) : F0(f0), overload<Frest...>(rest...) {}

  using F0::operator();
  using overload<Frest...>::operator();
};

template <class F0>
struct overload<F0> : F0 {
  overload(F0 f0) : F0(f0) {}

  using F0::operator();
};

}  // namespace

template <class... Fs>
constexpr auto make_visitor(Fs... fs) {
  return overload<Fs...>(fs...);
}

}  // namespace nexus

#endif
