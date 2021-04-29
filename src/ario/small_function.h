#pragma once
#include <functional>
#include <type_traits>

namespace ario {

namespace _detail {
class Foo;
union PointerType {
  void* object;
  void (*func)();
  void (Foo::*member)();
};
constexpr size_t kMaxSize = sizeof(PointerType);

template <typename F>
struct IsLocalStorage {
  static constexpr bool value = sizeof(F) <= kMaxSize;
};
}  // namespace _detail

template <typename _>
class SmallFunction;

template <typename R, typename... Args>
class SmallFunction<R(Args...)> {
 public:
  template <typename F,
            typename = std::enable_if_t<_detail::IsLocalStorage<F>::value>>
  constexpr SmallFunction(F f) : func_(std::move(f)) {}

  template <typename F,
            typename = std::enable_if_t<!_detail::IsLocalStorage<F>::value>>
  constexpr SmallFunction(F f, ...) {
    static_assert(sizeof(F) < _detail::kMaxSize, "Function size too big");
  }

  constexpr typename std::function<R(Args...)> Release() {
    return std::move(func_);
  }

 private:
  typename std::function<R(Args...)> func_;
};

}  // namespace ario
