#ifndef NEXUS_COMMON_TYPEDEF_H_
#define NEXUS_COMMON_TYPEDEF_H_

#include <boost/serialization/strong_typedef.hpp>
#include <cstdint>
#include <functional>

#define NS_STRONG_TYPEDEF(NS, T, D)                         \
  namespace NS {                                            \
  BOOST_STRONG_TYPEDEF(T, D)                                \
  }                                                         \
  namespace std {                                           \
  template <>                                               \
  struct hash<NS::D> {                                      \
    std::size_t operator()(NS::D const& s) const noexcept { \
      return std::hash<T>{}(s.t);                           \
    }                                                       \
  };                                                        \
  }

NS_STRONG_TYPEDEF(nexus, uint64_t, GlobalId)

#endif
