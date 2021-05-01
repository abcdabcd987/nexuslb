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

// NodeId
NS_STRONG_TYPEDEF(nexus, uint32_t, NodeId)

// QueryId is assigned by each frontend.
// Only meaningful at the origin frontend.
NS_STRONG_TYPEDEF(nexus, uint64_t, QueryId)

// GlobalId is assigned by Dispatcher.
NS_STRONG_TYPEDEF(nexus, uint64_t, GlobalId)

// PlanId is assigned by Dispatcher.
NS_STRONG_TYPEDEF(nexus, uint64_t, PlanId)

// ModelIndex is assigned by Dispatcher. Maps to a model session.
// Start from 0.
NS_STRONG_TYPEDEF(nexus, uint32_t, ModelIndex)

namespace nexus {

struct AvgStd {
  AvgStd() : avg(0.0), std(0.0) {}
  AvgStd(double avg, double std) : avg(avg), std(std) {}

  double avg;
  double std;
};

}  // namespace nexus

#endif
