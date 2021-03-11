#pragma once
#include <chrono>
#include <type_traits>

namespace ario {

using Clock = std::chrono::system_clock;
using TimePoint = std::chrono::time_point<Clock, std::chrono::nanoseconds>;
static_assert(std::is_same<Clock::time_point, TimePoint>::value,
              "Precision of the system_clock is not nanoseconds.");

}  // namespace ario
