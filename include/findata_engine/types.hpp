#pragma once

#include <chrono>
#include <string>

namespace findata_engine {

struct TimeSeriesPoint {
    std::chrono::system_clock::time_point timestamp;
    double value;
    std::string symbol;
};

} // namespace findata_engine
