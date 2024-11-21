#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <filesystem>
#include "memory_layer.hpp"

namespace findata_engine {

struct DiskConfig {
    bool enable_compression = true;
    size_t batch_size = 1000;
    size_t max_segment_size_mb = 64;
};

class DiskLayer {
public:
    DiskLayer(const std::filesystem::path& data_directory, const DiskConfig& config = DiskConfig{});
    ~DiskLayer();

    // Write operations
    bool write_batch(const std::vector<TimeSeriesPoint>& points);
    bool commit_segment(const std::string& symbol);

    // Read operations
    std::vector<TimeSeriesPoint> read_range(
        const std::string& symbol,
        const std::chrono::system_clock::time_point& start,
        const std::chrono::system_clock::time_point& end);

    // Maintenance operations
    void compact_segments(const std::string& symbol);
    void optimize_index();
    size_t get_storage_size() const;

private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
};

} // namespace findata_engine
