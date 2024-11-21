#pragma once

#include "memory_layer.hpp"
#include "disk_layer.hpp"
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <optional>
#include <unordered_set>

namespace findata_engine {

struct DiskLayerConfig {
    size_t disk_cache_size_mb;
    size_t max_disk_segment_size_mb;
};

struct EngineConfig {
    size_t memory_cache_size_mb = 256;
    std::filesystem::path data_directory;
    bool enable_compression = true;
    size_t batch_size = 1000;
    size_t max_segment_size_mb = 64;
    size_t max_memory_points = 1000000; // Maximum points to keep in memory before flushing
    DiskLayerConfig disk_config;
};

struct EngineStats {
    size_t total_points;
    size_t cache_hits;
    size_t cache_misses;
    double cache_hit_ratio;
    size_t storage_size_bytes;
};

class StorageEngine {
public:
    explicit StorageEngine(const EngineConfig& config);
    ~StorageEngine();

    // Write operations
    bool write_point(const TimeSeriesPoint& point);
    bool write_batch(const std::vector<TimeSeriesPoint>& points);
    bool flush();

    // Read operations
    std::vector<TimeSeriesPoint> read_range(
        const std::string& symbol,
        std::chrono::system_clock::time_point start,
        std::chrono::system_clock::time_point end);

    std::optional<TimeSeriesPoint> get_latest(const std::string& symbol);
    std::unordered_set<std::string> get_symbols() const;

    // Maintenance operations
    void optimize();
    EngineStats get_stats() const;

private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
};

} // namespace findata_engine
