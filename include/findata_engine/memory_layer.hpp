#pragma once

#include "findata_engine/utils.hpp"
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <shared_mutex>
#include "findata_engine/types.hpp"

namespace findata_engine {

class MemoryLayer {
private:
    struct SymbolData {
        utils::LRUCache<std::chrono::system_clock::time_point, double, utils::TimePointHash> cache;
        size_t cache_hits;
        size_t cache_misses;
        
        explicit SymbolData(size_t cache_size) : cache(cache_size), cache_hits(0), cache_misses(0) {}
    };
    
    std::unordered_map<std::string, std::unique_ptr<SymbolData>> symbols_;
    size_t cache_size_mb_;
    mutable std::shared_mutex mutex_;
    size_t total_points_;
    
public:
    explicit MemoryLayer(size_t cache_size_mb);
    ~MemoryLayer();

    // Write operations
    bool insert(const TimeSeriesPoint& point);
    bool insert_batch(const std::vector<TimeSeriesPoint>& points);

    // Read operations
    std::optional<TimeSeriesPoint> get_latest(const std::string& symbol) const;
    std::vector<TimeSeriesPoint> get_range(
        const std::string& symbol,
        std::chrono::system_clock::time_point start,
        std::chrono::system_clock::time_point end) const;

    // Cache management
    void clear_cache();
    void flush();
    size_t cache_size() const;
    
    // Symbol management
    std::unordered_set<std::string> get_symbols() const;

    // Stats
    size_t get_total_points() const;
    double get_cache_hit_ratio() const;

private:
    struct Impl;
    std::unique_ptr<Impl> pimpl_;
};

} // namespace findata_engine
