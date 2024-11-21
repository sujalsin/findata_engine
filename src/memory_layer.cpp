#include "findata_engine/memory_layer.hpp"
#include "findata_engine/utils.hpp"
#include <unordered_map>
#include <algorithm>
#include <mutex>
#include <shared_mutex>

namespace findata_engine {

struct MemoryLayer::Impl {
    struct SymbolData {
        std::vector<TimeSeriesPoint> points;
        std::shared_mutex mutex;
        size_t total_points = 0;
        
        explicit SymbolData() = default;
    };
    
    std::unordered_map<std::string, std::unique_ptr<SymbolData>> symbol_data;
    size_t cache_size_mb;
    mutable std::shared_mutex global_mutex;
    std::atomic<size_t> total_points_{0};
    
    explicit Impl(size_t cache_size_mb) : cache_size_mb(cache_size_mb) {}
    
    SymbolData* get_or_create_symbol_data(const std::string& symbol) {
        std::shared_lock read_lock(global_mutex);
        auto it = symbol_data.find(symbol);
        if (it != symbol_data.end()) {
            return it->second.get();
        }
        
        read_lock.unlock();
        std::unique_lock write_lock(global_mutex);
        
        // Check again in case another thread created it
        it = symbol_data.find(symbol);
        if (it != symbol_data.end()) {
            return it->second.get();
        }
        
        auto [new_it, _] = symbol_data.emplace(
            symbol,
            std::make_unique<SymbolData>());
        return new_it->second.get();
    }
};

MemoryLayer::MemoryLayer(size_t cache_size_mb)
    : pimpl_(std::make_unique<Impl>(cache_size_mb)) {}

MemoryLayer::~MemoryLayer() = default;

bool MemoryLayer::insert(const TimeSeriesPoint& point) {
    auto* symbol_data = pimpl_->get_or_create_symbol_data(point.symbol);
    
    std::unique_lock lock(symbol_data->mutex);
    
    // Insert into sorted position
    auto it = std::lower_bound(
        symbol_data->points.begin(),
        symbol_data->points.end(),
        point,
        [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
            return a.timestamp < b.timestamp;
        });
    
    // Don't allow duplicates
    if (it != symbol_data->points.end() && it->timestamp == point.timestamp) {
        return false;
    }
    
    symbol_data->points.insert(it, point);
    symbol_data->total_points++;
    pimpl_->total_points_++;
    return true;
}

bool MemoryLayer::insert_batch(const std::vector<TimeSeriesPoint>& points) {
    if (points.empty()) return true;
    
    // Group points by symbol
    std::unordered_map<std::string, std::vector<TimeSeriesPoint>> grouped_points;
    for (const auto& point : points) {
        grouped_points[point.symbol].push_back(point);
    }
    
    // Insert each group
    bool success = true;
    for (const auto& [symbol, symbol_points] : grouped_points) {
        auto* symbol_data = pimpl_->get_or_create_symbol_data(symbol);
        std::unique_lock lock(symbol_data->mutex);
        
        // Pre-allocate space
        symbol_data->points.reserve(symbol_data->points.size() + symbol_points.size());
        
        // Sort new points
        auto sorted_points = symbol_points;
        std::sort(sorted_points.begin(), sorted_points.end(),
                 [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
                     return a.timestamp < b.timestamp;
                 });
        
        // Merge with existing points
        std::vector<TimeSeriesPoint> merged;
        merged.reserve(symbol_data->points.size() + sorted_points.size());
        
        std::merge(
            symbol_data->points.begin(), symbol_data->points.end(),
            sorted_points.begin(), sorted_points.end(),
            std::back_inserter(merged),
            [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
                return a.timestamp < b.timestamp;
            });
        
        // Remove duplicates
        auto unique_end = std::unique(
            merged.begin(), merged.end(),
            [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
                return a.timestamp == b.timestamp;
            });
        
        merged.erase(unique_end, merged.end());
        
        // Update points
        size_t new_points = merged.size() - symbol_data->points.size();
        symbol_data->points = std::move(merged);
        symbol_data->total_points += new_points;
        pimpl_->total_points_ += new_points;
    }
    
    return success;
}

std::optional<TimeSeriesPoint> MemoryLayer::get_latest(const std::string& symbol) const {
    std::shared_lock global_lock(pimpl_->global_mutex);
    auto it = pimpl_->symbol_data.find(symbol);
    if (it == pimpl_->symbol_data.end()) {
        return std::nullopt;
    }
    
    std::shared_lock symbol_lock(it->second->mutex);
    if (it->second->points.empty()) {
        return std::nullopt;
    }
    
    return it->second->points.back();
}

std::vector<TimeSeriesPoint> MemoryLayer::get_range(
    const std::string& symbol,
    std::chrono::system_clock::time_point start,
    std::chrono::system_clock::time_point end) const {
    
    std::shared_lock global_lock(pimpl_->global_mutex);
    auto it = pimpl_->symbol_data.find(symbol);
    if (it == pimpl_->symbol_data.end()) {
        return {};
    }
    
    std::shared_lock symbol_lock(it->second->mutex);
    const auto& points = it->second->points;
    
    // Find range using binary search
    auto start_it = std::lower_bound(
        points.begin(), points.end(),
        TimeSeriesPoint{.symbol = symbol, .timestamp = start, .value = 0.0},
        [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
            return a.timestamp < b.timestamp;
        });
        
    auto end_it = std::upper_bound(
        start_it, points.end(),
        TimeSeriesPoint{.symbol = symbol, .timestamp = end, .value = 0.0},
        [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
            return a.timestamp < b.timestamp;
        });
    
    return {start_it, end_it};
}

void MemoryLayer::clear_cache() {
    std::unique_lock lock(pimpl_->global_mutex);
    for (auto& [_, symbol_data] : pimpl_->symbol_data) {
        std::unique_lock symbol_lock(symbol_data->mutex);
        symbol_data->points.clear();
        symbol_data->total_points = 0;
    }
    pimpl_->total_points_ = 0;
}

size_t MemoryLayer::cache_size() const {
    return pimpl_->total_points_.load();
}

size_t MemoryLayer::get_total_points() const {
    return pimpl_->total_points_.load();
}

double MemoryLayer::get_cache_hit_ratio() const {
    return 1.0; // We don't use caching anymore
}

std::unordered_set<std::string> MemoryLayer::get_symbols() const {
    std::shared_lock lock(pimpl_->global_mutex);
    std::unordered_set<std::string> symbols;
    symbols.reserve(pimpl_->symbol_data.size());
    
    for (const auto& [symbol, _] : pimpl_->symbol_data) {
        symbols.insert(symbol);
    }
    
    return symbols;
}

} // namespace findata_engine
