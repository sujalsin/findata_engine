#include "findata_engine/storage_engine.hpp"
#include <filesystem>
#include <stdexcept>
#include <shared_mutex>

namespace findata_engine {

struct StorageEngine::Impl {
    EngineConfig config;
    std::unique_ptr<MemoryLayer> memory_layer;
    std::unique_ptr<DiskLayer> disk_layer;
    mutable std::shared_mutex mutex;
    std::atomic<size_t> total_points{0};
    std::atomic<size_t> cache_hits{0};
    std::atomic<size_t> cache_misses{0};
    
    explicit Impl(const EngineConfig& cfg) : config(cfg) {
        if (!std::filesystem::exists(config.data_directory)) {
            std::filesystem::create_directories(config.data_directory);
        }
        
        memory_layer = std::make_unique<MemoryLayer>(config.memory_cache_size_mb);
        disk_layer = std::make_unique<DiskLayer>(config.data_directory);
    }
    
    bool write_point(const TimeSeriesPoint& point) {
        std::unique_lock lock(mutex);
        
        if (!memory_layer->insert(point)) {
            return false;
        }
        
        // If memory layer is getting full, schedule a flush
        bool needs_flush = memory_layer->cache_size() >= config.max_memory_points;
        total_points++;
        
        // Release lock before flushing
        lock.unlock();
        
        if (needs_flush) {
            return flush();
        }
        
        return true;
    }
    
    bool write_batch(const std::vector<TimeSeriesPoint>& points) {
        if (points.empty()) return true;
        
        std::unique_lock lock(mutex);
        
        if (!memory_layer->insert_batch(points)) {
            return false;
        }
        
        // If memory layer is getting full, schedule a flush
        bool needs_flush = memory_layer->cache_size() >= config.max_memory_points;
        total_points += points.size();
        
        // Release lock before flushing
        lock.unlock();
        
        if (needs_flush) {
            return flush();
        }
        
        return true;
    }
    
    bool flush() {
        // First get all data under a lock
        std::vector<TimeSeriesPoint> points_to_flush;
        {
            std::unique_lock lock(mutex);
            auto symbols = memory_layer->get_symbols();
            
            for (const auto& symbol : symbols) {
                auto symbol_points = memory_layer->get_range(
                    symbol,
                    std::chrono::system_clock::time_point::min(),
                    std::chrono::system_clock::time_point::max()
                );
                points_to_flush.insert(
                    points_to_flush.end(),
                    symbol_points.begin(),
                    symbol_points.end()
                );
            }
        }
        
        if (points_to_flush.empty()) {
            return true;
        }
        
        // Write to disk without holding the lock
        bool success = disk_layer->write_batch(points_to_flush);
        
        // If successful, clear memory under lock
        if (success) {
            std::unique_lock lock(mutex);
            memory_layer->clear_cache();
        }
        
        return success;
    }
    
    std::vector<TimeSeriesPoint> read_range(
        const std::string& symbol,
        std::chrono::system_clock::time_point start,
        std::chrono::system_clock::time_point end) {
        
        // Get memory points under shared lock
        std::vector<TimeSeriesPoint> memory_points;
        {
            std::shared_lock lock(mutex);
            memory_points = memory_layer->get_range(symbol, start, end);
        }
        
        // Get disk points without lock
        auto disk_points = disk_layer->read_range(symbol, start, end);
        
        // Merge results
        std::vector<TimeSeriesPoint> result;
        result.reserve(memory_points.size() + disk_points.size());
        result.insert(result.end(), memory_points.begin(), memory_points.end());
        result.insert(result.end(), disk_points.begin(), disk_points.end());
        
        // Sort by timestamp
        std::sort(result.begin(), result.end(),
                  [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
                      return a.timestamp < b.timestamp;
                  });
        
        return result;
    }
    
    std::optional<TimeSeriesPoint> get_latest(const std::string& symbol) {
        // First try memory layer
        {
            std::shared_lock lock(mutex);
            if (auto point = memory_layer->get_latest(symbol)) {
                return point;
            }
        }
        
        // Then try disk layer
        auto disk_points = disk_layer->read_range(
            symbol,
            std::chrono::system_clock::time_point::min(),
            std::chrono::system_clock::time_point::max()
        );
        
        if (!disk_points.empty()) {
            return disk_points.back();
        }
        
        return std::nullopt;
    }
    
    std::unordered_set<std::string> get_symbols() const {
        std::shared_lock lock(mutex);
        return memory_layer->get_symbols();
    }
    
    void optimize() {
        // First flush everything
        flush();
        
        // Then optimize disk layer
        disk_layer->optimize_index();
    }
    
    double get_cache_hit_ratio() const {
        const auto total_hits = cache_hits.load();
        const auto total_misses = cache_misses.load();
        const auto total_requests = total_hits + total_misses;
        
        return total_requests > 0 ? 
            static_cast<double>(total_hits) / total_requests : 0.0;
    }
    
    size_t get_total_points() const {
        return total_points.load();
    }
    
    size_t get_storage_size() const {
        return disk_layer->get_storage_size();
    }
};

StorageEngine::StorageEngine(const EngineConfig& config)
    : pimpl_(std::make_unique<Impl>(config)) {}

StorageEngine::~StorageEngine() = default;

bool StorageEngine::write_point(const TimeSeriesPoint& point) {
    return pimpl_->write_point(point);
}

bool StorageEngine::write_batch(const std::vector<TimeSeriesPoint>& points) {
    return pimpl_->write_batch(points);
}

bool StorageEngine::flush() {
    return pimpl_->flush();
}

std::vector<TimeSeriesPoint> StorageEngine::read_range(
    const std::string& symbol,
    std::chrono::system_clock::time_point start,
    std::chrono::system_clock::time_point end) {
    return pimpl_->read_range(symbol, start, end);
}

std::optional<TimeSeriesPoint> StorageEngine::get_latest(const std::string& symbol) {
    return pimpl_->get_latest(symbol);
}

std::unordered_set<std::string> StorageEngine::get_symbols() const {
    return pimpl_->get_symbols();
}

void StorageEngine::optimize() {
    pimpl_->optimize();
}

EngineStats StorageEngine::get_stats() const {
    return EngineStats{
        .total_points = pimpl_->get_total_points(),
        .cache_hits = pimpl_->cache_hits.load(),
        .cache_misses = pimpl_->cache_misses.load(),
        .cache_hit_ratio = pimpl_->get_cache_hit_ratio(),
        .storage_size_bytes = pimpl_->get_storage_size()
    };
}

} // namespace findata_engine
