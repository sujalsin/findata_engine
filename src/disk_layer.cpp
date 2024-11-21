#include "findata_engine/disk_layer.hpp"
#include "findata_engine/rust_bindings.hpp"
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <unordered_map>
#include <shared_mutex>
#include <atomic>

namespace findata_engine {

namespace {
constexpr size_t SEGMENT_SIZE = 64 * 1024 * 1024; // 64MB segments
constexpr size_t INDEX_BLOCK_SIZE = 4096; // 4KB index blocks
} // namespace

struct DiskLayer::Impl {
    struct SegmentInfo {
        std::chrono::system_clock::time_point start_time;
        std::chrono::system_clock::time_point end_time;
        size_t num_points;
        std::string file_path;
        bool compressed;
    };
    
    struct SegmentHeader {
        size_t num_points;
        std::chrono::system_clock::time_point start_time;
        std::chrono::system_clock::time_point end_time;
    };
    
    std::filesystem::path data_dir;
    std::unordered_map<std::string, std::unordered_map<size_t, SegmentInfo>> metadata;
    std::shared_mutex mutex;
    DiskConfig config;
    
    explicit Impl(const std::filesystem::path& dir, const DiskConfig& cfg) 
        : data_dir(dir), config(cfg) {
        std::filesystem::create_directories(dir);
        load_existing_segments();
    }
    
    void load_existing_segments() {
        for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
            if (!entry.is_regular_file()) continue;
            
            const auto& path = entry.path();
            if (path.extension() != ".dat") continue;
            
            // Parse filename: symbol_starttime_endtime.dat
            std::string filename = path.stem().string();
            auto first_underscore = filename.find('_');
            auto last_underscore = filename.rfind('_');
            
            if (first_underscore == std::string::npos || last_underscore == std::string::npos) {
                continue;
            }
            
            std::string symbol = filename.substr(0, first_underscore);
            int64_t start_time = std::stoll(filename.substr(first_underscore + 1, last_underscore - first_underscore - 1));
            int64_t end_time = std::stoll(filename.substr(last_underscore + 1));
            
            SegmentInfo info{
                .start_time = std::chrono::system_clock::time_point(std::chrono::microseconds(start_time)),
                .end_time = std::chrono::system_clock::time_point(std::chrono::microseconds(end_time)),
                .num_points = count_points_in_segment(path),
                .file_path = path.string(),
                .compressed = false
            };
            
            std::unique_lock lock(mutex);
            metadata[symbol][0] = info;
        }
    }
    
    size_t count_points_in_segment(const std::filesystem::path& path) {
        std::ifstream file(path, std::ios::binary);
        if (!file) return 0;
        
        file.seekg(0, std::ios::end);
        auto size = file.tellg();
        file.seekg(0, std::ios::beg);
        
        // Read header to get point count
        uint64_t num_points;
        file.read(reinterpret_cast<char*>(&num_points), sizeof(num_points));
        
        return num_points;
    }
    
    void write_segment(const std::string& symbol,
                      const std::vector<TimeSeriesPoint>& points,
                      size_t segment_id) {
        if (points.empty()) return;

        // Create segment file path
        auto segment_file = (data_dir / (symbol + "_" + std::to_string(segment_id) + ".seg")).string();
        
        std::vector<uint8_t> data;
        size_t data_size = 0;
        
        if (config.enable_compression) {
            // Convert C++ points to Rust TimePoints
            std::vector<TimePoint> rust_points;
            rust_points.reserve(points.size());
            for (const auto& p : points) {
                rust_points.push_back(TimePoint{
                    .timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                        p.timestamp.time_since_epoch()).count(),
                    .value = p.value
                });
            }
            
            // Compress using Rust
            size_t compressed_size;
            uint8_t* compressed = compress_time_series(
                rust_points.data(),
                rust_points.size(),
                &compressed_size
            );
            
            // Copy compressed data
            data.resize(compressed_size);
            std::memcpy(data.data(), compressed, compressed_size);
            data_size = compressed_size;
            
            // Free Rust-allocated memory
            free_compressed_data(compressed, compressed_size);
        } else {
            // Store points directly
            data.resize(points.size() * sizeof(TimeSeriesPoint));
            std::memcpy(data.data(), points.data(), data.size());
            data_size = data.size();
        }
        
        // Write to file
        std::ofstream out(segment_file, std::ios::binary);
        if (!out) {
            throw std::runtime_error("Failed to create segment file: " + segment_file);
        }
        
        // Write header
        SegmentInfo info{
            .start_time = points.front().timestamp,
            .end_time = points.back().timestamp,
            .num_points = points.size(),
            .file_path = segment_file,
            .compressed = config.enable_compression
        };
        
        out.write(reinterpret_cast<const char*>(&info), sizeof(info));
        out.write(reinterpret_cast<const char*>(&data_size), sizeof(data_size));
        out.write(reinterpret_cast<const char*>(data.data()), data_size);
        
        out.close();
        
        // Update metadata
        std::unique_lock lock(mutex);
        metadata[symbol][segment_id] = info;
    }

    std::vector<TimeSeriesPoint> read_segment(const std::string& symbol, size_t segment_id) {
        std::shared_lock lock(mutex);
        
        auto symbol_it = metadata.find(symbol);
        if (symbol_it == metadata.end()) {
            return {};
        }
        
        auto segment_it = symbol_it->second.find(segment_id);
        if (segment_it == symbol_it->second.end()) {
            return {};
        }
        
        const auto& info = segment_it->second;
        
        // Open segment file
        std::ifstream in(info.file_path, std::ios::binary);
        if (!in) {
            throw std::runtime_error("Failed to open segment file: " + info.file_path);
        }
        
        // Skip header
        in.seekg(sizeof(SegmentInfo));
        
        // Read data size
        size_t data_size;
        in.read(reinterpret_cast<char*>(&data_size), sizeof(data_size));
        
        // Read data
        std::vector<uint8_t> data(data_size);
        in.read(reinterpret_cast<char*>(data.data()), data_size);
        
        std::vector<TimeSeriesPoint> points;
        
        if (info.compressed) {
            // Decompress using Rust
            size_t num_points;
            TimePoint* rust_points = decompress_time_series(
                data.data(),
                data_size,
                &num_points
            );
            
            // Convert Rust points to C++ points
            points.reserve(num_points);
            for (size_t i = 0; i < num_points; ++i) {
                points.push_back(TimeSeriesPoint{
                    .symbol = symbol,
                    .timestamp = std::chrono::system_clock::time_point(
                        std::chrono::microseconds(rust_points[i].timestamp)),
                    .value = rust_points[i].value
                });
            }
            
            // Free Rust-allocated memory
            free_time_points(rust_points, num_points);
        } else {
            // Read uncompressed points directly
            const auto* point_data = reinterpret_cast<const TimeSeriesPoint*>(data.data());
            points = std::vector<TimeSeriesPoint>(point_data, point_data + info.num_points);
        }
        
        return points;
    }
    
    std::vector<TimeSeriesPoint> range_query(const std::string& symbol,
                                           std::chrono::system_clock::time_point start,
                                           std::chrono::system_clock::time_point end) {
        std::shared_lock<std::shared_mutex> lock(mutex);
        std::vector<TimeSeriesPoint> results;
        
        auto symbol_it = metadata.find(symbol);
        if (symbol_it == metadata.end()) {
            return results;
        }
        
        // Find relevant segments
        std::vector<size_t> relevant_segments;
        for (const auto& [segment_id, segment_info] : symbol_it->second) {
            if (segment_info.start_time <= end && segment_info.end_time >= start) {
                relevant_segments.push_back(segment_id);
            }
        }
        
        // Read and filter points from each relevant segment
        for (size_t segment_id : relevant_segments) {
            auto points = read_segment(symbol, segment_id);
            for (const auto& point : points) {
                if (point.timestamp >= start && point.timestamp < end) {
                    results.push_back(point);
                }
            }
        }
        
        // Sort results by timestamp
        std::sort(results.begin(), results.end(),
                 [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
                     return a.timestamp < b.timestamp;
                 });
        
        return results;
    }
    
    void optimize_segments(const std::string& symbol) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        
        auto symbol_it = metadata.find(symbol);
        if (symbol_it == metadata.end()) {
            return;
        }
        
        // Collect all points
        std::vector<TimeSeriesPoint> all_points;
        for (const auto& [segment_id, segment_info] : symbol_it->second) {
            auto points = read_segment(symbol, segment_id);
            all_points.insert(all_points.end(), points.begin(), points.end());
            
            // Remove old segment file
            std::filesystem::remove(segment_info.file_path);
        }
        
        if (all_points.empty()) {
            return;
        }
        
        // Sort points
        std::sort(all_points.begin(), all_points.end(),
                 [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
                     return a.timestamp < b.timestamp;
                 });
        
        // Clear existing segments
        symbol_it->second.clear();
        
        // Write new optimized segments
        const size_t points_per_segment = 10000;
        size_t current_segment = 0;
        
        for (size_t i = 0; i < all_points.size(); i += points_per_segment) {
            size_t end_idx = std::min(i + points_per_segment, all_points.size());
            std::vector<TimeSeriesPoint> segment_points(
                all_points.begin() + i,
                all_points.begin() + end_idx
            );
            
            write_segment(symbol, segment_points, current_segment++);
        }
    }
};

DiskLayer::DiskLayer(const std::filesystem::path& data_directory, const DiskConfig& config)
    : pimpl_(std::make_unique<Impl>(data_directory, config)) {}

DiskLayer::~DiskLayer() = default;

bool DiskLayer::write_batch(const std::vector<TimeSeriesPoint>& points) {
    if (points.empty()) return true;

    // Group points by symbol
    std::unordered_map<std::string, std::vector<TimeSeriesPoint>> grouped_points;
    for (const auto& point : points) {
        grouped_points[point.symbol].push_back(point);
    }

    // Write each group to its own segment
    for (const auto& [symbol, symbol_points] : grouped_points) {
        std::vector<TimeSeriesPoint> sorted_points = symbol_points;
        std::sort(sorted_points.begin(), sorted_points.end(),
                 [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
                     return a.timestamp < b.timestamp;
                 });

        // Get next segment ID
        std::shared_lock read_lock(pimpl_->mutex);
        size_t next_segment_id = 0;
        auto it = pimpl_->metadata.find(symbol);
        if (it != pimpl_->metadata.end() && !it->second.empty()) {
            next_segment_id = std::max_element(it->second.begin(), it->second.end(),
                [](const auto& a, const auto& b) { return a.first < b.first; })->first + 1;
        }
        read_lock.unlock();

        // Write the segment
        pimpl_->write_segment(symbol, sorted_points, next_segment_id);
    }

    return true;
}

bool DiskLayer::commit_segment(const std::string& symbol) {
    // No need for explicit commit in our implementation
    return true;
}

std::vector<TimeSeriesPoint> DiskLayer::read_range(
    const std::string& symbol,
    const std::chrono::system_clock::time_point& start,
    const std::chrono::system_clock::time_point& end) {
    
    return pimpl_->range_query(symbol, start, end);
}

void DiskLayer::compact_segments(const std::string& symbol) {
    pimpl_->optimize_segments(symbol);
}

void DiskLayer::optimize_index() {
    // Get symbols with a quick shared lock
    std::vector<std::string> symbols;
    {
        std::shared_lock lock(pimpl_->mutex);
        for (const auto& [symbol, _] : pimpl_->metadata) {
            symbols.push_back(symbol);
        }
    } // Release shared lock

    // Process each symbol independently
    for (const auto& symbol : symbols) {
        try {
            std::unique_lock lock(pimpl_->mutex);
            
            auto symbol_it = pimpl_->metadata.find(symbol);
            if (symbol_it == pimpl_->metadata.end()) {
                continue;
            }

            // Collect all points with existing lock
            std::vector<TimeSeriesPoint> all_points;
            for (const auto& [segment_id, segment_info] : symbol_it->second) {
                auto points = pimpl_->read_segment(symbol, segment_id);
                all_points.insert(all_points.end(), points.begin(), points.end());
            }

            if (all_points.empty()) {
                continue;
            }

            // Sort points
            std::sort(all_points.begin(), all_points.end(),
                     [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
                         return a.timestamp < b.timestamp;
                     });

            // Remove duplicates
            auto it = std::unique(all_points.begin(), all_points.end(),
                             [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
                                 return a.timestamp == b.timestamp;
                             });
            all_points.erase(it, all_points.end());

            // Remove old segment files
            for (const auto& [segment_id, segment_info] : symbol_it->second) {
                std::filesystem::remove(segment_info.file_path);
            }

            // Clear existing segments
            symbol_it->second.clear();

            // Write new optimized segments
            const size_t points_per_segment = 10000;
            size_t current_segment = 0;

            for (size_t i = 0; i < all_points.size(); i += points_per_segment) {
                size_t end_idx = std::min(i + points_per_segment, all_points.size());
                std::vector<TimeSeriesPoint> segment_points(
                    all_points.begin() + i,
                    all_points.begin() + end_idx
                );
                
                pimpl_->write_segment(symbol, segment_points, current_segment++);
            }
        } catch (const std::exception& e) {
            // Log error and continue with next symbol
            fprintf(stderr, "Error optimizing symbol %s: %s\n", symbol.c_str(), e.what());
            continue;
        }
    }
}

size_t DiskLayer::get_storage_size() const {
    size_t total_size = 0;
    std::shared_lock lock(pimpl_->mutex);
    
    for (const auto& [symbol, data] : pimpl_->metadata) {
        for (const auto& [segment_id, segment] : data) {
            std::error_code ec;
            total_size += std::filesystem::file_size(segment.file_path, ec);
        }
    }
    
    return total_size;
}

} // namespace findata_engine
