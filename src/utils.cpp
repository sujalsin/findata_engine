#include "findata_engine/utils.hpp"
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <immintrin.h>
#include <cstring>
#include <stdexcept>

namespace findata_engine {
namespace utils {

// SIMD-optimized compression for numerical data
std::vector<uint8_t> compress_doubles(std::span<const double> data) {
    if (data.empty()) return {};
    
    const size_t num_doubles = data.size();
    std::vector<uint8_t> compressed;
    compressed.reserve(num_doubles * sizeof(double));

    // Write size first
    compressed.insert(compressed.end(), 
        reinterpret_cast<const uint8_t*>(&num_doubles),
        reinterpret_cast<const uint8_t*>(&num_doubles) + sizeof(num_doubles));

    // Use delta encoding with SIMD operations
    alignas(32) double prev_values[4] = {0.0, 0.0, 0.0, 0.0};
    const size_t vec_size = 4;
    const size_t num_vectors = num_doubles / vec_size;
    
    for (size_t i = 0; i < num_vectors; ++i) {
        __m256d curr = _mm256_loadu_pd(data.data() + i * vec_size);
        __m256d prev = _mm256_load_pd(prev_values);
        __m256d delta = _mm256_sub_pd(curr, prev);
        
        // Store deltas
        alignas(32) double deltas[4];
        _mm256_store_pd(deltas, delta);
        
        // Store raw bytes of deltas
        for (size_t j = 0; j < vec_size; ++j) {
            compressed.insert(compressed.end(),
                reinterpret_cast<const uint8_t*>(&deltas[j]),
                reinterpret_cast<const uint8_t*>(&deltas[j]) + sizeof(double));
        }
        
        _mm256_store_pd(prev_values, curr);
    }
    
    // Handle remaining elements
    for (size_t i = num_vectors * vec_size; i < num_doubles; ++i) {
        double delta = data[i] - prev_values[0];
        prev_values[0] = data[i];
        
        compressed.insert(compressed.end(),
            reinterpret_cast<const uint8_t*>(&delta),
            reinterpret_cast<const uint8_t*>(&delta) + sizeof(double));
    }
    
    return compressed;
}

std::vector<double> decompress_doubles(const std::vector<uint8_t>& compressed_data) {
    if (compressed_data.empty()) return {};
    
    // Read size
    size_t num_doubles;
    std::memcpy(&num_doubles, compressed_data.data(), sizeof(num_doubles));
    
    std::vector<double> decompressed;
    decompressed.reserve(num_doubles);
    
    const uint8_t* ptr = compressed_data.data() + sizeof(num_doubles);
    double prev_value = 0.0;
    
    for (size_t i = 0; i < num_doubles; ++i) {
        double delta;
        std::memcpy(&delta, ptr, sizeof(delta));
        ptr += sizeof(delta);
        
        double value = prev_value + delta;
        decompressed.push_back(value);
        prev_value = value;
    }
    
    return decompressed;
}

// Memory-mapped file implementation
MemoryMappedFile::MemoryMappedFile(const std::string& path, size_t size)
    : size_(size), data_(nullptr), fd_(-1) {
    fd_ = open(path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd_ == -1) {
        throw std::runtime_error("Failed to open file: " + path);
    }
    
    // Set file size
    if (ftruncate(fd_, size) == -1) {
        close(fd_);
        throw std::runtime_error("Failed to set file size");
    }
    
    // Map file into memory
    data_ = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (data_ == MAP_FAILED) {
        close(fd_);
        throw std::runtime_error("Failed to map file into memory");
    }
}

MemoryMappedFile::~MemoryMappedFile() {
    if (data_ != nullptr) {
        munmap(data_, size_);
    }
    if (fd_ != -1) {
        close(fd_);
    }
}

void MemoryMappedFile::flush() {
    if (data_ != nullptr) {
        msync(data_, size_, MS_SYNC);
    }
}

void MemoryMappedFile::resize(size_t new_size) {
    if (new_size == size_) return;
    
    // Unmap current memory
    if (data_ != nullptr) {
        munmap(data_, size_);
    }
    
    // Resize file
    if (ftruncate(fd_, new_size) == -1) {
        throw std::runtime_error("Failed to resize file");
    }
    
    // Remap with new size
    data_ = mmap(nullptr, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (data_ == MAP_FAILED) {
        throw std::runtime_error("Failed to remap file");
    }
    
    size_ = new_size;
}

// Time-series compression implementation
std::vector<uint8_t> compress_time_series(const std::vector<TimeSeriesPoint>& points) {
    if (points.empty()) return {};

    // Extract timestamps and values
    std::vector<int64_t> timestamp_deltas;
    std::vector<double> values;
    std::string prev_symbol;
    std::vector<size_t> symbol_indices;
    std::vector<std::string> unique_symbols;

    timestamp_deltas.reserve(points.size());
    values.reserve(points.size());
    symbol_indices.reserve(points.size());

    // Compute deltas for timestamps
    auto prev_time = points[0].timestamp;
    timestamp_deltas.push_back(prev_time.time_since_epoch().count());
    values.push_back(points[0].value);

    // Handle first symbol
    symbol_indices.push_back(0);
    unique_symbols.push_back(points[0].symbol);
    prev_symbol = points[0].symbol;

    for (size_t i = 1; i < points.size(); ++i) {
        auto delta = points[i].timestamp - prev_time;
        timestamp_deltas.push_back(delta.count());
        values.push_back(points[i].value);
        prev_time = points[i].timestamp;

        // Handle symbols
        if (points[i].symbol != prev_symbol) {
            auto it = std::find(unique_symbols.begin(), unique_symbols.end(), points[i].symbol);
            if (it == unique_symbols.end()) {
                symbol_indices.push_back(unique_symbols.size());
                unique_symbols.push_back(points[i].symbol);
            } else {
                symbol_indices.push_back(std::distance(unique_symbols.begin(), it));
            }
            prev_symbol = points[i].symbol;
        } else {
            symbol_indices.push_back(symbol_indices.back());
        }
    }

    // Compress components
    auto compressed_timestamps = compress_doubles(std::span<const double>(
        reinterpret_cast<const double*>(timestamp_deltas.data()),
        timestamp_deltas.size()));
    
    auto compressed_values = compress_doubles(std::span<const double>(values.data(), values.size()));

    // Serialize to final format
    std::vector<uint8_t> result;
    
    // Write sizes
    auto write_size = [&result](size_t size) {
        result.insert(result.end(), 
            reinterpret_cast<uint8_t*>(&size),
            reinterpret_cast<uint8_t*>(&size) + sizeof(size));
    };

    write_size(points.size());
    write_size(unique_symbols.size());
    write_size(compressed_timestamps.size());
    write_size(compressed_values.size());

    // Write symbols
    for (const auto& symbol : unique_symbols) {
        write_size(symbol.size());
        result.insert(result.end(), symbol.begin(), symbol.end());
    }

    // Write symbol indices
    result.insert(result.end(), 
        reinterpret_cast<uint8_t*>(symbol_indices.data()),
        reinterpret_cast<uint8_t*>(symbol_indices.data() + symbol_indices.size()));

    // Write compressed data
    result.insert(result.end(), compressed_timestamps.begin(), compressed_timestamps.end());
    result.insert(result.end(), compressed_values.begin(), compressed_values.end());

    return result;
}

std::vector<TimeSeriesPoint> decompress_time_series(const std::vector<uint8_t>& compressed) {
    if (compressed.empty()) return {};

    const uint8_t* ptr = compressed.data();
    
    // Read sizes
    auto read_size = [&ptr]() {
        size_t size;
        std::memcpy(&size, ptr, sizeof(size));
        ptr += sizeof(size);
        return size;
    };

    const size_t num_points = read_size();
    const size_t num_symbols = read_size();
    const size_t timestamps_size = read_size();
    const size_t values_size = read_size();

    // Read symbols
    std::vector<std::string> symbols;
    symbols.reserve(num_symbols);
    
    for (size_t i = 0; i < num_symbols; ++i) {
        size_t symbol_size = read_size();
        symbols.emplace_back(reinterpret_cast<const char*>(ptr), symbol_size);
        ptr += symbol_size;
    }

    // Read symbol indices
    std::vector<size_t> symbol_indices(num_points);
    std::memcpy(symbol_indices.data(), ptr, num_points * sizeof(size_t));
    ptr += num_points * sizeof(size_t);

    // Read compressed data
    std::vector<uint8_t> compressed_timestamps(ptr, ptr + timestamps_size);
    ptr += timestamps_size;
    
    std::vector<uint8_t> compressed_values(ptr, ptr + values_size);
    ptr += values_size;

    // Decompress timestamps
    auto timestamp_deltas_double = decompress_doubles(compressed_timestamps);
    std::vector<int64_t> timestamp_deltas(timestamp_deltas_double.size());
    for (size_t i = 0; i < timestamp_deltas_double.size(); ++i) {
        timestamp_deltas[i] = static_cast<int64_t>(timestamp_deltas_double[i]);
    }

    // Reconstruct timestamps
    std::vector<std::chrono::system_clock::time_point> timestamps;
    timestamps.reserve(num_points);
    
    auto base_time = std::chrono::system_clock::time_point(
        std::chrono::system_clock::duration(timestamp_deltas[0]));
    timestamps.push_back(base_time);
    
    for (size_t i = 1; i < timestamp_deltas.size(); ++i) {
        base_time += std::chrono::system_clock::duration(timestamp_deltas[i]);
        timestamps.push_back(base_time);
    }

    // Decompress values
    auto values = decompress_doubles(compressed_values);

    // Reconstruct points
    std::vector<TimeSeriesPoint> points;
    points.reserve(num_points);
    
    for (size_t i = 0; i < num_points; ++i) {
        points.push_back(TimeSeriesPoint{
            .timestamp = timestamps[i],
            .value = values[i],
            .symbol = symbols[symbol_indices[i]]
        });
    }

    return points;
}

} // namespace utils
} // namespace findata_engine
