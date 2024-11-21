#include <gtest/gtest.h>
#include "findata_engine/utils.hpp"
#include "findata_engine/types.hpp"
#include <vector>
#include <string>
#include <fstream>
#include <future>
#include <chrono>

using namespace findata_engine;
using namespace findata_engine::utils;
using namespace std::chrono;

TEST(UtilsTest, LRUCacheTest) {
    LRUCache<std::string, std::vector<TimeSeriesPoint>, std::hash<std::string>> cache(1024); // 1KB cache
    
    std::vector<TimeSeriesPoint> points = {
        TimeSeriesPoint{
            .timestamp = system_clock::now(),
            .value = 100.5,
            .symbol = "AAPL"
        },
        TimeSeriesPoint{
            .timestamp = system_clock::now() + seconds(1),
            .value = 101.0,
            .symbol = "AAPL"
        }
    };
    
    cache.put("AAPL", points);
    auto result = cache.get("AAPL");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 2);
    EXPECT_EQ((*result)[0].value, 100.5);
    EXPECT_EQ((*result)[1].value, 101.0);
    
    // Test eviction
    std::vector<TimeSeriesPoint> large_points;
    for (int i = 0; i < 1000; ++i) {
        large_points.push_back(TimeSeriesPoint{
            .timestamp = system_clock::now() + seconds(i),
            .value = static_cast<double>(i),
            .symbol = "MSFT"
        });
    }
    
    cache.put("MSFT", large_points); // Should evict AAPL
    EXPECT_FALSE(cache.get("AAPL").has_value());
    EXPECT_TRUE(cache.get("MSFT").has_value());
}

TEST(UtilsTest, CompressionTest) {
    std::vector<double> values;
    for (int i = 0; i < 1000; ++i) {
        values.push_back(static_cast<double>(i) + 0.5);
    }
    
    std::vector<uint8_t> compressed = compress_doubles(values);
    std::vector<double> decompressed = decompress_doubles(compressed);
    
    EXPECT_EQ(values.size(), decompressed.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_DOUBLE_EQ(values[i], decompressed[i]);
    }
}

TEST(UtilsTest, MemoryMappedFileTest) {
    std::string filename = "test_mmap.dat";
    const size_t file_size = 1024 * 1024; // 1MB
    
    // Create a test file
    {
        std::ofstream ofs(filename, std::ios::binary);
        std::vector<char> data(file_size, 'A');
        ofs.write(data.data(), data.size());
    }
    
    // Test memory mapping
    MemoryMappedFile mmap(filename, file_size);
    EXPECT_TRUE(mmap.data() != nullptr);
    EXPECT_EQ(mmap.size(), file_size);
    
    // Write some data
    char* data = static_cast<char*>(mmap.data());
    std::memcpy(data, "Hello", 5);
    
    // Clean up
    mmap = MemoryMappedFile("", 0); // Release mapping
    std::remove(filename.c_str());
}

TEST(UtilsTest, TimeSeriesCompressionTest) {
    std::vector<TimeSeriesPoint> points;
    auto start_time = system_clock::now();
    
    // Generate test data
    for (int i = 0; i < 1000; ++i) {
        points.push_back(TimeSeriesPoint{
            .timestamp = start_time + seconds(i),
            .value = static_cast<double>(i) + 0.5,
            .symbol = "TEST"
        });
    }
    
    // Compress points
    std::vector<uint8_t> compressed = compress_time_series(points);
    
    // Decompress points
    std::vector<TimeSeriesPoint> decompressed = decompress_time_series(compressed);
    
    // Verify
    EXPECT_EQ(points.size(), decompressed.size());
    for (size_t i = 0; i < points.size(); ++i) {
        EXPECT_EQ(points[i].timestamp, decompressed[i].timestamp);
        EXPECT_DOUBLE_EQ(points[i].value, decompressed[i].value);
        EXPECT_EQ(points[i].symbol, decompressed[i].symbol);
    }
}
