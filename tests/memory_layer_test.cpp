#include <gtest/gtest.h>
#include "findata_engine/memory_layer.hpp"
#include <chrono>
#include <thread>
#include <future>
#include <random>

using namespace findata_engine;
using namespace std::chrono;

class MemoryLayerTest : public ::testing::Test {
protected:
    void SetUp() override {
        layer_ = std::make_unique<MemoryLayer>(64); // 64MB cache
    }
    
    std::unique_ptr<MemoryLayer> layer_;
};

TEST_F(MemoryLayerTest, InsertAndRetrieve) {
    auto now = system_clock::now();
    TimeSeriesPoint point{
        .timestamp = now,
        .value = 100.5,
        .symbol = "AAPL"
    };
    
    EXPECT_TRUE(layer_->insert(point));
    
    auto result = layer_->get_latest("AAPL");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->timestamp, now);
    EXPECT_DOUBLE_EQ(result->value, 100.5);
    EXPECT_EQ(result->symbol, "AAPL");
}

TEST_F(MemoryLayerTest, BatchInsertAndQuery) {
    auto start_time = system_clock::now();
    const int num_points = 100; 
    std::vector<TimeSeriesPoint> points;
    
    // Generate test points
    for (int i = 0; i < num_points; ++i) {
        points.push_back(TimeSeriesPoint{
            .timestamp = start_time + microseconds(i * 1000),
            .value = static_cast<double>(i),
            .symbol = "AAPL"
        });
    }
    
    EXPECT_TRUE(layer_->insert_batch(points));
    
    auto mid_time = start_time + microseconds(50000);
    auto results = layer_->get_range("AAPL", start_time, mid_time);
    
    ASSERT_EQ(results.size(), 51); 
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i].timestamp, start_time + microseconds(i * 1000));
    }
}

TEST_F(MemoryLayerTest, ConcurrentAccess) {
    const int num_threads = 2; 
    const int points_per_thread = 100; 
    std::vector<std::future<void>> futures;
    auto start_time = system_clock::now();
    
    // Launch writer threads
    for (int t = 0; t < num_threads; ++t) {
        futures.push_back(std::async(std::launch::async, [this, t, start_time]() {
            std::string symbol = "SYM" + std::to_string(t);
            std::vector<TimeSeriesPoint> points;
            for (int i = 0; i < points_per_thread; ++i) {
                points.push_back(TimeSeriesPoint{
                    .timestamp = start_time + microseconds(i * 1000),
                    .value = static_cast<double>(i),
                    .symbol = symbol
                });
            }
            EXPECT_TRUE(layer_->insert_batch(points));
        }));
    }
    
    // Launch reader threads
    for (int t = 0; t < num_threads; ++t) {
        futures.push_back(std::async(std::launch::async, [this, t, start_time]() {
            std::string symbol = "SYM" + std::to_string(t);
            
            // Repeatedly query while writes are happening
            for (int i = 0; i < 5; ++i) { 
                auto results = layer_->get_range(
                    symbol,
                    start_time,
                    start_time + seconds(100)); 
                    
                EXPECT_TRUE(results.size() <= points_per_thread);
                std::this_thread::sleep_for(milliseconds(10));
            }
        }));
    }
    
    // Wait for all operations to complete with timeout
    auto timeout = seconds(10);
    auto deadline = system_clock::now() + timeout;
    
    for (auto& future : futures) {
        auto status = future.wait_until(deadline);
        ASSERT_EQ(status, std::future_status::ready) << "Test timed out after " << timeout.count() << " seconds";
        future.get();
    }
}

TEST_F(MemoryLayerTest, CacheManagement) {
    auto start_time = system_clock::now();
    const int num_batches = 3; 
    const int points_per_batch = 100; 
    
    // Insert data in batches
    for (int i = 0; i < num_batches; ++i) {
        std::vector<TimeSeriesPoint> points;
        for (int j = 0; j < points_per_batch; ++j) {
            points.push_back(TimeSeriesPoint{
                .timestamp = start_time + seconds(i * 60) + microseconds(j * 1000),
                .value = static_cast<double>(j),
                .symbol = "TSLA"
            });
        }
        EXPECT_TRUE(layer_->insert_batch(points));
    }
    
    // Query data to verify cache
    auto results = layer_->get_range(
        "TSLA",
        start_time,
        start_time + seconds(num_batches * 60));
        
    ASSERT_EQ(results.size(), num_batches * points_per_batch);
    
    // Verify points are sorted
    EXPECT_TRUE(std::is_sorted(
        results.begin(),
        results.end(),
        [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
            return a.timestamp < b.timestamp;
        }));
}
