#include <gtest/gtest.h>
#include "findata_engine/storage_engine.hpp"
#include <filesystem>
#include <random>
#include <chrono>
#include <future>
#include <thread>

using namespace findata_engine;
using namespace std::chrono;
namespace fs = std::filesystem;

class StorageEngineTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary test directory
        test_dir_ = std::filesystem::temp_directory_path() / "findata_test";
        std::filesystem::create_directories(test_dir_);
        
        // Configure engine
        EngineConfig config{
            .memory_cache_size_mb = 64,
            .data_directory = test_dir_,
            .enable_compression = true,
            .batch_size = 1000,
            .max_segment_size_mb = 16
        };
        
        engine_ = std::make_unique<StorageEngine>(config);
    }
    
    void TearDown() override {
        engine_.reset();
        std::filesystem::remove_all(test_dir_);
    }
    
    std::vector<TimeSeriesPoint> generate_test_data(
        const std::string& symbol,
        size_t count,
        system_clock::time_point start_time,
        microseconds interval) {
        
        std::vector<TimeSeriesPoint> points;
        points.reserve(count);
        
        std::random_device rd;
        std::mt19937 gen(rd());
        std::normal_distribution<double> dist(100.0, 10.0);
        
        auto current_time = start_time;
        for (size_t i = 0; i < count; ++i) {
            points.push_back(TimeSeriesPoint{
                .timestamp = current_time,
                .value = dist(gen),
                .symbol = symbol
            });
            current_time += interval;
        }
        return points;
    }
    
    std::unique_ptr<StorageEngine> engine_;
    fs::path test_dir_;
};

TEST_F(StorageEngineTest, InsertAndQuerySinglePoint) {
    auto now = system_clock::now();
    TimeSeriesPoint point{
        .timestamp = now,
        .value = 100.5,
        .symbol = "AAPL"
    };
    
    EXPECT_TRUE(engine_->write_point(point));
    
    auto latest = engine_->get_latest("AAPL");
    ASSERT_TRUE(latest.has_value());
    EXPECT_EQ(latest->timestamp, now);
    EXPECT_DOUBLE_EQ(latest->value, 100.5);
    EXPECT_EQ(latest->symbol, "AAPL");
}

TEST_F(StorageEngineTest, BatchInsertAndRangeQuery) {
    auto start_time = system_clock::now();
    auto points = generate_test_data("MSFT", 100, start_time, microseconds(1000));
    
    EXPECT_TRUE(engine_->write_batch(points));
    
    auto mid_time = start_time + microseconds(50000);
    auto results = engine_->read_range("MSFT", start_time, mid_time);
    
    ASSERT_EQ(results.size(), 51); 
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i].timestamp, start_time + microseconds(i * 1000));
    }
}

TEST_F(StorageEngineTest, MemoryToDiscFlush) {
    auto start_time = system_clock::now();
    const int batches = 3;
    const int points_per_batch = 100;
    
    // Insert enough data to trigger flush
    for (int i = 0; i < batches; ++i) {
        auto points = generate_test_data(
            "GOOG",
            points_per_batch,
            start_time + seconds(i * 60),
            microseconds(1000));
            
        EXPECT_TRUE(engine_->write_batch(points));
    }
    
    // Force flush
    engine_->flush();
    
    // Query all data
    auto results = engine_->read_range(
        "GOOG",
        start_time,
        start_time + seconds(batches * 60));
    
    ASSERT_EQ(results.size(), batches * points_per_batch);
    
    // Verify points are sorted
    EXPECT_TRUE(std::is_sorted(
        results.begin(),
        results.end(),
        [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
            return a.timestamp < b.timestamp;
        }));
}

TEST_F(StorageEngineTest, MultiSymbolTest) {
    auto start_time = system_clock::now();
    std::vector<std::string> symbols = {"AAPL", "MSFT", "GOOG", "AMZN"};
    const int points_per_symbol = 100;
    
    // Insert data for multiple symbols
    for (const auto& symbol : symbols) {
        auto points = generate_test_data(
            symbol,
            points_per_symbol,
            start_time,
            microseconds(1000));
            
        EXPECT_TRUE(engine_->write_batch(points));
    }
    
    // Query each symbol
    for (const auto& symbol : symbols) {
        auto results = engine_->read_range(
            symbol,
            start_time,
            start_time + seconds(100));
            
        ASSERT_EQ(results.size(), points_per_symbol);
        
        // Verify symbol consistency
        for (const auto& point : results) {
            EXPECT_EQ(point.symbol, symbol);
        }
    }
}

TEST_F(StorageEngineTest, ConcurrentAccess) {
    const int num_threads = 2;
    const int points_per_thread = 100;
    std::vector<std::future<void>> futures;
    auto start_time = system_clock::now();
    
    // Launch writer threads
    for (int t = 0; t < num_threads; ++t) {
        futures.push_back(std::async(std::launch::async, [this, t, start_time]() {
            std::string symbol = "SYM" + std::to_string(t);
            auto points = generate_test_data(
                symbol,
                points_per_thread,
                start_time,
                microseconds(1000));
                
            EXPECT_TRUE(engine_->write_batch(points));
        }));
    }
    
    // Launch reader threads
    for (int t = 0; t < num_threads; ++t) {
        futures.push_back(std::async(std::launch::async, [this, t, start_time]() {
            std::string symbol = "SYM" + std::to_string(t);
            
            // Repeatedly query while writes are happening
            for (int i = 0; i < 5; ++i) {
                auto results = engine_->read_range(
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

TEST_F(StorageEngineTest, OptimizationAndCompaction) {
    std::cout << "Starting OptimizationAndCompaction test..." << std::endl;
    
    auto start_time = system_clock::now();
    const int points_per_batch = 10; 
    
    // First batch: Insert points with timestamps 0, 1000, 2000, ...
    std::vector<TimeSeriesPoint> first_batch;
    for (int i = 0; i < points_per_batch; ++i) {
        first_batch.push_back(TimeSeriesPoint{
            .timestamp = start_time + microseconds(i * 1000),
            .value = static_cast<double>(i),
            .symbol = "FB"
        });
    }
    std::cout << "Inserting first batch..." << std::endl;
    EXPECT_TRUE(engine_->write_batch(first_batch));
    std::cout << "Flushing first batch..." << std::endl;
    engine_->flush();
    
    // Sleep briefly to ensure timestamps are different
    std::this_thread::sleep_for(milliseconds(100));
    
    // Second batch: Insert points with different values
    std::vector<TimeSeriesPoint> second_batch;
    for (int i = 0; i < points_per_batch; ++i) {
        second_batch.push_back(TimeSeriesPoint{
            .timestamp = start_time + microseconds(i * 1000),
            .value = static_cast<double>(100 + i), 
            .symbol = "FB"
        });
    }
    std::cout << "Inserting second batch..." << std::endl;
    EXPECT_TRUE(engine_->write_batch(second_batch));
    std::cout << "Flushing second batch..." << std::endl;
    engine_->flush();
    
    std::cout << "Querying initial data..." << std::endl;
    auto initial_results = engine_->read_range(
        "FB",
        start_time,
        start_time + seconds(10));
    
    std::cout << "Initial results size: " << initial_results.size() << std::endl;
    
    std::cout << "Running optimize..." << std::endl;
    engine_->optimize();
    
    std::cout << "Querying final data..." << std::endl;
    auto final_results = engine_->read_range(
        "FB",
        start_time,
        start_time + seconds(10));
    
    std::cout << "Final results size: " << final_results.size() << std::endl;
    
    // After optimization, we should have only one point per timestamp
    EXPECT_EQ(final_results.size(), points_per_batch);
    
    // Verify points are sorted
    EXPECT_TRUE(std::is_sorted(
        final_results.begin(),
        final_results.end(),
        [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
            return a.timestamp < b.timestamp;
        }));
    
    // Verify we kept the latest values (from second batch)
    for (const auto& point : final_results) {
        EXPECT_GE(point.value, 100.0); 
    }
    
    std::cout << "Test completed successfully." << std::endl;
}
