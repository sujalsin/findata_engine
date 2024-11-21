#include <gtest/gtest.h>
#include "findata_engine/disk_layer.hpp"
#include <filesystem>
#include <chrono>
#include <thread>
#include <future>
#include <random>

using namespace findata_engine;
using namespace std::chrono;
namespace fs = std::filesystem;

class DiskLayerTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() / "findata_test";
        fs::create_directories(test_dir_);
        disk_layer_ = std::make_unique<DiskLayer>(test_dir_);
    }
    
    void TearDown() override {
        disk_layer_.reset();
        fs::remove_all(test_dir_);
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
    
    std::unique_ptr<DiskLayer> disk_layer_;
    fs::path test_dir_;
};

TEST_F(DiskLayerTest, InsertAndRetrieve) {
    auto now = system_clock::now();
    TimeSeriesPoint point{
        .timestamp = now,
        .value = 100.5,
        .symbol = "AAPL"
    };
    
    std::vector<TimeSeriesPoint> points = {point};
    EXPECT_TRUE(disk_layer_->write_batch(points));
    
    // Commit to ensure data is written to disk
    EXPECT_TRUE(disk_layer_->commit_segment("AAPL"));
    
    auto results = disk_layer_->read_range("AAPL", now - hours(1), now + hours(1));
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].timestamp, now);
    EXPECT_DOUBLE_EQ(results[0].value, 100.5);
    EXPECT_EQ(results[0].symbol, "AAPL");
}

TEST_F(DiskLayerTest, BatchWriteAndRead) {
    auto start_time = system_clock::now();
    const int num_points = 100; 
    auto points = generate_test_data("AAPL", num_points, start_time, microseconds(1000));
    
    EXPECT_TRUE(disk_layer_->write_batch(points));
    EXPECT_TRUE(disk_layer_->commit_segment("AAPL"));
    
    auto mid_time = start_time + microseconds(50000);
    auto results = disk_layer_->read_range("AAPL", start_time, mid_time);
    
    ASSERT_EQ(results.size(), 51); 
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i].timestamp, start_time + microseconds(i * 1000));
    }
}

TEST_F(DiskLayerTest, SegmentManagement) {
    auto start_time = system_clock::now();
    const int num_segments = 3; 
    const int points_per_segment = 100; 
    
    // Write multiple segments
    for (int i = 0; i < num_segments; ++i) {
        auto points = generate_test_data(
            "GOOG",
            points_per_segment,
            start_time + seconds(i * 60),
            microseconds(1000));
            
        EXPECT_TRUE(disk_layer_->write_batch(points));
        EXPECT_TRUE(disk_layer_->commit_segment("GOOG"));
    }
    
    // Read all data
    auto results = disk_layer_->read_range(
        "GOOG",
        start_time,
        start_time + seconds(num_segments * 60));
        
    ASSERT_EQ(results.size(), num_segments * points_per_segment);
    
    // Verify points are sorted
    EXPECT_TRUE(std::is_sorted(
        results.begin(),
        results.end(),
        [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
            return a.timestamp < b.timestamp;
        }));
}

TEST_F(DiskLayerTest, Compaction) {
    auto start_time = system_clock::now();
    const int num_segments = 3;
    const int points_per_segment = 100;
    
    // Write overlapping segments
    for (int i = 0; i < num_segments; ++i) {
        auto points = generate_test_data(
            "AMZN",
            points_per_segment,
            start_time,
            microseconds(2000)); // Overlapping points
            
        EXPECT_TRUE(disk_layer_->write_batch(points));
        EXPECT_TRUE(disk_layer_->commit_segment("AMZN"));
    }
    
    // Compact segments
    disk_layer_->compact_segments("AMZN");
    
    // Read data
    auto results = disk_layer_->read_range(
        "AMZN",
        start_time,
        start_time + seconds(100));
    
    // After compaction, we should have no duplicates
    // Since we wrote overlapping points (2000us interval), we should have half the points
    EXPECT_EQ(results.size(), points_per_segment);
    
    // Verify points are sorted and unique
    EXPECT_TRUE(std::is_sorted(
        results.begin(),
        results.end(),
        [](const TimeSeriesPoint& a, const TimeSeriesPoint& b) {
            return a.timestamp < b.timestamp;
        }));
        
    // Verify no duplicates
    for (size_t i = 1; i < results.size(); ++i) {
        EXPECT_LT(results[i-1].timestamp, results[i].timestamp);
    }
}

TEST_F(DiskLayerTest, Persistence) {
    auto start_time = system_clock::now();
    const int num_points = 100; 
    auto points = generate_test_data("FB", num_points, start_time, microseconds(1000));
    
    // Write data
    EXPECT_TRUE(disk_layer_->write_batch(points));
    EXPECT_TRUE(disk_layer_->commit_segment("FB"));
    
    // Create new disk layer instance
    DiskLayer new_layer(test_dir_);
    
    // Read data from new instance
    auto results = new_layer.read_range(
        "FB",
        start_time,
        start_time + seconds(100)); 
    
    ASSERT_EQ(results.size(), num_points);
    
    // Verify data consistency
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i].timestamp, start_time + microseconds(i * 1000));
        EXPECT_EQ(results[i].symbol, "FB");
    }
}
