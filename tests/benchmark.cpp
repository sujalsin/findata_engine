#include "findata_engine/storage_engine.hpp"
#include "findata_engine/rust_bindings.hpp"
#include <gtest/gtest.h>
#include <random>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <vector>
#include <cmath>

using namespace findata_engine;
using Clock = std::chrono::high_resolution_clock;

class BenchmarkTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Set up random number generator
        std::random_device rd;
        gen = std::mt19937(rd());
        price_dist = std::normal_distribution<double>(100.0, 10.0);
    }

    std::vector<TimeSeriesPoint> generate_random_data(size_t n, const std::string& symbol) {
        std::vector<TimeSeriesPoint> points;
        points.reserve(n);
        
        auto now = std::chrono::system_clock::now();
        auto timestamp = now - std::chrono::hours(n);
        
        for (size_t i = 0; i < n; ++i) {
            points.push_back(TimeSeriesPoint{
                .symbol = symbol,
                .timestamp = timestamp + std::chrono::seconds(i),
                .value = price_dist(gen)
            });
        }
        
        return points;
    }

    std::vector<double> extract_values(const std::vector<TimeSeriesPoint>& points) {
        std::vector<double> values;
        values.reserve(points.size());
        for (const auto& p : points) {
            values.push_back(p.value);
        }
        return values;
    }

    void print_benchmark_header() {
        std::cout << std::setw(30) << std::left << "Test Name"
                  << std::setw(15) << std::right << "Time (ms)"
                  << std::setw(15) << "Memory (KB)"
                  << std::setw(20) << "Throughput (ops/s)"
                  << std::endl;
        std::cout << std::string(80, '-') << std::endl;
    }

    void print_benchmark_result(const std::string& name, 
                              std::chrono::microseconds duration,
                              size_t memory_bytes,
                              size_t operations) {
        double ms = duration.count() / 1000.0;
        double throughput = operations / (duration.count() / 1e6);
        
        std::cout << std::setw(30) << std::left << name
                  << std::setw(15) << std::right << std::fixed << std::setprecision(2) << ms
                  << std::setw(15) << (memory_bytes / 1024)
                  << std::setw(20) << std::setprecision(0) << throughput
                  << std::endl;
    }

    std::mt19937 gen;
    std::normal_distribution<double> price_dist;
};

TEST_F(BenchmarkTest, CompressionRatioBenchmark) {
    std::cout << "\nCompression Ratio Benchmark\n" << std::string(80, '=') << std::endl;
    
    const std::vector<size_t> sizes = {1000, 10000, 100000};
    
    for (size_t n : sizes) {
        auto points = generate_random_data(n, "AAPL");
        
        // Convert to Rust TimePoints
        std::vector<TimePoint> rust_points;
        rust_points.reserve(points.size());
        for (const auto& p : points) {
            rust_points.push_back(TimePoint{
                .timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                    p.timestamp.time_since_epoch()).count(),
                .value = p.value
            });
        }
        
        // Measure original size
        size_t original_size = rust_points.size() * sizeof(TimePoint);
        
        // Compress
        size_t compressed_size;
        auto start = Clock::now();
        uint8_t* compressed = compress_time_series(
            rust_points.data(),
            rust_points.size(),
            &compressed_size
        );
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - start);
        
        // Calculate ratio
        double compression_ratio = static_cast<double>(original_size) / compressed_size;
        
        std::cout << "Points: " << std::setw(8) << n
                  << " | Original: " << std::setw(8) << original_size
                  << " | Compressed: " << std::setw(8) << compressed_size
                  << " | Ratio: " << std::fixed << std::setprecision(2) << compression_ratio
                  << "x | Time: " << duration.count() / 1000.0 << "ms"
                  << std::endl;
        
        free_compressed_data(compressed, compressed_size);
    }
}

TEST_F(BenchmarkTest, SIMDOperationsBenchmark) {
    std::cout << "\nSIMD Operations Benchmark\n" << std::string(80, '=') << std::endl;
    print_benchmark_header();
    
    const size_t n = 1000000;
    const size_t window = 20;
    const double alpha = 0.1;
    
    auto points = generate_random_data(n, "AAPL");
    auto values = extract_values(points);
    std::vector<double> output(n);
    
    // Benchmark Simple Moving Average
    {
        auto start = Clock::now();
        compute_moving_average_simd(values.data(), n, window, output.data());
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - start);
        print_benchmark_result(
            "Moving Average", duration,
            n * sizeof(double) * 2,  // Input + output arrays
            n - window + 1  // Number of averages computed
        );
    }
    
    // Benchmark Exponential Moving Average
    {
        auto start = Clock::now();
        compute_exponential_moving_average_simd(values.data(), n, alpha, output.data());
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - start);
        print_benchmark_result(
            "Exponential Moving Average", duration,
            n * sizeof(double) * 2,
            n - 1  // Number of EMAs computed
        );
    }
    
    // Benchmark Standard Deviation
    {
        auto start = Clock::now();
        compute_standard_deviation_simd(values.data(), n, window, output.data());
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - start);
        print_benchmark_result(
            "Standard Deviation", duration,
            n * sizeof(double) * 2,
            n - window + 1
        );
    }
}

TEST_F(BenchmarkTest, StorageEngineBenchmark) {
    std::cout << "\nStorage Engine Benchmark\n" << std::string(80, '=') << std::endl;
    print_benchmark_header();
    
    const size_t n = 100000;
    const std::vector<std::string> symbols = {"AAPL", "GOOGL", "MSFT", "AMZN"};
    
    // Create temporary directory for testing
    auto temp_dir = std::filesystem::temp_directory_path() / "findata_benchmark";
    std::filesystem::create_directories(temp_dir);
    
    EngineConfig config{
        .memory_cache_size_mb = 64,
        .data_directory = temp_dir,
        .enable_compression = true,
        .batch_size = 1000,
        .max_segment_size_mb = 16
    };
    
    StorageEngine engine(config);
    
    // Benchmark write performance
    {
        auto start = Clock::now();
        for (const auto& symbol : symbols) {
            auto points = generate_random_data(n, symbol);
            engine.write_batch(points);
        }
        engine.flush();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - start);
        
        print_benchmark_result(
            "Write Performance", duration,
            n * symbols.size() * sizeof(TimeSeriesPoint),
            n * symbols.size()
        );
    }
    
    // Benchmark read performance
    {
        auto start_time = std::chrono::system_clock::now() - std::chrono::hours(n);
        auto end_time = std::chrono::system_clock::now();
        
        auto start = Clock::now();
        for (const auto& symbol : symbols) {
            auto points = engine.read_range(symbol, start_time, end_time);
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - start);
        
        print_benchmark_result(
            "Read Performance", duration,
            n * symbols.size() * sizeof(TimeSeriesPoint),
            n * symbols.size()
        );
    }
    
    // Clean up
    std::filesystem::remove_all(temp_dir);
}
