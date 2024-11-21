#pragma once

#include <cstddef>
#include <cstdint>

extern "C" {

struct TimePoint {
    int64_t timestamp;  // microseconds since epoch
    double value;
};

struct CompressedData {
    uint8_t* data;
    size_t size;
};

// Compression functions
uint8_t* compress_time_series(const TimePoint* points, size_t len, size_t* out_size);
TimePoint* decompress_time_series(const uint8_t* data, size_t size, size_t* out_len);
void free_compressed_data(uint8_t* data, size_t size);
void free_time_points(TimePoint* points, size_t len);

// SIMD operations
int compute_moving_average_simd(const double* values, size_t len, size_t window, double* out);
int compute_exponential_moving_average_simd(const double* values, size_t len, double alpha, double* out);
int compute_standard_deviation_simd(const double* values, size_t len, size_t window, double* out);

} // extern "C"
