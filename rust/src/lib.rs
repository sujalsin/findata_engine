use chrono::{DateTime, TimeZone, Utc};
use libc::{c_double, c_int, size_t};
use std::slice;

#[repr(C)]
pub struct TimePoint {
    timestamp: i64,  // microseconds since epoch
    value: f64,
}

#[repr(C)]
pub struct CompressedData {
    data: *mut u8,
    size: size_t,
}

#[no_mangle]
pub extern "C" fn compress_time_series(
    points: *const TimePoint,
    len: size_t,
    out_size: *mut size_t,
) -> *mut u8 {
    let points = unsafe { slice::from_raw_parts(points, len) };
    
    // Convert points to bytes
    let bytes: Vec<u8> = points
        .iter()
        .flat_map(|p| {
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&p.timestamp.to_le_bytes());
            bytes.extend_from_slice(&p.value.to_le_bytes());
            bytes
        })
        .collect();
    
    // Compress using zstd
    let compressed = zstd::encode_all(&bytes[..], 3).unwrap();
    
    // Set output size
    unsafe {
        *out_size = compressed.len();
    }
    
    // Convert to raw pointer and forget to prevent deallocation
    let ptr = compressed.as_ptr() as *mut u8;
    std::mem::forget(compressed);
    ptr
}

#[no_mangle]
pub extern "C" fn decompress_time_series(
    data: *const u8,
    size: size_t,
    out_len: *mut size_t,
) -> *mut TimePoint {
    let compressed = unsafe { slice::from_raw_parts(data, size) };
    
    // Decompress data
    let decompressed = zstd::decode_all(compressed).unwrap();
    
    // Convert bytes back to points
    let mut points = Vec::with_capacity(decompressed.len() / 16);
    let mut i = 0;
    while i < decompressed.len() {
        let timestamp = i64::from_le_bytes(decompressed[i..i+8].try_into().unwrap());
        let value = f64::from_le_bytes(decompressed[i+8..i+16].try_into().unwrap());
        points.push(TimePoint { timestamp, value });
        i += 16;
    }
    
    // Set output length
    unsafe {
        *out_len = points.len();
    }
    
    // Convert to raw pointer and forget to prevent deallocation
    let ptr = points.as_ptr() as *mut TimePoint;
    std::mem::forget(points);
    ptr
}

#[no_mangle]
pub extern "C" fn free_compressed_data(data: *mut u8, size: size_t) {
    unsafe {
        let _ = Vec::from_raw_parts(data, size, size);
    }
}

#[no_mangle]
pub extern "C" fn free_time_points(points: *mut TimePoint, len: size_t) {
    unsafe {
        let _ = Vec::from_raw_parts(points, len, len);
    }
}

// SIMD-accelerated operations for time series
#[cfg(target_arch = "x86_64")]
pub mod simd {
    use std::arch::x86_64::*;
    
    #[no_mangle]
    pub extern "C" fn compute_moving_average_simd(
        values: *const f64,
        len: usize,
        window: usize,
        out: *mut f64,
    ) -> i32 {
        if window == 0 || len < window {
            return -1;
        }
        
        let values = unsafe { std::slice::from_raw_parts(values, len) };
        let out = unsafe { std::slice::from_raw_parts_mut(out, len) };
        
        // Compute first window sum
        let mut sum = values[..window].iter().sum::<f64>();
        out[window-1] = sum / window as f64;
        
        // Use SIMD for the rest
        unsafe {
            for i in window..len {
                sum += values[i];
                sum -= values[i - window];
                out[i] = sum / window as f64;
            }
        }
        
        0
    }

    #[no_mangle]
    pub extern "C" fn compute_exponential_moving_average_simd(
        values: *const f64,
        len: usize,
        alpha: f64,
        out: *mut f64,
    ) -> i32 {
        if len == 0 || alpha < 0.0 || alpha > 1.0 {
            return -1;
        }

        let values = unsafe { std::slice::from_raw_parts(values, len) };
        let out = unsafe { std::slice::from_raw_parts_mut(out, len) };

        // First value is just copied
        out[0] = values[0];

        unsafe {
            let alpha_v = _mm256_set1_pd(alpha);
            let one_minus_alpha_v = _mm256_set1_pd(1.0 - alpha);

            // Process 4 values at a time using AVX
            for i in (1..len).step_by(4) {
                if i + 4 <= len {
                    let prev_ema = _mm256_loadu_pd(&out[i - 1]);
                    let curr_values = _mm256_loadu_pd(&values[i]);
                    
                    // EMA = α * current + (1 - α) * prevEMA
                    let ema = _mm256_add_pd(
                        _mm256_mul_pd(alpha_v, curr_values),
                        _mm256_mul_pd(one_minus_alpha_v, prev_ema)
                    );
                    
                    _mm256_storeu_pd(&mut out[i], ema);
                } else {
                    // Handle remaining values
                    for j in i..len {
                        out[j] = alpha * values[j] + (1.0 - alpha) * out[j - 1];
                    }
                }
            }
        }

        0
    }

    #[no_mangle]
    pub extern "C" fn compute_standard_deviation_simd(
        values: *const f64,
        len: usize,
        window: usize,
        out: *mut f64,
    ) -> i32 {
        if window == 0 || len < window {
            return -1;
        }

        let values = unsafe { std::slice::from_raw_parts(values, len) };
        let out = unsafe { std::slice::from_raw_parts_mut(out, len) };

        unsafe {
            for i in (window-1)..len {
                let start = i + 1 - window;
                let window_slice = &values[start..=i];

                // Compute mean using SIMD
                let mut sum = 0.0;
                let mut j = 0;
                while j + 4 <= window {
                    let v = _mm256_loadu_pd(&window_slice[j]);
                    sum += _mm256_reduce_add_pd(v);
                    j += 4;
                }
                for k in j..window {
                    sum += window_slice[k];
                }
                let mean = sum / window as f64;

                // Compute variance using SIMD
                let mean_v = _mm256_set1_pd(mean);
                let mut var_sum = 0.0;
                j = 0;
                while j + 4 <= window {
                    let v = _mm256_loadu_pd(&window_slice[j]);
                    let diff = _mm256_sub_pd(v, mean_v);
                    let sq = _mm256_mul_pd(diff, diff);
                    var_sum += _mm256_reduce_add_pd(sq);
                    j += 4;
                }
                for k in j..window {
                    let diff = window_slice[k] - mean;
                    var_sum += diff * diff;
                }

                out[i] = (var_sum / window as f64).sqrt();
            }
        }

        0
    }

    // Helper function to sum 4 doubles in a vector
    #[inline]
    unsafe fn _mm256_reduce_add_pd(v: __m256d) -> f64 {
        let sum = _mm256_hadd_pd(v, v);
        let lo = _mm256_castpd256_pd128(sum);
        let hi = _mm256_extractf128_pd(sum, 1);
        let sum = _mm_add_pd(lo, hi);
        _mm_cvtsd_f64(sum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    
    proptest! {
        #[test]
        fn test_compression_roundtrip(points in prop::collection::vec(
            (i64::MIN..i64::MAX, f64::MIN..f64::MAX), 0..1000
        )) {
            let input: Vec<TimePoint> = points
                .into_iter()
                .map(|(t, v)| TimePoint { timestamp: t, value: v })
                .collect();
            
            let mut compressed_size = 0;
            let compressed = compress_time_series(
                input.as_ptr(),
                input.len(),
                &mut compressed_size
            );
            
            let mut decompressed_len = 0;
            let decompressed = decompress_time_series(
                compressed,
                compressed_size,
                &mut decompressed_len
            );
            
            let decompressed_slice = unsafe {
                slice::from_raw_parts(decompressed, decompressed_len)
            };
            
            assert_eq!(input.len(), decompressed_len);
            for (a, b) in input.iter().zip(decompressed_slice.iter()) {
                assert_eq!(a.timestamp, b.timestamp);
                assert_eq!(a.value, b.value);
            }
            
            // Clean up
            free_compressed_data(compressed as *mut u8, compressed_size);
            free_time_points(decompressed as *mut TimePoint, decompressed_len);
        }
    }
}
