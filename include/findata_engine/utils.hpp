#pragma once

#include <cstdint>
#include <vector>
#include <string_view>
#include <chrono>
#include <span>
#include <immintrin.h>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include "types.hpp"

namespace findata_engine {
namespace utils {

// SIMD-optimized compression for numerical data
std::vector<uint8_t> compress_doubles(std::span<const double> data);
std::vector<double> decompress_doubles(const std::vector<uint8_t>& compressed_data);

// Time-series specific compression
std::vector<uint8_t> compress_time_series(const std::vector<TimeSeriesPoint>& points);
std::vector<TimeSeriesPoint> decompress_time_series(const std::vector<uint8_t>& compressed);

// Add hash function for time_point
struct TimePointHash {
    std::size_t operator()(const std::chrono::system_clock::time_point& tp) const {
        return std::hash<std::chrono::system_clock::rep>{}(tp.time_since_epoch().count());
    }
};

// Memory-mapped file utilities
class MemoryMappedFile {
public:
    MemoryMappedFile(const std::string& path, size_t size);
    ~MemoryMappedFile();

    void* data() const { return data_; }
    size_t size() const { return size_; }
    void flush();
    void resize(size_t new_size);

private:
    void* data_;
    size_t size_;
    int fd_;
};

// Cache management utilities
template<typename K, typename V, typename Hash = std::hash<K>>
class LRUCache {
private:
    struct Node {
        K key;
        V value;
        Node* prev;
        Node* next;
        
        Node(const K& k, const V& v) : key(k), value(v), prev(nullptr), next(nullptr) {}
    };
    
    size_t max_size_;
    Node* head_;
    Node* tail_;
    std::unordered_map<K, std::unique_ptr<Node>, Hash> cache_;
    
    void move_to_front(Node* node) {
        if (node == head_) return;
        
        // Remove from current position
        node->prev->next = node->next;
        if (node->next) node->next->prev = node->prev;
        else tail_ = node->prev;
        
        // Move to front
        node->next = head_;
        node->prev = nullptr;
        head_->prev = node;
        head_ = node;
    }
    
    void remove_node(Node* node) {
        if (node->prev) node->prev->next = node->next;
        else head_ = node->next;
        
        if (node->next) node->next->prev = node->prev;
        else tail_ = node->prev;
    }
    
public:
    explicit LRUCache(size_t max_size) : max_size_(max_size), head_(nullptr), tail_(nullptr) {}
    
    std::optional<V> get(const K& key) {
        auto it = cache_.find(key);
        if (it == cache_.end()) return std::nullopt;
        
        Node* node = it->second.get();
        move_to_front(node);
        return node->value;
    }
    
    void put(const K& key, V value) {
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            it->second->value = value;
            move_to_front(it->second.get());
            return;
        }
        
        auto node = std::make_unique<Node>(key, value);
        Node* node_ptr = node.get();
        
        if (!head_) {
            head_ = tail_ = node_ptr;
        } else {
            node_ptr->next = head_;
            head_->prev = node_ptr;
            head_ = node_ptr;
        }
        
        cache_[key] = std::move(node);
        
        if (cache_.size() > max_size_) {
            auto old_key = tail_->key;
            Node* new_tail = tail_->prev;
            new_tail->next = nullptr;
            cache_.erase(old_key);
            tail_ = new_tail;
        }
    }
    
    void clear() {
        cache_.clear();
        head_ = tail_ = nullptr;
    }
    
    size_t size() const {
        return cache_.size();
    }
};

} // namespace utils
} // namespace findata_engine
