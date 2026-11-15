#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>

template <typename T, size_t Capacity>
struct SPSCQueue {
    static constexpr size_t MASK = Capacity - 1; // Capacity must be a power of 2

    // Aligned to 64-byte cache lines to prevent false sharing between cores
    alignas(64) std::atomic<size_t> head{0};
    alignas(64) std::atomic<size_t> tail{0};
    alignas(64) T buffer[Capacity];

    bool push(const T& item) {
        size_t t = tail.load(std::memory_order_relaxed);
        size_t h = head.load(std::memory_order_acquire);
        if (t - h == Capacity) return false;

        buffer[t & MASK] = item;
        tail.store(t + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        size_t h = head.load(std::memory_order_relaxed);
        size_t t = tail.load(std::memory_order_acquire);
        if (h == t) return false;

        item = buffer[h & MASK];
        head.store(h + 1, std::memory_order_release);
        return true;
    }

    size_t size() const {
        return tail.load(std::memory_order_acquire) - head.load(std::memory_order_acquire);
    }
};
