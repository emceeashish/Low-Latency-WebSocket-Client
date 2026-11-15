#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>
#include "spsc_queue.h"

constexpr size_t POOL_BLOCK_SIZE = 4096;
constexpr size_t POOL_NUM_BLOCKS = 256; // 1 MB pre-allocated pool

struct Message {
    uint8_t* data = nullptr;
    uint32_t size = 0;
    bool is_text  = true;
};

struct MemoryPool {
    std::vector<uint8_t> storage;
    SPSCQueue<uint8_t*, POOL_NUM_BLOCKS> free_blocks;

    MemoryPool() : storage(POOL_BLOCK_SIZE * POOL_NUM_BLOCKS) {
        for (size_t i = 0; i < POOL_NUM_BLOCKS; i++) {
            free_blocks.push(storage.data() + (i * POOL_BLOCK_SIZE));
        }
    }

    uint8_t* allocate() {
        uint8_t* block = nullptr;
        free_blocks.pop(block);
        return block;
    }

    void deallocate(uint8_t* block) {
        if (block != nullptr) {
            free_blocks.push(block);
        }
    }
};
