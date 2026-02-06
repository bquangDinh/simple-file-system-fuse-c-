#pragma once

#include <mutex>

#include "block.hpp"
#include "superblock.hpp"

class DataBlockManager {
private:
    std::mutex data_bitmap_lock;

    DataBlockManager() {}

    ~DataBlockManager() = default;

    // Copy and assign behavior undefine for singleton since there must be only one instance
    DataBlockManager(const DataBlockManager&) = delete;
    DataBlockManager& operator=(const DataBlockManager&) = delete;

    error_t init_bitmap();
public:
    static DataBlockManager& instance () {
        static DataBlockManager instance;

        return instance;
    }

    error_t init();

    error_t get_available_blk(blk_t& out);

    error_t release_data_block(blk_t blk);

    error_t release_multi_data_blk(blk_t* blks, size_t len);
};