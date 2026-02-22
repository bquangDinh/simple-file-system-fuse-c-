#pragma once

#include <stdint.h>
#include <shared_mutex>

#include "myfs/block.hpp"

#define SUPERBLOCK_BLK_NUM 0
#define SUPERBLOCK_MAGIC_NUM 0x1234
#define MAX_INODE_NUM 1024
#define MAX_DATA_BLK_NUM 7165
#define INODE_BITMAP_BYTES ((MAX_INODE_NUM + 7) / 8) // in bytes
#define DATA_BITMAP_BYTES ((MAX_DATA_BLK_NUM + 7) / 8) // in bytes

struct superblock {
    uint16_t magic_num;
    
    uint32_t max_inum;
    uint32_t max_dnum;

    uint32_t i_bitmap_blk;
    uint32_t d_bitmap_blk;

    uint32_t i_start_blk;
    uint32_t d_start_blk;

    uint32_t free_blk_count;
    uint32_t free_ino_count;
};

class SuperblockManager {
private:
    std::shared_mutex superblock_lock;

    StorageManager& storage;

    SuperblockManager() : storage(StorageManager::instance()) {}

    ~SuperblockManager() = default;

    // Copy and assign behavior undefine for singleton since there must be only one instance
    SuperblockManager(const SuperblockManager&) = delete;
    SuperblockManager& operator=(const SuperblockManager&) = delete;

    superblock _superblock;
public:
    bool superblock_existed = false;
    
    static SuperblockManager& instance() {
        static SuperblockManager instance;

        return instance;
    }

    error_t init();

    error_t save();

    // Getters
    uint32_t get_max_inum();
    uint32_t get_max_dnum();

    uint32_t get_ino_bm_start_blk();
    uint32_t get_ino_bm_end_blk();

    uint32_t get_data_bm_start_blk();
    uint32_t get_data_bm_end_blk();

    uint32_t get_ino_region_start_blk();
    uint32_t get_ino_region_end_blk();

    uint32_t get_data_region_start_blk();
    uint32_t get_data_region_end_blk();

    uint32_t get_free_data_blk_count();
    uint32_t get_free_ino_count();

    uint16_t get_magic_num();

    uint16_t get_ino_bitmap_blks();

    uint16_t get_data_bitmap_blks();

    // Setters
    void increase_free_data_blk_count();
    void decrease_free_data_blk_count();

    void increase_free_ino_count();
    void decrease_free_ino_count();

    void print_info();
};