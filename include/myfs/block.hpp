#pragma once

// define limits on integer such as uint8, uint16, etc
#include <stdint.h>
#include <errno.h>
#include <shared_mutex>

typedef uint32_t blk_t;

class StorageManager {
public:
    static constexpr size_t NUM_LOCKS = 128;
private:
    int _diskfile_fd = -1;

    std::shared_mutex rwlocks[StorageManager::NUM_LOCKS];

    StorageManager() {}

    ~StorageManager() = default;

    // Copy and assign behavior undefine for singleton since there must be only one instance
    StorageManager(const StorageManager&) = delete;
    StorageManager& operator=(const StorageManager&) = delete;

    bool is_opened();

    std::shared_mutex& get_lock_for_blk(blk_t blk_num);
public:
    static const int DISK_SIZE = 32 * 1024 * 1024;
    static const int BLOCK_SIZE = 4096;
    static const int NUM_BLK = DISK_SIZE / BLOCK_SIZE;

    static StorageManager& instance () {
        static StorageManager instance;

        return instance;
    }

    error_t init(const char* diskfile_path);

    error_t storage_fsync();

    error_t storage_close();

    error_t block_read(const blk_t block_num, void* buf);

    error_t block_write(const blk_t block_num, const void* buf);
};

