// Provide functions to open, read, close file along with FLAGS such as O_CREAT, O_RDWR, etc..
#include <fcntl.h>
#include <assert.h>
#include <unistd.h>
#include <mutex>

#include "myfs/block.hpp"

#define STORAGE_OPEN_OPS O_CREAT | O_RDWR
#define STORAGE_OPEN_PERMS S_IRUSR | S_IWUSR

bool StorageManager::is_opened() {
    return _diskfile_fd != -1;
}

std::shared_mutex& StorageManager::get_lock_for_blk(blk_t blk_num) {
    return rwlocks[blk_num % NUM_LOCKS];
}

error_t StorageManager::init(const char* diskfile_path) {
    if (is_opened()) return 0;

    _diskfile_fd = open(diskfile_path, STORAGE_OPEN_OPS, STORAGE_OPEN_PERMS);

    if (_diskfile_fd < 0) {
        return -errno;
    }

    if (ftruncate(_diskfile_fd, StorageManager::DISK_SIZE) < 0) {
        return -errno;
    }

    return 0;
}

error_t StorageManager::storage_fsync() {
    assert(is_opened());

    return fsync(_diskfile_fd);
}

error_t StorageManager::storage_close() {
    assert(is_opened());

    if (close(_diskfile_fd) < 0) {
        return -errno;
    }

    return 0;
}

error_t StorageManager::block_read(const blk_t block_num, void* buf) {
    assert(is_opened());
    assert(block_num >= 0 && block_num < NUM_BLK);

    // Acquire read lock
    std::shared_lock<std::shared_mutex> lock(get_lock_for_blk(block_num));

    ssize_t bytes_read = pread(_diskfile_fd, buf, BLOCK_SIZE, block_num * BLOCK_SIZE);

    if (bytes_read < 0) {
        return -errno;
    }

    return bytes_read;
}

error_t StorageManager::block_write(const blk_t block_num, const void* buf) {
    assert(is_opened());
    assert(block_num >= 0 && block_num < NUM_BLK);

    // Acquire write lock
    std::unique_lock<std::shared_mutex> lock(get_lock_for_blk(block_num));

    ssize_t bytes_written = pwrite(_diskfile_fd, buf, BLOCK_SIZE, block_num * BLOCK_SIZE);

    if (bytes_written < 0) {
        return -errno;
    }

    return bytes_written;
}


