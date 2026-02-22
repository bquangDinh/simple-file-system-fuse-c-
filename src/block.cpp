// Provide functions to open, read, close file along with FLAGS such as O_CREAT, O_RDWR, etc..
#include <fcntl.h>
#include <assert.h>
#include <unistd.h>
#include <mutex>

#include "myfs/block.hpp"
#include "utilities.hpp"

#define STORAGE_OPEN_OPS O_CREAT | O_RDWR
#define STORAGE_OPEN_PERMS S_IRUSR | S_IWUSR

bool StorageManager::is_opened() {
    return _diskfile_fd != -1;
}

std::shared_mutex& StorageManager::get_lock_for_blk(blk_t blk_num) {
    return rwlocks[blk_num % NUM_LOCKS];
}

error_t StorageManager::init(const char* diskfile_path) {
    DBG("Opening storage in file: %s", diskfile_path);

    if (is_opened()) {
        DBG("File is already opened");

        return 0;
    }

    _diskfile_fd = open(diskfile_path, STORAGE_OPEN_OPS, STORAGE_OPEN_PERMS);

    if (_diskfile_fd < 0) {
        DBG("Failed to open diskfile -errno: %d", errno);
        return -errno;
    }

    DBG("Truncating diskfile...");

    if (ftruncate(_diskfile_fd, StorageManager::DISK_SIZE) < 0) {
        DBG("Failed to truncate diskfile - errno: %d", errno);

        return -errno;
    }

    DBG("Done.");

    return 0;
}

error_t StorageManager::storage_fsync() {
    DBG("Syncing storage...");

    assert(is_opened());

    return fsync(_diskfile_fd);
}

error_t StorageManager::storage_close() {
    DBG("Closing storage...");

    assert(is_opened());

    if (close(_diskfile_fd) < 0) {
        DBG("Failed to close storage - errno: %d", errno);
        return -errno;
    }

    DBG("Closed");

    return 0;
}

error_t StorageManager::block_read(const blk_t block_num, void* buf) {
    DBG("Reading from block: %u", block_num);

    assert(is_opened());
    assert(block_num >= 0 && block_num < NUM_BLK);

    // Acquire read lock
    std::shared_lock<std::shared_mutex> lock(get_lock_for_blk(block_num));

    ssize_t bytes_read = pread(_diskfile_fd, buf, BLOCK_SIZE, block_num * BLOCK_SIZE);

    if (bytes_read < 0) {
        DBG("Failed to read block - bytes read: %zd", bytes_read);
        return -errno;
    }

    DBG("Done. Bytes read: %zd", bytes_read);

    return bytes_read;
}

error_t StorageManager::block_write(const blk_t block_num, const void* buf) {
    DBG("Writing into block: %u", block_num);
    
    assert(is_opened());
    assert(block_num >= 0 && block_num < NUM_BLK);

    // Acquire write lock
    std::unique_lock<std::shared_mutex> lock(get_lock_for_blk(block_num));

    ssize_t bytes_written = pwrite(_diskfile_fd, buf, BLOCK_SIZE, block_num * BLOCK_SIZE);

    if (bytes_written < 0) {
        DBG("Failed to write block - bytes written: %zd", bytes_written);
        return -errno;
    }

    DBG("Done. Bytes written: %zd", bytes_written);

    return bytes_written;
}


error_t StorageManager::block_read_not_locked(const blk_t block_num, void* buf) {
    DBG("Reading from block: %u", block_num);

    assert(is_opened());
    assert(block_num >= 0 && block_num < NUM_BLK);

    ssize_t bytes_read = pread(_diskfile_fd, buf, BLOCK_SIZE, block_num * BLOCK_SIZE);

    if (bytes_read < 0) {
        DBG("Failed to read block - bytes read: %zd", bytes_read);
        return -errno;
    }

    DBG("Done. Bytes read: %zd", bytes_read);

    return bytes_read;
}

error_t StorageManager::block_write_not_locked(const blk_t block_num, const void* buf) {
    DBG("Writing into block: %u", block_num);
    
    assert(is_opened());
    assert(block_num >= 0 && block_num < NUM_BLK);

    ssize_t bytes_written = pwrite(_diskfile_fd, buf, BLOCK_SIZE, block_num * BLOCK_SIZE);

    if (bytes_written < 0) {
        DBG("Failed to write block - bytes written: %zd", bytes_written);
        return -errno;
    }

    DBG("Done. Bytes written: %zd", bytes_written);

    return bytes_written;
}
