#pragma once

// define limits on integer such as uint8, uint16, etc
#include <stdint.h>
#include <linux/limits.h>
#include <sys/stat.h>
#include <mutex>

#include "block.hpp"
#include "superblock.hpp"
#include "data.hpp"

#define DIRECT_PTRS_COUNT 12
#define IS_DATA_BLK_ALLOCATED(blk) blk != 0
#define ROOT_INO 1

typedef uint32_t open_count_t;
typedef uint32_t uid_t;
typedef uint32_t gid_t;
typedef uint32_t mode_t;

struct inode_t {
    uint32_t ino;
    uint16_t valid;
    uint64_t size;
    mode_t mode;
    
    uint16_t nlink;
    open_count_t open_count;

    uid_t uid;
    gid_t gid;

    struct timespec atime;
    struct timespec mtime;
    struct timespec ctime;

    blk_t directs[DIRECT_PTRS_COUNT];
    blk_t singly_indirect_ptr;
};

class Inode {
private:
    uid_t context_uid;
    gid_t context_gid;
public:
    inode_t inode;

    Inode();
    Inode(ino_t, mode_t, nlink_t, uid_t, gid_t);
    Inode(const Inode&);
    
    ~Inode() = default;

    Inode& operator=(const Inode&);
    Inode& operator=(const inode_t&);

    void from(ino_t, mode_t, nlink_t, uid_t, gid_t);

    bool is_valid();

    bool is_direct_allocated_at(uint16_t);

    bool is_singly_indirect_allocated();

    blk_t get_block_direct_at(uint16_t) const;

    mode_t get_perm_based_ctx();

    bool can_write();

    bool can_read();

    bool can_execute();

    bool is_dir();
    
    bool is_dir_sticky();

    bool is_dir_empty();

    bool should_file_be_deleted();

    bool user_is_owner();

    bool user_is_group();

    ino_t get_ino() const;

    error_t get_indirect_blk_data(int* out);

    error_t save();

    // Release this inode back to inode bitmap and all of its associated data blocks
    error_t release();
};

class InodeManager {
private:
    std::mutex inode_bitmap_lock;

    InodeManager() {}

    ~InodeManager() = default;

    // Copy and assign behavior undefine for singleton since there must be only one instance
    InodeManager(const InodeManager&) = delete;
    InodeManager& operator=(const InodeManager&) = delete;

    error_t init_bitmap();

    error_t init_root();
public:
    Inode* root = nullptr;

    static InodeManager& instance () {
        static InodeManager instance;

        return instance;
    }

    error_t init();

    error_t get_available_ino(ino_t& out);

    error_t release_ino(ino_t ino);

    error_t get_inode(ino_t ino, Inode& out);

    error_t get_inode_from_path(const char* path, ino_t start, Inode& out);
};

