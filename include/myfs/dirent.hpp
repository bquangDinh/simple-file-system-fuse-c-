#pragma once

#include <linux/limits.h>

#include "inode.hpp"

#define DIR_FIND_FOUND_ITEM 1
#define DIR_FIND_NOT_FOUND_ITEM 0

struct dirent_t {
    ino_t ino;
    uint16_t valid;
    char name[NAME_MAX + 1];
    uint16_t len;
};

class DirentManager {
private:
    DirentManager() {}

    ~DirentManager() = default;

    // Copy and assign behavior undefine for singleton since there must be only one instance
    DirentManager(const DirentManager&) = delete;
    DirentManager& operator=(const DirentManager&) = delete;
public:
    static const uint16_t NUM_DIRENT_PER_BLOCK = StorageManager::BLOCK_SIZE / sizeof(dirent_t);

    static DirentManager& instance () {
        static DirentManager instance;

        return instance;
    }

    error_t dir_find(ino_t ino, const char* fname, dirent_t* out);

    error_t dir_add(Inode& parent, const Inode& child, const char* fname);

    error_t dir_add(Inode& parent, ino_t child_ino, const char* fname);

    error_t dir_remove(Inode& parent, const char* fname);

    int dir_entries_count(Inode& dir);

    error_t dir_update_dotdot(Inode& dir_inode, Inode& new_parent);
};