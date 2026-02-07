#
#include <assert.h>

#include <cstring>
#include <cstdlib>

#include "myfs/dirent.hpp"
#include "utilities.hpp"

error_t DirentManager::dir_find(ino_t ino, const char* fname, dirent_t* out) {
    SuperblockManager& superblock = SuperblockManager::instance();
    StorageManager& storage = StorageManager::instance();
    InodeManager& inode_manager = InodeManager::instance();

    assert(ino < superblock.get_max_inum());
    assert(fname != nullptr);

    size_t name_len = strlen(fname);

    if (name_len > NAME_MAX) return -ENAMETOOLONG;

    Inode dir_inode;

    error_t err = inode_manager.get_inode(ino, dir_inode);

    if (err < 0) return err;

    if (!dir_inode.is_valid()) return -ENOENT;

    dirent_t* buffer = (dirent_t*)malloc(storage.BLOCK_SIZE);

    if (buffer == nullptr) return -ENOMEM;

    uint16_t num_entries_per_block = storage.BLOCK_SIZE / sizeof(dirent_t);

    for (uint16_t i = 0; i < DIRECT_PTRS_COUNT; ++i) {
        if (!dir_inode.is_direct_allocated_at(i)) continue;

        err = storage.block_read(dir_inode.get_block_direct_at(i), buffer);

        if (err < 0) {
            free(buffer);

            return err;
        }

        for (uint16_t j = 0; j < num_entries_per_block; ++j) {
            if (buffer[j].valid == 1) {
                if (strncmp(buffer[j].name, fname, name_len) == 0) {
                    // If argument dirent is not null
                    // Fill dirent with result
                    if (out != nullptr) {
                        *out = buffer[j];
                    }

                    free(buffer);

                    return 0;
                }
            }
        }
    }

    free(buffer);

    return -ENOENT;
}

error_t DirentManager::dir_add(Inode& parent, const Inode& child, const char* fname) {
    return dir_add(parent, child.get_ino(), fname);
}

error_t DirentManager::dir_add(Inode& parent, ino_t child_ino, const char* fname) {
    SuperblockManager& superblock = SuperblockManager::instance();
    StorageManager& storage = StorageManager::instance();
    InodeManager& inode_manager = InodeManager::instance();
    DataBlockManager& datablock_manager = DataBlockManager::instance();

    assert(child_ino < superblock.get_max_inum());
    assert(fname != nullptr);

    // Check if this parent already has this child dirent
    error_t err = dir_find(parent.get_ino(), fname, nullptr);

    if (err == 0) return -EEXIST;
    else if (err < 0) return err;

    // -ENAMETOOLONG is already reported in dir_find()
    //  so in here, we can assume the name is good
    size_t name_len = strlen(fname);

    Inode finode;

    err = inode_manager.get_inode(child_ino, finode);

    if (err < 0) return err;

    dirent_t* buffer = (dirent_t*)malloc(storage.BLOCK_SIZE);

    if (buffer == nullptr) return -ENOMEM;

	// Find the first free slot or append it at the end
    uint16_t blk_idx = 0;

    uint16_t num_dirents_per_block = storage.BLOCK_SIZE / sizeof(dirent_t);

    blk_t blk;

    for (; blk_idx < DIRECT_PTRS_COUNT; ++blk_idx) {
		// Add item to the first unallocated block found
        if (!parent.is_direct_allocated_at(blk_idx)) break;

        blk = parent.get_block_direct_at(blk_idx);

        err = storage.block_read(blk, buffer);

        if (err < 0) {
            free(buffer);

            return err;
        }

        for (uint16_t i = 0; i < num_dirents_per_block; ++i) {
            if (buffer[i].valid == 0) {
                buffer[i].ino = child_ino;
                buffer[i].valid = 1;

                memcpy(buffer[i].name, fname, name_len);
                buffer[i].name[name_len] = '\0';
                buffer[i].len = name_len;

                err = storage.block_write(blk, buffer);

                if (err < 0) {
                    free(buffer);

                    return err;
                }

                // Update mtime
                parent.inode.mtime = Utilities::now();

                err = parent.save();
                
                if (err < 0) {
                    free(buffer);

                    return err;
                }

                free(buffer);

                return 0;
            }
        }
    }

    if (blk_idx == DIRECT_PTRS_COUNT) {
        free(buffer);

        return -ENOSPC;
    }

    assert(!parent.is_direct_allocated_at(blk_idx));

    blk_t data_block_idx;

    err = datablock_manager.get_available_blk(data_block_idx);

    if (err < 0) {
        free(buffer);

        return err;
    }

    memset(buffer, 0, storage.BLOCK_SIZE);

    buffer[0].ino = child_ino;
    buffer[0].valid = 1;
    memcpy(buffer[0].name, fname, name_len);
    buffer[0].name[name_len] = '\0';
    buffer[0].len = name_len;

    err = storage.block_write(data_block_idx, buffer);

    if (err < 0) {
        free(buffer);

        return err;
    }
    
    free(buffer);

    // Update parent inode
    parent.inode.directs[blk_idx] = data_block_idx;
    parent.inode.size += storage.BLOCK_SIZE;
    parent.inode.mtime = Utilities::now();

    err = parent.save();

    if (err) return err;

    return 0;
}

error_t DirentManager::dir_remove(Inode& parent, const char* fname) {
    StorageManager& storage = StorageManager::instance();

    assert(fname != nullptr);

    size_t name_len = strlen(fname);

    if (name_len > NAME_MAX) return -ENAMETOOLONG;

    dirent_t* buffer = (dirent_t*)malloc(storage.BLOCK_SIZE);

    if (buffer == nullptr) return -ENOMEM;

    uint16_t num_entries_per_block = storage.BLOCK_SIZE / sizeof(dirent_t);

    error_t err;

    blk_t blk;

    for (uint16_t i = 0; i < DIRECT_PTRS_COUNT; ++i) {
        if (!parent.is_direct_allocated_at(i)) continue;

        blk = parent.get_block_direct_at(i);

        err = storage.block_read(blk, buffer);

        if (err < 0) {
            free(buffer);

            return err;
        }

        for (uint16_t j = 0; j < num_entries_per_block; ++j) {
            if (buffer[j].valid == 1 && strncmp(buffer[j].name, fname, name_len) == 0) {
                buffer[j].valid = 0;
                buffer[j].name[0] = '\0';

                err = storage.block_write(blk, buffer);

                if (err < 0) {
                    free(buffer);

                    return err;
                }

                free(buffer);

                // Update parent inode
                parent.inode.mtime = Utilities::now();

                err = parent.save();

                if (err < 0) return err;

                return 0;
            }
        }
    }

    // target does not exist
    free(buffer);

    return -ENOENT;
}

int DirentManager::dir_entries_count(Inode& dir) {
    StorageManager& storage = StorageManager::instance();

    dirent_t* buffer = (dirent_t*)malloc(storage.BLOCK_SIZE);

    if (buffer == nullptr) return -ENOMEM;

    uint16_t num_entries_per_block = storage.BLOCK_SIZE / sizeof(dirent_t);

    int16_t count = 0;
    
    error_t err;

    blk_t blk;

    for (uint16_t i = 0; i < DIRECT_PTRS_COUNT; ++i) {
        if (!dir.is_direct_allocated_at(i)) continue;

        blk = dir.get_block_direct_at(i);
        
        err = storage.block_read(blk, buffer);

        if (err < 0) {
            free(buffer);
            
            return err;
        }

        for (uint16_t j = 0; j < num_entries_per_block; ++j) {
            if (buffer[j].valid == 1) count++;
        }
    }

    free(buffer);

    return count;
}

int DirentManager::dir_entries_count(inode_t dir) {
    StorageManager& storage = StorageManager::instance();

    dirent_t* buffer = (dirent_t*)malloc(storage.BLOCK_SIZE);

    if (buffer == nullptr) return -ENOMEM;

    uint16_t num_entries_per_block = storage.BLOCK_SIZE / sizeof(dirent_t);

    int16_t count = 0;
    
    error_t err;

    blk_t blk;

    for (uint16_t i = 0; i < DIRECT_PTRS_COUNT; ++i) {
        if (dir.directs[i] == 0) continue;

        blk = dir.directs[i];
        
        err = storage.block_read(blk, buffer);

        if (err < 0) {
            free(buffer);
            
            return err;
        }

        for (uint16_t j = 0; j < num_entries_per_block; ++j) {
            if (buffer[j].valid == 1) count++;
        }
    }

    free(buffer);

    return count;
}

error_t DirentManager::dir_update_dotdot(Inode& dir_inode, Inode& new_parent) {
    StorageManager& storage = StorageManager::instance();
    InodeManager& inode_manager = InodeManager::instance();

    dirent_t* buffer = (dirent_t*)malloc(storage.BLOCK_SIZE);

    if (buffer == nullptr) return -ENOMEM;

    uint16_t num_entries_per_block = storage.BLOCK_SIZE / sizeof(dirent_t);

    error_t err;

    blk_t blk;

    for (uint16_t i = 0; i < DIRECT_PTRS_COUNT; ++i) {
        if (!dir_inode.is_direct_allocated_at(i)) continue;

        blk = dir_inode.get_block_direct_at(i);

        err = storage.block_read(blk, buffer);

        if (err < 0) {
            free(buffer);

            return err;
        } 

        for (uint16_t j = 0; j < num_entries_per_block; ++j) {
            if (buffer[j].valid == 1 && strncmp(buffer[j].name, "..", 2) == 0) {
                if (buffer[j].ino == new_parent.get_ino()) {
                    // Nothing changes
                    free(buffer);

                    return 0;
                }

                // Read old parent of this dir inode
                Inode old_parent;

                err = inode_manager.get_inode(buffer[j].ino, old_parent);

                if (err < 0) {
                    free(buffer);

                    return err;
                }

                assert(old_parent.inode.nlink > 0);

                old_parent.inode.nlink--;

                err = old_parent.save();

                if (err < 0) {
                    free(buffer);

                    return err;
                }

                new_parent.inode.nlink++;

                err = new_parent.save();

                if (err < 0) {
                    free(buffer);

                    return err;
                }

                free(buffer);

                return 0;
            }
        }
    }

    free(buffer);

    return -ENOENT;
}

bool DirentManager::is_descendant(const Inode& i, const Inode& origin) {
    return is_descendant(i.inode, origin.inode);
}

bool DirentManager::is_descendant(const inode_t& i, const inode_t& origin) {
    // If origin is root folder
    // then it's always true as every inode has root
    if (origin.ino == ROOT_INO) return true;

    // Walk '..' entry of i until reached root or origin
    dirent_t parent = { 0 };

    ino_t current_ino = i.ino;

    do {
        assert(dir_find(current_ino, "..", &parent) == 0);

        if (parent.ino == origin.ino) return true;

        current_ino = parent.ino;
    } while (parent.ino != ROOT_INO);

    return false;
}