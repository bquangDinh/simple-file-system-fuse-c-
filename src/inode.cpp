#define FUSE_USE_VERSION 30

#include <assert.h>

#include <fuse3/fuse.h>
#include <cstring>
#include <cstdlib>

#include "myfs/superblock.hpp"
#include "myfs/inode.hpp"
#include "myfs/dirent.hpp"
#include "utilities.hpp"


/* --------------- Inode class ------------------ */
Inode::Inode()
{
    inode = { 0 };
}

Inode::Inode(ino_t ino, mode_t mode, nlink_t nlink, uid_t uid, gid_t gid)
{
    assert(ino < SuperblockManager::instance().get_max_inum());

    inode.ino = ino;
    inode.valid = 1;
    inode.size = 0;
    inode.mode = mode;
    inode.nlink = nlink;
    inode.open_count = 0;
    inode.uid = uid;
    inode.gid = gid;

    inode.atime = Utilities::now();
    inode.mtime = Utilities::now();
    inode.ctime = Utilities::now();

    Utilities::fill_array<blk_t>(inode.directs, 0, DIRECT_PTRS_COUNT);

    inode.singly_indirect_ptr = 0;
}

Inode::Inode(const Inode& from)
{
    inode.ino = from.inode.ino;
    inode.valid = from.inode.valid;
    inode.size = from.inode.size;
    inode.mode = from.inode.mode;
    inode.nlink = from.inode.nlink;
    inode.open_count = from.inode.open_count;
    inode.uid = from.inode.uid;
    inode.gid = from.inode.gid;
    
    inode.atime = from.inode.atime;
    inode.mtime = from.inode.mtime;
    inode.ctime = from.inode.ctime;

    memcpy(inode.directs, from.inode.directs, DIRECT_PTRS_COUNT * sizeof(blk_t));

    inode.singly_indirect_ptr = from.inode.singly_indirect_ptr;
}

Inode& Inode::operator=(const Inode& from) {
    Inode* ino = new Inode(from);

    return *ino;
}

Inode& Inode::operator=(const inode_t& from) {
    Inode* ino = new Inode();

    ino->inode = from;

    return *ino;
}

bool Inode::is_valid() {
    return inode.valid == 1;
}

bool Inode::is_direct_allocated_at(uint16_t i) {
    return inode.directs[i] > 0;
}

blk_t Inode::get_block_direct_at(uint16_t i) const {
    assert(i < DIRECT_PTRS_COUNT);

    return inode.directs[i];
}

ino_t Inode::get_ino() const {
    return (ino_t)inode.ino;
}

bool Inode::can_write() {
    fuse_context* ctx = fuse_get_context();

    assert(ctx != nullptr);

    mode_t perm = get_perm_based_ctx();

    return PERM_CAN_WRITE(perm) || IS_ROOT(ctx->uid);
}

bool Inode::can_read() {
    fuse_context* ctx = fuse_get_context();

    assert(ctx != nullptr);

    mode_t perm = get_perm_based_ctx();

    return PERM_CAN_READ(perm) || IS_ROOT(ctx->uid);
}

bool Inode::can_execute() {
    fuse_context* ctx = fuse_get_context();

    assert(ctx != nullptr);

    mode_t perm = get_perm_based_ctx();

    return PERM_CAN_EXECUTE(perm) || IS_ROOT(ctx->uid);
}

bool Inode::is_dir() {
    return S_ISDIR(inode.mode);
}

error_t Inode::save() {
    StorageManager& storage = StorageManager::instance();
    SuperblockManager& superblock = SuperblockManager::instance();

    uint32_t inodes_per_block = storage.BLOCK_SIZE / sizeof(inode_t);
    uint32_t block_index = inode.ino / inodes_per_block;
    uint32_t inode_index = inode.ino % inodes_per_block;
    uint32_t inode_region_start_idx = superblock.get_ino_region_start_blk();

    // Read inode block
    inode_t* buf = (inode_t*)std::malloc(storage.BLOCK_SIZE);

    if (buf == nullptr) return -ENOMEM;

    error_t err = storage.block_read(block_index, buf);

    if (err < 0) {
        free(buf);

        return err;
    }

    // Update ctime
    inode.ctime = Utilities::now();

    memcpy(buf + inode_index, &inode, sizeof(inode_t));

    err = storage.block_write(block_index, buf);

    if (err < 0) {
        free(buf);

        return err;
    }

    return 0;
}

error_t Inode::release() {
    DataBlockManager& data_manager = DataBlockManager::instance();
    InodeManager& inode_manager = InodeManager::instance();
    StorageManager& storage = StorageManager::instance();

    error_t err = data_manager.release_multi_data_blk(inode.directs, DIRECT_PTRS_COUNT);

    if (err < 0) return err;

    if (inode.singly_indirect_ptr > 0) {
        size_t num_blk = storage.BLOCK_SIZE / sizeof(blk_t);

        blk_t* buf = (blk_t*)malloc(storage.BLOCK_SIZE);

        if (buf == nullptr) return -ENOMEM;

        err = storage.block_read(inode.singly_indirect_ptr, buf);

        if (err < 0) {
            free(buf);

            return err;
        }

        err = data_manager.release_multi_data_blk(buf, num_blk);

        if (err < 0) {
            free(buf);

            return err;
        }

        err = data_manager.release_data_block(inode.singly_indirect_ptr);

        if (err < 0) {
            free(buf);

            return err;
        }

        free(buf);
    }

    err = inode_manager.release_ino(inode.ino);

    if (err < 0) return err;

    return 0;
}

mode_t Inode::get_perm_based_ctx() {
    fuse_context* ctx = fuse_get_context();

    assert(ctx != nullptr);

    uid_t uid = ctx->uid;
    gid_t gid = ctx->gid;

    mode_t perm;

    if (uid == inode.uid) perm = Utilities::PermissionOps::get_user_perm(inode.mode);
    else if (gid == inode.gid) perm = Utilities::PermissionOps::get_group_perm(inode.mode);
    else perm = Utilities::PermissionOps::get_other_perm(inode.mode);

    return perm;
}

void Inode::from(ino_t ino, mode_t mode, nlink_t nlink, uid_t uid, gid_t gid) {
    inode.ino = ino;
    inode.mode = mode;
    inode.nlink = nlink;
    inode.uid = uid;
    inode.gid = gid;
}

error_t Inode::get_indirect_blk_data(int* out) {
    StorageManager& storage = StorageManager::instance();

    assert(inode.singly_indirect_ptr > 0);

    uint16_t num_blks_indirect = storage.BLOCK_SIZE / sizeof(int);

    error_t err = storage.block_read(inode.singly_indirect_ptr, out);

    if (err < 0) return err;

    return 0;
}

bool Inode::is_singly_indirect_allocated() {
    return inode.singly_indirect_ptr > 0;
}
/* ---------------------------------------------- */

/* -------------- Inode Manager ----------------- */
error_t InodeManager::init_bitmap() {
    SuperblockManager& superblock = SuperblockManager::instance();
    StorageManager& storage = StorageManager::instance();

    if (superblock.superblock_existed) {
        // Read from old
        return 0;
    }

    const uint16_t inode_bitmap_blocks = superblock.get_ino_bitmap_blks();

    bitmap_t buffer = (bitmap_t)malloc(storage.BLOCK_SIZE);

    if (buffer == nullptr) return -ENOMEM;

    // Set all bits to zero
    memset(buffer, 0, storage.BLOCK_SIZE);

    error_t err;

    for (uint16_t i = 0; i < inode_bitmap_blocks; ++i) {
        err = storage.block_write(superblock.get_ino_bm_start_blk() + i, buffer);
        
        if (err < 0) {
            free(buffer);

            return err;
        }
    }

    free(buffer);

    return 0;
}

error_t InodeManager::init_root() {
    SuperblockManager& superblock = SuperblockManager::instance();
    DirentManager& dirent_manager = DirentManager::instance();

    error_t err;
    
    if (superblock.superblock_existed) {
        return 0;
    }

    // Make sure to mark root_ino in the bitmap
    ino_t root_ino;

    // TODO: use set_ino()
    err = get_available_ino(root_ino);

    assert(root_ino == ROOT_INO);

    fuse_context* ctx = fuse_get_context();

    assert(ctx != nullptr);

    uid_t uid = ctx->uid;
    gid_t gid = ctx->gid;

    root = new Inode(ROOT_INO, S_IFDIR | 0775, 1, uid, gid);
    
    root->save();

    // Add "." entry to root inode
    err = dirent_manager.dir_add(*root, ROOT_INO, ".");

    if (err < 0) {
        delete root;

        return err;
    }

    delete root;

    return 0;
}

error_t InodeManager::init() {
    error_t err = init_bitmap();

    if (err < 0) return err;

    err = init_root();

    if (err < 0) return err;

    return 0;
}

error_t InodeManager::get_available_ino(ino_t& out) {
    SuperblockManager& superblock = SuperblockManager::instance();
    StorageManager& storage = StorageManager::instance();

    std::lock_guard<std::mutex> lock(inode_bitmap_lock);

    uint16_t inode_bitmap_blocks = superblock.get_ino_bitmap_blks();

    size_t num_bits_per_block = storage.BLOCK_SIZE * 8;

    assert(inode_bitmap_blocks >= 1);
    assert(num_bits_per_block > 0);

    bitmap_t bitmap = (bitmap_t)malloc(storage.BLOCK_SIZE);

    if (bitmap == nullptr) return -ENOMEM;

    error_t err;

    uint32_t offset = superblock.get_ino_bm_start_blk();

    for (uint16_t i = 0; i < inode_bitmap_blocks; ++i) {
        err = storage.block_read(offset + i, bitmap);

        if (err < 0) {
            free(bitmap);

            return err;
        }

        for (uint32_t j = i * num_bits_per_block; j < (i + 1) * num_bits_per_block; ++j) {
            // No ino available
            if (j >= superblock.get_max_inum() - 1) break;

            if (Utilities::BitmapOps::get_bitmap(bitmap, j - i * num_bits_per_block) == 0) {
                // Free ino found
                Utilities::BitmapOps::set_bitmap(bitmap, j - i * num_bits_per_block);

                err = storage.block_write(offset + i, bitmap);

                if (err < 0) {
                    free(bitmap);

                    return err;
                }

                free(bitmap);

                // Update superblock stat
                superblock.decrease_free_ino_count();

                err = superblock.save();

                if (err < 0) {
                    return err;
                }

                // Ino always start at 1
                out = j + 1;

                return 0;
            }
        }
    }

    free(bitmap);

    return -ENOSPC;
}

error_t InodeManager::release_ino(ino_t ino) {
    SuperblockManager& superblock = SuperblockManager::instance();
    StorageManager& storage = StorageManager::instance();

    assert(ino < superblock.get_max_inum());

    std::lock_guard<std::mutex> lock(inode_bitmap_lock);

    uint32_t num_bits_per_block = storage.BLOCK_SIZE * 8;

    blk_t blk = superblock.get_ino_bm_start_blk() + (ino / num_bits_per_block);

    uint32_t bit_idx = ino % num_bits_per_block;

    bitmap_t bitmap = (bitmap_t)malloc(storage.BLOCK_SIZE);

    if (bitmap == nullptr) return -ENOMEM;

    error_t err = storage.block_read(blk, bitmap);

    if (err < 0) {
        free(bitmap);

        return err;
    }

    Utilities::BitmapOps::unset_bitmap(bitmap, bit_idx);

    err = storage.block_write(blk, bitmap);

    free(bitmap);

    if (err < 0) {
        return err;
    }

    superblock.increase_free_ino_count();

    err = superblock.save();

    if (err < 0) return err;

    return 0;
}

error_t InodeManager::get_inode(ino_t ino, Inode& out) {
    SuperblockManager& superblock = SuperblockManager::instance();
    StorageManager& storage = StorageManager::instance();

    assert(ino < superblock.get_max_dnum());

    uint32_t inodes_per_block = storage.BLOCK_SIZE / sizeof(inode_t);
    uint32_t blk_idx = ino / inodes_per_block;
    uint32_t ino_idx = ino % inodes_per_block;
    uint32_t offset = superblock.get_ino_region_start_blk();

    inode_t* buffer = (inode_t*)malloc(storage.BLOCK_SIZE);

    if (buffer == nullptr) return -ENOMEM;

    error_t err = storage.block_read(offset + blk_idx, buffer);

    if (err < 0) {
        free(buffer);

        return err;
    }

    inode_t inode = { 0 };
    
    memcpy(&inode, buffer + ino_idx, sizeof(inode_t));

    out = inode;

    free(buffer);

    return 0;
}

error_t InodeManager::get_inode_from_path(const char* path, ino_t start, Inode& out) {
    SuperblockManager& superblock = SuperblockManager::instance();
    StorageManager& storage = StorageManager::instance();
    DirentManager& dirent_manager = DirentManager::instance();

    assert(path != nullptr);
    assert(root != nullptr);
    assert(start < superblock.get_max_inum());

    struct fuse_context* ctx = fuse_get_context();

    assert(ctx != nullptr);

    uid_t uid = ctx->uid;
    gid_t gid = ctx->gid;

    mode_t perm;

    Utilities::path_split p = { 0 };

    if (Utilities::split_path(path, &p) < 0) return -ENOMEM;

    char* base = p.base;

    if (strlen(base) > NAME_MAX) return -ENAMETOOLONG;

    Inode current;
    dirent_t dir_entry = { 0 };

    error_t err = get_inode(start, current);

    if (err < 0) {
        Utilities::free_path_split(&p);

        return err;
    }

    char* path_clone = strdup(path);

	if (path_clone == nullptr) {
		Utilities::free_path_split(&p);

		return -ENOMEM;
	}

    char* save_ptr;
    char* token = strtok_r(path_clone, "/", &save_ptr);
    size_t token_len = 0;

    while (token) {
        if (strcmp(token, base) != 0) {
            // Mean we are still in the middle of traversing
			// Check if the current token is a dir
			// Cannot traverse a file
            if (!current.is_dir()) {
                free(path_clone);

                Utilities::free_path_split(&p);

                return -ENOTDIR;
            }
        }

        token_len = strlen(token);

		// Check if current has "execute" bit
        if (!current.can_execute()) {
            free(path_clone);

            Utilities::free_path_split(&p);

            return -EACCES;
        }

        if (token_len > NAME_MAX) {
            free(path_clone);

            Utilities::free_path_split(&p);

            return -ENAMETOOLONG;
        }

		// Check if the token exists in the current dir
        err = dirent_manager.dir_find(current.inode.ino, token, &dir_entry);

        if (err < 0) {
            free(path_clone);

            Utilities::free_path_split(&p);

            return err;
        }

        // Advance current to the next
        err = get_inode(dir_entry.ino, current);

        if (err < 0) {
            free(path_clone);

            Utilities::free_path_split(&p);

            return err;
        }

        // Consider the next token
        token = strtok_r(nullptr, "/", &save_ptr);
    }

    out = current;

    free(path_clone);

	Utilities::free_path_split(&p);

    return 0;
}
/* ---------------------------------------------- */