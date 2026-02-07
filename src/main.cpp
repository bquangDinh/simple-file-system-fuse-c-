#define FUSE_USE_VERSION 30
#define GNU_SOURCE

#include <fuse3/fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <linux/limits.h>
#include <assert.h>
#include <shared_mutex>

#include "myfs/block.hpp"
#include "myfs/superblock.hpp"
#include "myfs/inode.hpp"
#include "myfs/data.hpp"
#include "myfs/dirent.hpp"
#include "utilities.hpp"

/* Helper Macros */
#define ACCMODE_FROM_FLAG(flag) (flag & O_ACCMODE)
#define ACCMODE_REQUEST_READ(acc_mode) ((acc_mode) == O_RDONLY || (acc_mode) == O_RDWR)
#define ACCMODE_REQUEST_WRITE(acc_mode) ((acc_mode) == O_WRONLY || (acc_mode) == O_RDWR)

/* Locks */
#define MAX_INODE_LOCKS 1024
std::shared_mutex inode_rw_locks[MAX_INODE_LOCKS];
FILE* thread_log_fd = nullptr;
const char* THREAD_LOG_FILE = "/home/myfs-thread.log";

#define RD_LOCK(m) do { \
	fprintf(thread_log_fd == nullptr ? stdout : thread_log_fd, "[%s] RD LOCK %s %p thread_id=%lu at %s:%d\n", \
	__func__, #m, static_cast<void*>(std::addressof((m))), (unsigned long)pthread_self(), __FILE__, __LINE__); \
	fflush(thread_log_fd == nullptr ? stdout : thread_log_fd); \
	std::shared_lock<std::shared_mutex> lock(m); \
} while (0)

#define WR_LOCK(m) do { \
	fprintf(thread_log_fd == nullptr ? stdout : thread_log_fd, "[%s] WR LOCK %s %p thread_id=%lu at %s:%d\n", \
	__func__, #m, static_cast<void*>(std::addressof((m))), (unsigned long)pthread_self(), __FILE__, __LINE__); \
	fflush(thread_log_fd == nullptr ? stdout : thread_log_fd); \
	std::unique_lock<std::shared_mutex> lock(m); \
} while (0)

struct file_handler {
	Inode* inode;
	int flags;	
};

/* Global singletons */
StorageManager& storage = StorageManager::instance();
SuperblockManager& superblock = SuperblockManager::instance();
InodeManager& inode_manager = InodeManager::instance();
DataBlockManager& datablock_manager = DataBlockManager::instance();
DirentManager& dirent_manager = DirentManager::instance();

char diskfile_path[PATH_MAX];

/* Thead Logs Ops */
error_t thread_fd_open(const char* file) {
	int fd = open(file, O_CREAT | O_WRONLY | O_TRUNC);

	if (fd < 0) {
		perror("open");

		return errno;
	}

	thread_log_fd = fdopen(fd, "w");

	if (!thread_log_fd) {
		perror("fdopen");

		close(fd);

		return errno;
	}
}

error_t thread_fd_close() {
	if (thread_log_fd == nullptr) return 0;

	return fclose(thread_log_fd);
}

/* Lock Ops */
std::shared_mutex& get_lock_for_inode(ino_t ino) {
	return inode_rw_locks[ino % MAX_INODE_LOCKS];
}

/* FUSE Helper Funcs */
uid_t get_context_uid() {
	fuse_context* ctx = fuse_get_context();

	assert(ctx != nullptr);

	return ctx->uid;
}

gid_t get_context_gid() {
	fuse_context* ctx = fuse_get_context();

	assert(ctx != nullptr);

	return ctx->gid;
}

bool is_context_user_root() {
	return get_context_uid() == 0;
}

error_t make_file(const char* path, mode_t mode, Inode& out) {
	assert(path != nullptr);

	DBG("path: %s | mode: %05o", path, mode & 0x7777);

	uid_t uid = get_context_uid();
	gid_t gid = get_context_gid();

	Utilities::path_split ps = { 0 };

	if (Utilities::split_path(path, &ps) < 0) return -ENOMEM;

	char *base = ps.base;
	char *dir = ps.dir;

	if (strlen(base) > NAME_MAX) return -ENAMETOOLONG;

	DBG("Dir: %s | Base: %s", dir, base);

	// Get parent inode and check if the target file already exists
	Inode parent_inode;

	error_t err = inode_manager.get_inode_from_path(dir, ROOT_INO, parent_inode);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	// Acquire write lock on parent inode because we're about to add an entry to parent
	WR_LOCK(get_lock_for_inode(parent_inode.get_ino()));

	if (!parent_inode.is_valid()) {
		Utilities::free_path_split(&ps);

		return -ENOENT;
	}

	err = dirent_manager.dir_find(parent_inode.get_ino(), base, nullptr);

	if (err <= 0) {
		Utilities::free_path_split(&ps);

		if (err == 0) return -EEXIST;

		return err;
	}

	Utilities::free_path_split(&ps);

	// Check if user has permission to write into parent dir
	if (!parent_inode.can_write()) {
		return -EACCES;
	}

	// Get the next available inode number of this file
	ino_t new_ino;

	err = inode_manager.get_available_ino(new_ino);

	if (err < 0) {
		return err;
	}

	// Acquire write lock on this new file inode
	WR_LOCK(get_lock_for_inode(new_ino));

	if (S_ISFIFO(mode)) {
		out.from(new_ino, S_IFIFO & mode, 1, uid, gid);
	} else if (S_ISREG(mode)) {
		out.from(new_ino, S_IFREG & mode, 1, uid, gid);
	} else if (S_ISCHR(mode)) {
		out.from(new_ino, S_IFCHR & mode, 1, uid, gid);
	} else if (S_ISBLK(mode)) {
		out.from(new_ino, S_IFBLK & mode, 1, uid, gid);
	} else if (S_ISSOCK(mode)) {
		out.from(new_ino, S_IFSOCK & mode, 1, uid, gid);
	} else {
		return -EOPNOTSUPP;
	}

	// Update open count
	out.inode.open_count++;

	err = out.save();

	if (err < 0) return err;

	err = dirent_manager.dir_add(parent_inode, new_ino, base);

	if (err < 0) return err;

	DBG("Done.");

	return 0;
}

error_t read_file(Inode& finode, char* buffer, size_t size, off_t offset) {
	if (finode.is_dir()) return -EISDIR;

	if (!finode.is_valid()) return -EINVAL;

	size_t bytes_read = 0;
	off_t current_offset = offset;
	size_t remaining = size;

	size_t block_index, block_offset, to_read;
	blk_t reading_blk = 0;

	void* blk_buffer = malloc(StorageManager::BLOCK_SIZE);

	if (blk_buffer == nullptr) return -ENOMEM;

	blk_t* indirect_buffer = (blk_t*)malloc(StorageManager::BLOCK_SIZE);

	if (indirect_buffer == nullptr) {
		free(blk_buffer);

		return -ENOMEM;
	}

	error_t err;

	while (remaining > 0) {
		block_index = current_offset / StorageManager::BLOCK_SIZE;
		block_offset = current_offset % StorageManager::BLOCK_SIZE;
		to_read = StorageManager::BLOCK_SIZE - block_offset;

		if (to_read > remaining) to_read = remaining;

		if (block_index < DIRECT_PTRS_COUNT) {
			reading_blk = finode.get_block_direct_at(block_index);
		} else {
			// In indirect region
			uint32_t blk_idx = block_index - DIRECT_PTRS_COUNT;
			uint32_t num_blks_indirect = StorageManager::BLOCK_SIZE / sizeof(blk_t);

			assert(blk_idx < num_blks_indirect);

			if (finode.is_singly_indirect_allocated()) {
				// Read indirect data block
				err = finode.get_indirect_blk_data(indirect_buffer);

				if (err < 0) {
					free(blk_buffer);
					free(indirect_buffer);

					return bytes_read == 0 ? err : bytes_read;
				}

				reading_blk = indirect_buffer[blk_idx];
			}
		}

		if (reading_blk == 0) {
			// Reading into hole or unallocated region
			memset(buffer + bytes_read, 0, to_read);
		} else {
			err = storage.block_read(reading_blk, blk_buffer);

			if (err < 0) {
				free(blk_buffer);
				free(indirect_buffer);

				return bytes_read == 0 ? err : bytes_read;
			}

			memcpy(buffer + bytes_read, (char*)blk_buffer + block_offset, to_read);
		}

		bytes_read += to_read;
		current_offset += to_read;
		remaining -= to_read;
	}

	free(blk_buffer);
	free(indirect_buffer);

	// Update time
	finode.inode.atime = Utilities::now();

	err = finode.save();

	if (err < 0) return bytes_read == 0 ? err : bytes_read;

	return bytes_read;
}

error_t write_file(Inode& finode, const char* buffer, size_t size, off_t offset) {
	if (finode.is_dir()) return -EISDIR;

	if (!finode.is_valid()) return -EINVAL;

	size_t bytes_written = 0;
	off_t current_offset = offset;
	size_t remaining = size;

	size_t block_index, block_offset, to_write;
	blk_t writing_blk = 0;

	void* blk_buffer = malloc(StorageManager::BLOCK_SIZE);

	if (blk_buffer == nullptr) return -ENOMEM;

	int* indirect_buffer = (int*)malloc(StorageManager::BLOCK_SIZE);

	if (indirect_buffer == nullptr) {
		free(blk_buffer);
		
		return -ENOMEM;
	}

	error_t err;

	bool indirect_just_created = false;

	while (remaining > 0) {
		block_index = current_offset / StorageManager::BLOCK_SIZE;
		block_offset = current_offset % StorageManager::BLOCK_SIZE;
		to_write = StorageManager::BLOCK_SIZE - block_offset;

		if (to_write > remaining) to_write = remaining;

		if (block_index < DIRECT_PTRS_COUNT) {
			writing_blk = finode.get_block_direct_at(block_index);

			if (writing_blk == 0) {
				err = datablock_manager.get_available_blk(writing_blk);

				if (err < 0) {
					free(blk_buffer);
					free(indirect_buffer);

					return bytes_written == 0 ? err : bytes_written;
				}

				finode.inode.directs[block_index] = writing_blk;
			}
		} else {
			// In indirect region
			uint32_t blk_idx = block_index - DIRECT_PTRS_COUNT;
			uint32_t num_blks_indirect = StorageManager::BLOCK_SIZE / sizeof(blk_t);

			assert(blk_idx < num_blks_indirect);

			if (!finode.is_singly_indirect_allocated()) {
				// Indirect not yet allocated
				blk_t indirect_blk;

				err = datablock_manager.get_available_blk(indirect_blk);

				if (err < 0) {
					free(blk_buffer);
					free(indirect_buffer);

					return bytes_written == 0 ? err : bytes_written;
				}

				// Make sure every bytes are set to zero
				// Since the data block we took may contain some trunk data
				Utilities::fill_array<int>(indirect_buffer, 0, num_blks_indirect);

				err = storage.block_write(indirect_blk, indirect_buffer);

				if (err < 0) {
					free(blk_buffer);
					free(indirect_buffer);

					return bytes_written == 0 ? err : bytes_written;
				}

				finode.inode.singly_indirect_ptr = indirect_blk;

				indirect_just_created = true;
			}

			if (!indirect_just_created) {
				assert(finode.is_singly_indirect_allocated());

				err = storage.block_read(finode.inode.singly_indirect_ptr, indirect_buffer);

				if (err < 0) {
					free(blk_buffer);
					free(indirect_buffer);

					return bytes_written == 0 ? err : bytes_written;
				}
			}

			if (indirect_buffer[blk_idx] == 0) {
				// Allocate new block
				err = datablock_manager.get_available_blk(writing_blk);

				if (err < 0) {
					free(blk_buffer);
					free(indirect_buffer);

					return bytes_written == 0 ? err : bytes_written;
				}

				indirect_buffer[blk_idx] = writing_blk;
			}
		}

		assert(writing_blk != 0);

		err = storage.block_read(writing_blk, blk_buffer);

		if (err < 0) {
			free(blk_buffer);
			free(indirect_buffer);

			return bytes_written == 0 ? err : bytes_written;
		}

		if (to_write == StorageManager::BLOCK_SIZE) {
			// rewrite the whole block
			memcpy(blk_buffer, buffer + bytes_written, to_write);
		} else {
			// partial write
			memcpy((char*)blk_buffer + block_offset, buffer + bytes_written, to_write);
		}

		err = storage.block_write(writing_blk, blk_buffer);

		if (err < 0) {
			free(blk_buffer);
			free(indirect_buffer);

			return bytes_written == 0 ? err : bytes_written;
		}

		bytes_written += to_write;
		current_offset += to_write;
		remaining -= to_write;
	}

	free(blk_buffer);
	free(indirect_buffer);

	// Update file size in inode
	if (offset + bytes_written > finode.inode.size) {
		finode.inode.size = offset + bytes_written;
	}

	// Update time
	finode.inode.mtime = Utilities::now();

	err = finode.save();

	if (err < 0) return bytes_written == 0 ? err : bytes_written;

	return bytes_written;
}

/* --------------- FUSE OPERATIONS ------------------------ */
static void* myfs_init(struct fuse_conn_info *conn, struct fuse_config* fconfig) {
    assert(diskfile_path != nullptr);

    DBG("MYFS initializing...");

    // MUST HAVE THIS
	// OTHERWISE stat() will report different inode numbers for hard links even if they are the same in FS
	// Due to getattr will ignore st_ino field if use_ino is not given
	fconfig->use_ino = 1;

    DBG("Init storage");

    error_t err = storage.init(diskfile_path);

    if (err < 0) {
        perror("storage-init");

        exit(EXIT_FAILURE);
    }

    DBG("Init superblock");

    err = superblock.init();

    if (err < 0) {
        perror("superblock-init");

        exit(EXIT_FAILURE);
    }

    DBG("Init inode manager");

    err = inode_manager.init();

    if (err < 0) {
        perror("inode-init");

        exit(EXIT_FAILURE);
    }

    DBG("Init datablock manager");

    err = datablock_manager.init();

    if (err < 0) {
        perror("datablock-init");

        exit(EXIT_FAILURE);
    }

    DBG("Done.");

    return 0;
}

/**
 * Destroy MYFS
 */
static void myfs_destroy(void *userdata) {
	DBG("Closing...");

	storage.storage_close();

	thread_fd_close();

	DBG("Done.");
}

static int myfs_getattr(const char* path, struct stat *st_buf, struct fuse_file_info *fi) {
	DBG("path: %s\n", path);

	Inode finode;

	error_t err = inode_manager.get_inode_from_path(path, ROOT_INO, finode);

	if (err < 0) {
		perror("getattr");

		return err;
	}

	// Acquire read lock
	RD_LOCK(get_lock_for_inode(finode.get_ino()));

	if (!finode.is_valid()) {
		return -ENOENT;
	}

	st_buf->st_ino = finode.inode.ino;
	st_buf->st_size = finode.inode.size;
	st_buf->st_nlink = finode.inode.nlink;
   	st_buf->st_mode = finode.inode.mode;
	st_buf->st_uid = finode.inode.uid;
	st_buf->st_gid = finode.inode.gid;
	st_buf->st_atim = finode.inode.atime;
	st_buf->st_mtim = finode.inode.mtime;
	st_buf->st_ctim = finode.inode.ctime;

	DBG("Done.");

	return 0;
}

static int myfs_opendir(const char* path, struct fuse_file_info *fi) {
	DBG("path: %s", path);

	Inode* dir_inode = new Inode();

	error_t err = inode_manager.get_inode_from_path(path, ROOT_INO, *dir_inode);

	if (err < 0) return err;

	// Acquire read lock
	RD_LOCK(get_lock_for_inode(dir_inode->get_ino()));

	if (!dir_inode->is_valid()) return -ENOENT;

	// Check permissions to open dir
	int access_mode = ACCMODE_FROM_FLAG(fi->flags);
	int need_read = ACCMODE_REQUEST_READ(access_mode);
	int need_write = ACCMODE_REQUEST_WRITE(access_mode);

	// Check permission bit
	if (need_read && !dir_inode->can_read()) return -EACCES;

	if (need_write && !dir_inode->can_write()) return -EACCES;

	dir_inode->inode.open_count++;

	err = dir_inode->save();

	if (err < 0) return err;

	// Save inode number of this dir to fh struct
	file_handler* fh = (file_handler*)malloc(sizeof(file_handler));

	fh->inode = dir_inode;
	fh->flags = fi->flags;

	fi->fh = (uint64_t)fh;

	DBG("Done.");

	return 0;
}

static int myfs_readdir(const char* path, void* buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
	DBG("path: %s", path);

	// Check if inode is cached with opendir()
	if (fi->fh == 0) {
		perror("opendir is not called prior to this function call");

		return -EPERM;
	}

	file_handler* fh = (file_handler*)fi->fh;

	assert(fh != nullptr);

	Inode* dir_inode = fh->inode;

	// Acquire read lock
	RD_LOCK(get_lock_for_inode(dir_inode->get_ino()));

	if (!dir_inode->is_valid()) {
		dir_inode = nullptr;

		delete fh->inode;

		fh->inode = nullptr;

		return -ENOENT;
	}

	uint16_t num_dirent_per_block = DirentManager::NUM_DIRENT_PER_BLOCK;

	dirent_t* dirent_buffer = (dirent_t*)malloc(StorageManager::BLOCK_SIZE);

	if (dirent_buffer == nullptr) return -ENOMEM;

	dirent_t entry = { 0 };

	// Read from direct first
	uint16_t blk_idx = 0;
	blk_t blk;
	error_t err;

	for (; blk_idx < DIRECT_PTRS_COUNT; ++blk_idx) {
		if (!dir_inode->is_direct_allocated_at(blk_idx)) continue;

		blk = dir_inode->get_block_direct_at(blk_idx);

		err = storage.block_read(blk, buffer);

		if (err < 0) {
			free(dirent_buffer);

			return err;
		}

		for (uint16_t i = 0; i < num_dirent_per_block; ++i) {
			entry = dirent_buffer[i];

			if (entry.valid == 1) {
				if (filler(buffer, entry.name, nullptr, 0, (fuse_fill_dir_flags)0) != 0) {
					free(dirent_buffer);

					return -ENOMEM;
				}
			}
		}
	}

	// TODO: read from indirect
	free(dirent_buffer);

	DBG("Done.");

	return 0;
}

static int myfs_mkdir(const char* path, mode_t mode) {
	DBG("path: %s | mode: %05o", path, mode & 07777);

	uid_t uid = get_context_uid();
	gid_t gid = get_context_gid();

	Utilities::path_split ps = { 0 };

	if (Utilities::split_path(path, &ps) < 0) {
		return -ENOMEM;
	}

	char *dir = ps.dir;
	char *base = ps.base;

	DBG("Dir: %s | Base: %s", dir, base);

	Inode parent_inode;

	error_t err = inode_manager.get_inode_from_path(dir, ROOT_INO, parent_inode);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	// Acquire write lock on parent inode
	WR_LOCK(get_lock_for_inode(parent_inode.get_ino()));

	if (!parent_inode.is_valid()) {
		Utilities::free_path_split(&ps);

		return -ENOENT;
	}

	// Check if the target directory already exists
	err = dirent_manager.dir_find(parent_inode.get_ino(), base, nullptr);

	if (err <= 0) {
		Utilities::free_path_split(&ps);

		if (err == 0) return -EEXIST;	

		return err;
	}

	// Check if user has write permission since we have to change parent dirent
	if (!parent_inode.can_write()) {
		Utilities::free_path_split(&ps);

		return -EACCES;
	}

	// Get the next available inode number from this directory
	ino_t new_ino;

	err = inode_manager.get_available_ino(new_ino);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		if (err == -1) return -ENOSPC;

		return err;
	}

	// Acquire lock on this new directory to write
	WR_LOCK(get_lock_for_inode(new_ino));

	DBG("Create new directory inode with ino: %ld", new_ino);

	Inode new_dir_inode{new_ino, S_IFDIR & mode, 2, uid, gid};

	err = new_dir_inode.save();

	DBG("Saved new directory inode");

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	// Add "." self
	err = dirent_manager.dir_add(new_dir_inode, new_ino, ".");

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	DBG("Added '.' entry to new dir inode: %ld", new_ino);

	err = dirent_manager.dir_add(new_dir_inode, parent_inode, "..");

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	DBG("Added '..' entry (%ld) to new dir inode: %ld", parent_inode->get_ino(), new_ino);

	err = dirent_manager.dir_add(parent_inode, new_dir_inode, base);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;	
	}

	DBG("Added entry of ino: %ld to parent dir (%ld)", new_dir_inode->get_ino(), parent_inode->get_ino());

	// Update nlink of parent inode
	parent_inode.inode.nlink++;

	err = parent_inode.save();

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;	
	}

	DBG("Updated nlink of parent inode (%ld) to nlink = %u", parent_inode->get_ino(), parent_inode->inode.nlink);

	Utilities::free_path_split(&ps);

	DBG("Done.");

	return 0;
}

static int myfs_create(const char* path, mode_t mode, struct fuse_file_info* fi) {
	DBG("path: %s | mode: %05o", path, mode & 0x7777);

	Inode* finode = new Inode();

	error_t err = make_file(path, mode, *finode);

	if (err < 0) {
		delete finode;

		return err;
	}

	// Save inode into cache
	file_handler* fh = (file_handler*)malloc(sizeof(file_handler));

	if (fh == nullptr) {
		delete finode;

		return -ENOMEM;
	}

	fh->inode = finode;
	fh->flags = fi->flags;

	fi->fh = (uint64_t)fh;

	DBG("Done.");

	return 0;
}

static int myfs_open(const char* path, struct fuse_file_info *fi) {
	DBG("path: %s", path);

	Inode* finode = new Inode();

	error_t err = inode_manager.get_inode_from_path(path, ROOT_INO, *finode);

	if (err < 0) return err;

	// Acquire write lock on this finode because we change its opencount
	WR_LOCK(get_lock_for_inode(finode->get_ino()));

	if (!finode->is_valid()) {
		delete finode;
		
		return -ENOENT;
	}

	// Check permissions
	int access_mode = ACCMODE_FROM_FLAG(fi->flags);
	int need_read = ACCMODE_REQUEST_READ(access_mode);
	int need_write = ACCMODE_REQUEST_WRITE(access_mode);

	if (need_read && !finode->can_read()) {
		delete finode;

		return -EACCES;
	}

	if (need_write && !finode->can_write()) {
		delete finode;

		return -EACCES;
	}

	// Update opencount
	finode->inode.open_count++;

	err = finode->save();

	if (err < 0) {
		delete finode;

		return err;
	}

	// Save inode to cache
	file_handler* fh = (file_handler*)malloc(sizeof(file_handler));

	if (fh == nullptr) {
		delete finode;

		return -ENOMEM;
	}

	fh->inode = finode;
	fh->flags = fi->flags;

	fi->fh = (uint64_t)fh;

	DBG("Done.");

	return 0;
}

static int myfs_read(const char* path, char* buffer, size_t size, off_t offset, struct fuse_file_info* fi) {
	DBG("path: %s | size: %zu | offset: %ld", path, size, offset);

	if (fi->fh == 0) {
		perror("open() is not called prior to this function call");

		return -EPERM;
	}

	file_handler* fh = (file_handler*)fi->fh;

	assert(fh != nullptr);

	Inode* finode = fh->inode;

	assert(finode != nullptr);

	// Acquire write lock on this file inode
	WR_LOCK(get_lock_for_inode(finode->get_ino()));
	
	if (!finode->is_valid()) {
		finode = nullptr;

		delete fh->inode;

		fh->inode = nullptr;

		return -ENOENT;
	}

	if (finode->is_dir()) {
		finode = nullptr;

		delete fh->inode;

		fh->inode = nullptr;

		return -EISDIR;
	}

	error_t res = read_file(*finode, buffer, size, offset);

	if (res < 0) return res;

	DBG("Bytes read: %zu", res);

	DBG("Done.\n");

	return res;
}

static int myfs_write(const char* path, const char* buffer, size_t size, off_t offset, struct fuse_file_info* fi) {
	DBG("path: %s | size: %zu | offset: %ld", path, size, offset);

	file_handler* fh = (file_handler*)fi->fh;

	assert(fh != nullptr);

	Inode* finode = fh->inode;

	assert(finode != nullptr);

	// Acquire read lock on this file inode
	WR_LOCK(get_lock_for_inode(finode->get_ino()));
	
	if (!finode->is_valid()) {
		finode = nullptr;

		delete fh->inode;

		fh->inode = nullptr;

		return -ENOENT;
	}

	if (finode->is_dir()) {
		finode = nullptr;

		delete fh->inode;

		fh->inode = nullptr;

		return -EISDIR;
	}

	error_t res = write_file(*finode, buffer, size, offset);

	if (res < 0) return res;

	DBG("Bytes written: %zu", res);

	DBG("Done.");

	return res;
}

static off_t myfs_lseek(const char* path, off_t off, int whence, struct fuse_file_info *fi) {
	return -ENOTSUP;
}

static int myfs_fsync(const char* path, int datasync, struct fuse_file_info* fi) {
	return storage.storage_fsync();
}

static int myfs_rename(const char* source_path, const char* target_path, unsigned int flag) {
	DBG("source: %s | target: %s | flags = %u", source_path, target_path, flag);

	Utilities::path_split source_ps = { 0 };
	Utilities::path_split target_ps = { 0 };

	if (Utilities::split_path(source_path, &source_ps) < 0) return -ENOMEM;
	if (Utilities::split_path(target_path, &target_ps) < 0) {
		Utilities::free_path_split(&source_ps);

		return -ENOMEM;
	}

	char* source_dir = source_ps.dir;
	char* source_base = source_ps.base;

	char* target_dir = target_ps.dir;
	char* target_base = target_ps.base;

	if (IS_STR_EMPTY(source_base) || IS_STR_EMPTY(target_base)) {
		Utilities::free_path_split(&source_ps);
		Utilities::free_path_split(&target_ps);

		return -EINVAL;
	}

	// If mv refers the same source and target within the same directory, then we do nothing
	if (strcmp(source_dir, target_dir) == 0 && strcmp(source_base, target_base) == 0) return 0;

	// Look up parents of both source and target
	Inode source_parent_inode, target_parent_inode, source_inode, target_inode;

	dirent_t source_entry = { 0 }, target_entry = { 0 };

	error_t err = inode_manager.get_inode_from_path(source_dir, ROOT_INO, source_parent_inode);

	if (err < 0) {
		Utilities::free_path_split(&source_ps);
		Utilities::free_path_split(&target_ps);

		return err;
	}

	// Obtain write lock on working inodes
	WR_LOCK(get_lock_for_inode(source_parent_inode.get_ino()));

	// Check if source parent is a valid directory
	if (!source_parent_inode.is_dir() || !source_parent_inode.is_valid()) {
		Utilities::free_path_split(&source_ps);
		Utilities::free_path_split(&target_ps);
		
		return -ENOTDIR;
	}

	err = inode_manager.get_inode_from_path(target_dir, ROOT_INO, target_parent_inode);

	if (err < 0) {
		Utilities::free_path_split(&source_ps);
		Utilities::free_path_split(&target_ps);

		return err;
	}

	// In case of two parents are the same (mv within the same directory)
	// Having another lock on target directory will introduce deadlock
	// So, only lock when two parent inodes are different
	if (target_parent_inode.get_ino() != source_parent_inode.get_ino()) {
		WR_LOCK(get_lock_for_inode(target_parent_inode.get_ino()));
	}

	// Check if the target parent is a valid directory
	if (!target_parent_inode.is_dir() || !target_parent_inode.is_valid()) {
		Utilities::free_path_split(&source_ps);
		Utilities::free_path_split(&target_ps);

		return -ENOTDIR;
	}

	// Check permissions
	// Since we want to write into source parent inode, thus we need write and execute permissions
	if (!source_parent_inode.can_execute() || !source_parent_inode.can_write()) {
		Utilities::free_path_split(&source_ps);
		Utilities::free_path_split(&target_ps);

		return -EACCES;
	}

	// Check the same thing on target parent inode
	if (!target_parent_inode.can_execute() || !target_parent_inode.can_write()) {
		Utilities::free_path_split(&source_ps);
		Utilities::free_path_split(&target_ps);

		return -EACCES;	
	}

	// Find source entry
	err = dirent_manager.dir_find(source_parent_inode.get_ino(), source_base, &source_entry);

	if (err < 0) {
		Utilities::free_path_split(&source_ps);
		Utilities::free_path_split(&target_ps);

		return err;
	} 

	// Obtain source inode
	err = inode_manager.get_inode(source_entry.ino, source_inode);

	if (err < 0) {
		Utilities::free_path_split(&source_ps);
		Utilities::free_path_split(&target_ps);

		return err;
	}

	// Acquire write lock on source inode
	WR_LOCK(get_lock_for_inode(source_inode.get_ino()));

	if (source_parent_inode.is_dir_sticky()) {
		if (
			!is_context_user_root() &&							// user is not the root user
			!source_parent_inode.user_is_owner() &&		// user does not own the source directory
			!source_inode.user_is_owner()				// user does not own the source item
		) {
			Utilities::free_path_split(&source_ps);
			Utilities::free_path_split(&target_ps);

			return -EPERM;
		}
	}

	// Lookup destination
	bool target_already_exist = false;

	err = dirent_manager.dir_find(target_parent_inode.get_ino(), target_base, &target_entry);

	if (err < 0) {
		if (err != -ENOENT) {
			Utilities::free_path_split(&source_ps);
			Utilities::free_path_split(&target_ps);

			return err;
		}

		target_already_exist = false;
	} else {
		target_already_exist = true;
	}

	if (target_already_exist) {
		assert(target_entry.ino != 0);

		// Obtain lock on target
		// Since it could be possible that source inode and target inode are the same
		// maybe target inode is a hard link to source inode
		// obtain the same lock twice will possibly introduce deadlock
		if (target_entry.ino != source_inode.get_ino()) {
			WR_LOCK(get_lock_for_inode(target_entry.ino));
		}

		err = inode_manager.get_inode(target_entry.ino, target_inode);

		if (err < 0) {
			Utilities::free_path_split(&source_ps);
			Utilities::free_path_split(&target_ps);

			return err;
		}

		// Check for sticky behavior on the target directory
		if (target_parent_inode.is_dir_sticky()) {
			if (
				!is_context_user_root() &&							// user is not the root user
				!target_parent_inode.user_is_owner() &&		// user does not own the target directory
				!target_inode.user_is_owner()				// user does not own the target item
			) {
				Utilities::free_path_split(&source_ps);
				Utilities::free_path_split(&target_ps);

				return -EPERM;
			}
		}

		// Check for compatibility
		// you can't rename a file into a directory or vice versa
		if (source_inode.is_dir() && !target_inode.is_dir()) {
			Utilities::free_path_split(&source_ps);
			Utilities::free_path_split(&target_ps);

			return -ENOTDIR;
		}

		if (!source_inode.is_dir() && target_inode.is_dir()) {
			Utilities::free_path_split(&source_ps);
			Utilities::free_path_split(&target_ps);

			return -EISDIR;
		}

		// Since if we want to replace a directory, the target directory must be empty
		if (target_inode.is_dir() && !target_inode.is_dir_empty()) {
			Utilities::free_path_split(&source_ps);
			Utilities::free_path_split(&target_ps);

			return -ENOTEMPTY;
		}
	}

	// For source entry
	// If source entry is a directory, we can't move the source entry (a dir) into its own subtree
	if (source_inode.is_dir()) {
		// Ex: c has structure like this
		// c:
		// 	--> b
		//	--> d/e
		// we can't do "rename c -> c/d/e/<new_c>"
		if (source_parent_inode.get_ino() != target_parent_inode.get_ino()) {
			if (dirent_manager.is_descendant(target_parent_inode, source_inode)) {
				Utilities::free_path_split(&source_ps);
				Utilities::free_path_split(&target_ps);

				return -EINVAL;
			}
		}
	}

	// If rename cross-directory, user must own the source directory (in case of directory)
	if (source_inode.is_dir() && source_parent_inode.get_ino() != target_parent_inode.get_ino()) {
		if (
			!is_context_user_root() &&					// user is not the root user
			!source_inode.user_is_owner()				// user does not own the source item
		) {
			Utilities::free_path_split(&source_ps);
			Utilities::free_path_split(&target_ps);

			return -EACCES;
		}
	}

	// All checks are good, perform rename
	if (target_already_exist) {
		assert(target_inode.get_ino() != 0);

		// Rename target entry from target directory
		err = dirent_manager.dir_remove(target_parent_inode, target_base);

		if (err < 0) {
			Utilities::free_path_split(&source_ps);
			Utilities::free_path_split(&target_ps);

			return err;
		}

		// Update nlink of target inode
		target_inode.inode.nlink--;

		err = target_inode.save();

		if (err < 0) {
			Utilities::free_path_split(&source_ps);
			Utilities::free_path_split(&target_ps);

			return err;
		}

		if (target_inode.should_file_be_deleted()) {
			// Remove inode
			err = target_inode.release();

			if (err < 0) {
				Utilities::free_path_split(&source_ps);
				Utilities::free_path_split(&target_ps);

				return err;
			}
		}
	}

	// Remove source entry from source parent
	err = dirent_manager.dir_remove(source_parent_inode, source_base);

	if (err < 0) {
		Utilities::free_path_split(&source_ps);
		Utilities::free_path_split(&target_ps);	

		return err;
	}

	// Add new entry into target parent
	err = dirent_manager.dir_add(target_parent_inode, source_inode.get_ino(), target_base);

	if (err < 0) {
		Utilities::free_path_split(&source_ps);
		Utilities::free_path_split(&target_ps);

		return err;
	}

	// If cross-directory rename
	// then update ".." to point to target dir
	if (source_inode.is_dir() && source_parent_inode.get_ino() != target_parent_inode.get_ino()) {
		err = dirent_manager.dir_update_dotdot(source_inode, target_parent_inode);

		if (err < 0) {
			Utilities::free_path_split(&source_ps);
			Utilities::free_path_split(&target_ps);

			return err;
		}
	}
	
	Utilities::free_path_split(&source_ps);
	Utilities::free_path_split(&target_ps);

	return 0;
}

static int myfs_rmdir(const char* path) {
	if (strcmp(path, "/") == 0) {
		// Cannot remove root directory
		return -EBUSY;
	}

	Utilities::path_split ps = { 0 };

	if (Utilities::split_path(path, &ps) < 0) return -ENOMEM;

	char *base = ps.base;
	char *dir = ps.dir;

	if (IS_STR_EMPTY(base)) {
		Utilities::free_path_split(&ps);

		return -EINVAL;
	}

	Inode dir_inode, parent_inode;

	error_t err = inode_manager.get_inode_from_path(path, ROOT_INO, dir_inode);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	// Acquire write lock on dir inode
	WR_LOCK(get_lock_for_inode(dir_inode.get_ino()));

	// Check if this is a directory
	if (!dir_inode.is_dir()) {
		Utilities::free_path_split(&ps);

		return -ENOTDIR;
	}

	// Check if the directory is empty
	// Can only remove an empty directory
	if (!dir_inode.is_dir_empty()) {
		Utilities::free_path_split(&ps);

		return -ENOTEMPTY;
	}

	err = inode_manager.get_inode_from_path(dir, ROOT_INO, parent_inode);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	// Acquire write lock on parent
	WR_LOCK(get_lock_for_inode(parent_inode.get_ino()));

	// Check if user has permission on parent dir to remove (write permission)
	if (!parent_inode.can_write()) {
		Utilities::free_path_split(&ps);

		return -EACCES;
	}

	// For sticky behavior on parent dir
	// Only owner of the entry being deleted or owner of the parent dir or root can perform rmdir
	if (parent_inode.is_dir_sticky()) {
		if (
			!is_context_user_root() &&				// user is not the root user
			!parent_inode.user_is_owner() &&		// user does not own the parent directory
			!dir_inode.user_is_owner()				// user does not own the item being removed
		) {
			Utilities::free_path_split(&ps);

			return -EPERM;
		}
	}

	// Remove dir entry from parent
	err = dirent_manager.dir_remove(parent_inode, base);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	Utilities::free_path_split(&ps);

	// Update parent nlink
	assert(parent_inode.inode.nlink > 0);

	parent_inode.inode.nlink--;

	err = parent_inode.save();

	if (err < 0) {
		return err;
	}

	// release dir inode back to the system
	err = dir_inode.release();

	if (err < 0) {
		return err;
	}

	return 0;
}

static int myfs_releasedir(const char* path, struct fuse_file_info *fi) {
	// Check if inode is cached with opendir()
	if (fi->fh == 0) {
		perror("opendir is not called prior to this function call");

		return -EPERM;
	}

	file_handler* fh = (file_handler*)fi->fh;

	assert(fh != nullptr);

	Inode* dir_inode = fh->inode;

	// Acquire write lock on dir inode
	WR_LOCK(get_lock_for_inode(dir_inode->get_ino()));

	assert(dir_inode->inode.open_count > 0);

	dir_inode->inode.open_count--;

	error_t err = dir_inode->save();
	
	delete fh->inode;

	free((void*)fi->fh);

	fi->fh = 0;

	if (err < 0) {
		return err;
	}

	return 0;
}

static int myfs_unlink(const char* path) {
	Inode finode, parent_inode;

	error_t err = inode_manager.get_inode_from_path(path, ROOT_INO, finode);

	if (err < 0) {
		return err;
	}

	// Acquire write lock on finode
	WR_LOCK(get_lock_for_inode(finode.get_ino()));

	// Check if this is a directory
	// unlink on directory is not permitted
	if (finode.is_dir()) return -EPERM;

	Utilities::path_split ps = { 0 };

	if (Utilities::split_path(path, &ps) < 0) return -ENOMEM;

	char* base = ps.base;
	char* dir = ps.dir;

	err = inode_manager.get_inode_from_path(dir, ROOT_INO, parent_inode);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	// Acquire write lock on parent
	WR_LOCK(get_lock_for_inode(parent_inode.get_ino()));

	// Check if user has permission to unlink (write)
	if (!parent_inode.can_write()) {
		Utilities::free_path_split(&ps);

		return -EACCES;
	}

	if (parent_inode.is_dir_sticky()) {
		if (
			is_context_user_root() &&			// user is not the root user
			!parent_inode.user_is_owner() &&	// user does not own parent directory
			!finode.user_is_owner()				// user does not own item being unlinked
		) {
			Utilities::free_path_split(&ps);

			return -EACCES;
		}
	}

	// Remove file entry from parent dir
	err = dirent_manager.dir_remove(parent_inode, base);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	Utilities::free_path_split(&ps);

	// Update nlink of finode
	assert(finode.inode.nlink > 0);

	finode.inode.nlink--;

	err = finode.save();

	if (err < 0) {
		return err;
	}

	// Check if the file inode should be released
	if (finode.should_file_be_deleted()) {
		err = finode.release();

		if (err < 0) {
			return err;
		}
	}
	
	return 0;
}

static int myfs_truncate(const char *path, off_t size, struct fuse_file_info *fi) {
	Inode* finode = nullptr;
	error_t err;
	file_handler* fh = nullptr;
	bool should_deleted = false;

	if (fi != nullptr && fi->fh != 0) {
		file_handler* fh = (file_handler*)fi->fh;

		assert(fh != nullptr);

		finode = fh->inode;
	} else {
		finode = new Inode();

		should_deleted = true;

		err = inode_manager.get_inode_from_path(path, ROOT_INO, *finode);
		
		if (err < 0) {
			return err;
		}
	}

	assert(finode != nullptr);

	// Acquire write lock on finode
	WR_LOCK(get_lock_for_inode(finode->get_ino()));

	// Check if inode is a directory
	if (finode->is_dir()) {
		if (should_deleted) delete finode;

		return -EISDIR;
	}

	// Check if the file is opened through open()
	if (fh != nullptr) {
		int access_mode = ACCMODE_FROM_FLAG(fh->flags);
		
		// Check if user specify flags that allow file to be written
		bool can_write = ACCMODE_REQUEST_WRITE(access_mode);

		if (!can_write) {
			// don't delete finode here as fi->finode stil carries it
			// finode will be deleted in myfs_release()
			return -EACCES;
		}
	} else {
		// Check if user has write permission on finode
		if (!finode->can_write()) {
			// finode is allocated locally
			if (should_deleted) delete finode;

			return -EACCES;
		}
	}
	
	// Maximum file size myfs allows
	uint32_t MAX_SIZE = DIRECT_PTRS_COUNT * StorageManager::BLOCK_SIZE + (StorageManager::BLOCK_SIZE / sizeof(blk_t)) * StorageManager::BLOCK_SIZE;

	if (size >= MAX_SIZE) {
		if (should_deleted) delete finode;

		return -EFBIG;
	}

	if (finode->inode.size == size) {
		// Do nothing since file size already equals to the requested size
		// Only update atime
		finode->inode.atime = Utilities::now();

		err = finode->save();

		if (err < 0) {
			if (should_deleted) delete finode;

			return err;
		}

		return 0;
	}

	if (finode->inode.size == 0 || finode->inode.size < size) {
		// 0 or expanding
		// simply set file size to size
		// I no need to allocate block here as it will be lazily allocated
		finode->inode.size = size;

		// Update time
		finode->inode.atime = Utilities::now();
		finode->inode.mtime = Utilities::now();

		err = finode->save();

		if (err < 0) {
			if (should_deleted) delete finode;

			return err;
		}

		return 0;
	}

	// Shrinking case
	uint32_t start_removed_blk_offset = size % StorageManager::BLOCK_SIZE;
	uint32_t start_removed_blk_idx = size / StorageManager::BLOCK_SIZE;
	uint32_t end_removed_blk_idx = finode->inode.size / StorageManager::BLOCK_SIZE;	

	// If offset is 0, it means we gonna include removing the start block
	start_removed_blk_idx = (start_removed_blk_offset == 0) ? start_removed_blk_idx : start_removed_blk_idx + 1;

	blk_t* indirect_buf = NULL;
	uint16_t blk_off;
	uint16_t num_blk_per_indirect = StorageManager::BLOCK_SIZE / sizeof(blk_t);

	for (uint16_t blk_idx = start_removed_blk_idx; blk_idx <= end_removed_blk_idx; ++blk_idx) {
		if (blk_idx < DIRECT_PTRS_COUNT) {
			if (!finode->is_direct_allocated_at(blk_idx)) continue;

			err = datablock_manager.release_data_block(finode->get_block_direct_at(blk_idx));

			if (err < 0) {
				if (should_deleted) delete finode;
				if (indirect_buf != nullptr) free(indirect_buf);

				return err;
			}
		} else {
			if (!finode->is_singly_indirect_allocated()) {
				// The data blocks are in indirect region but it is unallocated
				// so there is no need to perform releasing data block
				break;
			}

			blk_off = blk_idx - DIRECT_PTRS_COUNT;

			if (indirect_buf == nullptr) {
				indirect_buf = (blk_t*)malloc(StorageManager::BLOCK_SIZE);

				if (indirect_buf == nullptr) {
					if (should_deleted) delete finode;

					return -ENOMEM;
				}
			}

			err = finode->get_indirect_blk_data(indirect_buf);

			if (err < 0) {
				if (should_deleted) delete finode;
				if (indirect_buf != nullptr) free(indirect_buf);

				return err;
			}

			if (indirect_buf[blk_off] == 0) continue;

			err = datablock_manager.release_data_block(indirect_buf[blk_off]);

			if (err < 0) {
				if (should_deleted) delete finode;
				if (indirect_buf != nullptr) free(indirect_buf);

				return err;
			}
		}
	}

	if (indirect_buf != nullptr) free(indirect_buf);

	// TODO: should do truncate in two cases (in direct and in indirect region)

	// Update size
	finode->inode.size = size;

	// Update time
	finode->inode.atime = Utilities::now();
	finode->inode.mtime = Utilities::now();

	err = finode->save();

	if (err < 0) {
		if (should_deleted) delete finode;
		
		return err;
	}

	return 0;
}

static int myfs_flush(const char* path, struct fuse_file_info *fi) {
	return 0;
}

static int myfs_utimens(const char* path, const struct timespec tv[2], struct fuse_file_info* fi) {
	Inode* finode = nullptr;
	error_t err;
	file_handler* fh = nullptr;
	bool should_deleted = false;

	if (fi != nullptr && fi->fh != 0) {
		fh = (file_handler*)fi->fh;

		assert(fh != nullptr);

		finode = fh->inode;
	} else {
		finode = new Inode();
		
		should_deleted = true;

		err = inode_manager.get_inode_from_path(path, ROOT_INO, *finode);
	}
	
	// Acquire write lock on finode
	WR_LOCK(get_lock_for_inode(finode->get_ino()));

	bool can_write = false;
	bool owner_or_root = finode->user_is_owner() || is_context_user_root();

	if (fh != nullptr) {
		int access_mode = ACCMODE_FROM_FLAG(fh->flags);
		can_write = ACCMODE_REQUEST_WRITE(access_mode);
	} else {
		can_write = finode->can_write();
	}

	timespec now;

	clock_gettime(CLOCK_REALTIME, &now);

	// If times is NULL or both times are NOW
	// allowed if owner or root
	if (tv == nullptr) {
		if (!owner_or_root && !can_write) {
			if (should_deleted) delete finode;

			return -EACCES;
		}

		finode->inode.atime = now;
		finode->inode.mtime = now;
	} else {
		// Check if explicit bits exist
		// mean user want to specify some arbitrary time
		bool explicit_represent = false;

		for (uint8_t i = 0; i <= 1; ++i) {
			if (tv[i].tv_nsec != UTIME_OMIT && tv[i].tv_nsec != UTIME_NOW) {
				explicit_represent = true;
			}
		}

		if (!explicit_represent) {
			if (!owner_or_root && !can_write) {
				if (should_deleted) delete finode;
				
				return -EACCES;
			}

			if (tv[0].tv_nsec == UTIME_NOW) finode->inode.atime = now;
			if (tv[1].tv_nsec == UTIME_NOW) finode->inode.mtime = now;
		} else {
			if (!owner_or_root) {
				if (should_deleted) delete finode;
				
				return -EACCES;
			}

			
			// Update atime
			if (tv[0].tv_nsec == UTIME_NOW) {
				finode->inode.atime = now;
			} else if (tv[0].tv_nsec != UTIME_OMIT) {
				finode->inode.atime = tv[0];
			}
			
			// Update mtime
			if (tv[1].tv_nsec == UTIME_NOW) {
				finode->inode.mtime = now;
			} else if (tv[1].tv_nsec != UTIME_OMIT) {
				finode->inode.mtime = tv[1];
			}
		}
	}

	err = finode->save();

	if (err < 0) {
		if (should_deleted) delete finode;

		return err;
	}

	return 0;
}

static int myfs_release(const char* path, struct fuse_file_info *fi) {
	// Check if inode is cached with opendir()
	if (fi->fh == 0) {
		perror("open() is not called prior to this function call");

		return -EPERM;
	}

	file_handler* fh = (file_handler*)fi->fh;

	assert(fh != nullptr);

	Inode* inode = fh->inode;

	// Acquire write lock on dir inode
	WR_LOCK(get_lock_for_inode(inode->get_ino()));

	assert(inode->inode.open_count > 0);

	inode->inode.open_count--;

	error_t err = inode->save();
	
	delete fh->inode;

	free((void*)fi->fh);

	fi->fh = 0;

	if (err < 0) {
		return err;
	}

	return 0;
}

static int myfs_fallocate(const char* path, int mode, off_t offset, off_t len, struct fuse_file_info* fi) {
	return 0;
}

static ssize_t myfs_copy_file_range(
	const char* path_in, 
	struct fuse_file_info* fi_in, 
	off_t offset_in, 
	const char* path_out, 
	struct fuse_file_info *fi_out, 
	off_t offset_out, 
	size_t size, 
	int flags
) {
	return -EOPNOTSUPP;
}

static int myfs_flock(const char* path, struct fuse_file_info *fi, int op) {
	return -EOPNOTSUPP;
}

static int myfs_symlink(const char* target, const char* link) {
	Utilities::path_split ps = { 0 };

	if (Utilities::split_path(link, &ps) > 0) {
		return -ENOMEM;
	}

	char *base = ps.base;
	char *dir = ps.dir;

	Inode parent_inode;

	error_t err = inode_manager.get_inode_from_path(dir, ROOT_INO, parent_inode);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	WR_LOCK(get_lock_for_inode(parent_inode.get_ino()));

	if (!parent_inode.is_valid()) {
		Utilities::free_path_split(&ps);

		return -ENOENT;
	}

	// Check if the target already exists
	err = dirent_manager.dir_find(parent_inode.get_ino(), base, nullptr);

	if (err <= 0) {
		Utilities::free_path_split(&ps);
		
		if (err == 0) return -EEXIST;

		return err;
	}

	ino_t new_ino;

	err = inode_manager.get_available_ino(new_ino);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	WR_LOCK(get_lock_for_inode(new_ino));

	if (!parent_inode.can_write()) {
		Utilities::free_path_split(&ps);

		return -EACCES;
	}

	Inode new_file_inode{new_ino, S_IFLNK & 0755, 1, get_context_uid(), get_context_gid()};

	err = new_file_inode.save();

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	err = dirent_manager.dir_add(parent_inode, new_ino, base);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	// Write path to symlink
	size_t buffer_size = strlen(target) + 1;

	if (buffer_size > StorageManager::BLOCK_SIZE) {
		Utilities::free_path_split(&ps);

		return -ENOSPC;
	}

	err = write_file(new_file_inode, target, buffer_size, 0);

	if (err < 0) {
		Utilities::free_path_split(&ps);

		return err;
	}

	Utilities::free_path_split(&ps);

	return 0;
}

static int myfs_link(const char* target, const char* link) {
	Utilities::path_split link_ps = { 0 };

	if (Utilities::split_path(link, &link_ps) < 0) return -ENOMEM;

	char* base = link_ps.base;
	char* dir = link_ps.dir;

	Inode parent_inode, target_inode;
	
	error_t err = inode_manager.get_inode_from_path(dir, ROOT_INO, parent_inode);

	if (err < 0) {
		Utilities::free_path_split(&link_ps);

		return err;
	}

	WR_LOCK(get_lock_for_inode(parent_inode.get_ino()));

	if (!parent_inode.is_valid()) {
		Utilities::free_path_split(&link_ps);

		return -ENOENT;
	}

	if (!parent_inode.can_write()) {
		Utilities::free_path_split(&link_ps);
		
		return -EACCES;
	}

	err = inode_manager.get_inode_from_path(target, ROOT_INO, target_inode);

	if (err < 0) {
		Utilities::free_path_split(&link_ps);

		return err;
	}

	WR_LOCK(get_lock_for_inode(target_inode.get_ino()));

	err = dirent_manager.dir_add(parent_inode, target_inode.get_ino(), base);

	if (err < 0) {
		Utilities::free_path_split(&link_ps);

		return err;
	}

	// Update target nlink
	target_inode.inode.nlink++;

	err = target_inode.save();

	if (err < 0) {
		Utilities::free_path_split(&link_ps);

		return err;
	}

	Utilities::free_path_split(&link_ps);

	return 0;
}

static int myfs_readlink(const char* link, char* buffer, size_t len) {
	Inode link_inode;

	error_t err = inode_manager.get_inode_from_path(link, ROOT_INO, link_inode);

	if (err < 0) {
		return err;
	}

	RD_LOCK(get_lock_for_inode(link_inode.get_ino()));

	if (!S_ISLNK(link_inode.inode.mode)) {
		return -EINVAL;
	}

	if (link_inode.get_block_direct_at(0) == 0) {
		// Empty link
		return -EINVAL;
	}

	err = read_file(link_inode, buffer, link_inode.inode.size, 0);

	if (err < 0) {
		return err;
	}

	buffer[link_inode.inode.size] = '\0';

	return 0;
}

static int myfs_mknod(const char* path, mode_t mode, dev_t dev) {
	Inode finode;

	error_t err = make_file(path, mode, finode);

	if (err < 0) {
		return err;
	}

	return 0;
}

static int myfs_access(const char* path, int mode) {
	Inode finode;

	error_t err = inode_manager.get_inode_from_path(path, ROOT_INO, finode);

	if (err < 0) {
		return err;
	}

	RD_LOCK(get_lock_for_inode(finode.get_ino()));

	if (!finode.is_valid()) {
		return -ENOENT;
	}

	if (mode == F_OK) {
		// User wants to know if the file exists
		return 0;
	}

	switch (mode) {
		case R_OK:
			// User wants to know if they can read this file
			return finode.can_read();
		case W_OK:
			// User wants to know if they can write this file
			return finode.can_write();
		case X_OK:
			// User wants to know if they can execute this file
			return finode.can_execute();
	}

	return -EINVAL;
}

static int myfs_chmod(const char* path, mode_t mode, struct fuse_file_info *fi) {
	Inode finode;

	error_t err = inode_manager.get_inode_from_path(path, ROOT_INO, finode);

	if (err < 0) return err;

	WR_LOCK(get_lock_for_inode(finode.get_ino()));

	// chmod if:
	// - user is the owner of the file
	// - user is root
	// - if setuid and setgid bits are present:
	//		- if user is not root or owner, user can chmod if they don't try to "add" setuid, setgid of permission bits
	// otherwise, disallow since they are not owner or root

	// Check if user trying to add set_uid bit in the request
	// If the file's mode already have set_uid bit, it is considered not "adding"
	// If user trying to add set_gid bit in the request, it is allowed if user belongs to gid group, otherwise disallowed
	mode_t old_perm = finode.inode.mode & 0x7777;
	mode_t req_perm = mode & 0x7777;

	bool is_owner_or_root = finode.user_is_owner() || is_context_user_root();
	bool adding_suid, adding_sgid;

	if (!is_owner_or_root) {
		// Check if the old perm does not contain set uid bit
		// but the requested perm wants to set uid
		adding_suid = (req_perm & S_ISUID) && !(old_perm & S_ISUID);

		adding_sgid = (req_perm & S_ISGID) && !(old_perm & S_ISGID);

		// Check if any other permission bits are changed beside suid and sgid
		mode_t changed = old_perm ^ req_perm;
		bool change_non_special = (changed & ~(S_ISUID | S_ISGID)) != 0;

		// Non-owner can never add setuid
		if (adding_suid) return -EPERM;

		// Non-owner may add setgid if they belong to the items' group and no other bits are changed
		if (adding_sgid) {
			if (!finode.user_is_group()) return -EPERM;

			// User change other bits are not allowed
			if (change_non_special) return -EPERM;
		} else {
			// Any other normal chmod by non-owner is not allowed
			if (change_non_special) return -EPERM;
		}
	}

	// If user is owner but not belongs to the item's group
	// and wish to setgid bit
	// the request is still forwarded but the setgid bit is cleared
	if ((req_perm & S_ISGID) && !finode.user_is_group() && !is_context_user_root()) {
		// Clear sgid bit
		req_perm &= ~S_ISGID;
	}

	// Clear suid bit if user is not root, but the requested perm has it
	if ((req_perm & S_ISUID) && !is_context_user_root()) {
		// Clear suid bit
		req_perm &= ~S_ISUID;
	}

	// Can change mode
	// Keep type bits, change permission bits
	finode.inode.mode = (finode.inode.mode & S_IFMT) | req_perm;

	err = finode.save();

	if (err < 0) {
		return err;
	}

	return 0;
}

static int myfs_chown(const char* path, uid_t uid, gid_t gid, struct fuse_file_info *fi) {
	Inode finode;

	error_t err = inode_manager.get_inode_from_path(path, ROOT_INO, finode);

	if (err < 0) return err;

	WR_LOCK(get_lock_for_inode(finode.get_ino()));

	if (uid != finode.inode.uid) {
		// User wish to change uid
		// Only root can change uid
		if (!is_context_user_root()) return -EPERM;
	}

	if (gid != finode.inode.gid) {
		// User wish to change gid

		// Root can change gid

		// Non-root user can change gid if:
		// - User owns the file AND the group of this item belongs to the user's groups
		// Since FUSE does not provide the list of groups that user belongs to
		// I gotta skip that condition and only check for primary group
		if (!is_context_user_root() && (!finode.user_is_owner() || !finode.user_is_group())) {
			return -EPERM;
		}
	}

	finode.inode.uid = uid;
	finode.inode.gid = gid;

	// Clear suid, sgid bits if non root
	if (!is_context_user_root()) {
		finode.inode.mode &= ~S_ISUID;
		finode.inode.mode &= ~S_ISGID;
	}

	err = finode.save();

	if (err < 0) return err;

	return 0;
}

static int myfs_statfs(const char* path, struct statvfs *stat) {
	stat->f_bsize = StorageManager::BLOCK_SIZE;
	stat->f_frsize = StorageManager::BLOCK_SIZE;
	stat->f_blocks = superblock.get_max_dnum();
	stat->f_namemax = NAME_MAX;
	stat->f_bfree = superblock.get_free_data_blk_count();

	return 0;
}

static struct fuse_operations myfs_ope = {
	.getattr = myfs_getattr,
	.readlink = myfs_readlink,
	.mknod = myfs_mknod,
	.mkdir = myfs_mkdir,
	.unlink = myfs_unlink,
	.rmdir = myfs_rmdir,
	.symlink = myfs_symlink,
	.rename = myfs_rename,
	.link = myfs_link,
	.chmod = myfs_chmod,
	.chown = myfs_chown,
	.truncate = myfs_truncate,
	.open = myfs_open,
	.read = myfs_read,
	.write = myfs_write,
	.statfs = myfs_statfs,
	.flush = myfs_flush,
	.release = myfs_release,
	.fsync = myfs_fsync,
#ifdef HAVE_SETXATTR
	.setxattr	= myfs_setxattr,
	.getxattr	= myfs_getxattr,
	.listxattr	= myfs_listxattr,
	.removexattr	= myfs_removexattr,
#endif
	.opendir = myfs_opendir,
	.readdir = myfs_readdir,
	.releasedir = myfs_releasedir,

	.init = myfs_init,
	.destroy = myfs_destroy,
	.access = myfs_access,
	.create = myfs_create,
#ifdef HAVE_UTIMESAT
	.utimens = myfs_utimens,
#endif
	.flock = myfs_flock,
	.fallocate = myfs_fallocate,
	.lseek = myfs_lseek,
#ifdef HAVE_COPY_FILE_RANGE
	.copy_file_range = myfs_copy_file_range,
#endif
#ifdef HAVE_STATX
	.statx		= myfs_statx,
#endif
};

int main(int argc, char* argv[]) {
	DBG("Opening thread log file at %s", THREAD_LOG_FILE);

	error_t err = thread_fd_open(THREAD_LOG_FILE);

	if (err < 0) {
		DBG("Failed to open thread log file at %s", THREAD_LOG_FILE);

		// Make sure to set fd back to nullptr
		thread_log_fd = nullptr;
	}

    DBG("Starting MYFS file system...");

	int fuse_stat;

	if (getcwd(diskfile_path, PATH_MAX) == NULL) {
		perror("Failed to get diskfile path");
		exit(EXIT_FAILURE);
	}
	
	DBG("Disk path is %s", diskfile_path);

	strcat(diskfile_path, "/DISKFILE");

	DBG("Disk file is %s", diskfile_path);
	
	DBG("Starting FUSE...");

	// Start FUSE
	fuse_stat = fuse_main(argc, argv, &myfs_ope, NULL);
	
	return fuse_stat;
}