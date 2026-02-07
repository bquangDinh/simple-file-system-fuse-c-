#include <cstring>
#include <cstdlib>
#include <assert.h>

#include "myfs/block.hpp"
#include "myfs/superblock.hpp"
#include "utilities.hpp"

#include "myfs/data.hpp"

error_t DataBlockManager::init_bitmap() {
    SuperblockManager& superblock = SuperblockManager::instance();
    StorageManager& storage = StorageManager::instance();

    if (superblock.superblock_existed) {
        // Read from old
        return 0;
    }

    const uint16_t data_bitmap_blocks = superblock.get_data_bitmap_blks();

    bitmap_t buffer = (bitmap_t)malloc(storage.BLOCK_SIZE);

    if (buffer == nullptr) return -ENOMEM;

    memset(buffer, 0, storage.BLOCK_SIZE);

    error_t err;

    for (uint16_t i = 0; i < data_bitmap_blocks; ++i) {
        err = storage.block_write(superblock.get_data_bm_start_blk() + i, buffer);

        if (err < 0) {
            free(buffer);

            return err;
        }
    }

    free(buffer);

    return 0;
}

error_t DataBlockManager::init() {
    error_t err = init_bitmap();

    if (err < 0) return err;

    return 0;
}

error_t DataBlockManager::get_available_blk(blk_t& out) {
    SuperblockManager& superblock = SuperblockManager::instance();
    StorageManager& storage = StorageManager::instance();

    std::lock_guard<std::mutex> lock(data_bitmap_lock);

    uint16_t data_bitmap_blocks = superblock.get_data_bitmap_blks();

    size_t num_bits_per_block = storage.BLOCK_SIZE * 8;

    assert(data_bitmap_blocks >= 1);
    assert(num_bits_per_block > 0);

    bitmap_t bitmap = (bitmap_t)malloc(storage.BLOCK_SIZE);

    if (bitmap == nullptr) return -ENOMEM;

    error_t err;

    uint32_t offset = superblock.get_data_bm_start_blk();

    for (uint16_t i = 0; i < data_bitmap_blocks; ++i) {
        err = storage.block_read(offset + i, bitmap);

        if (err < 0) {
            free(bitmap);

            return err;
        }

        for (uint32_t j = i * num_bits_per_block; j < (i + 1) * num_bits_per_block; ++j) {
            // No data block available
            if (j >= superblock.get_max_dnum() - 1) break;

            if (Utilities::BitmapOps::get_bitmap(bitmap, j - i * num_bits_per_block) == 0) {
                // Free data block found
                Utilities::BitmapOps::set_bitmap(bitmap, j - i * num_bits_per_block);

                // Update bitmap
                err = storage.block_write(offset + i, bitmap);

                if (err < 0) {
                    free(bitmap);

                    return err;
                }

                free(bitmap);

                // Make sure the newly allocated data block is set to zero
                // I'm not sure if I should do this as it hurts performance
                // but leaving garbage data will make ftruncate buggy (as indirect region will read the entire block)
                // but the index is not guaranteed to be within the approriate bound, thus it will read trunk data
                // and think there is an allocated data block
                void* buffer = malloc(storage.BLOCK_SIZE);

                if (buffer == nullptr) {
                    return -ENOMEM;
                }

                memset(buffer, 0, storage.BLOCK_SIZE);

                err = storage.block_write(j + superblock.get_data_region_start_blk(), buffer);

                if (err < 0) {
                    free(buffer);

                    return err;
                }

                free(buffer);

                // Update superblock stat
                superblock.decrease_free_data_blk_count();

                err = superblock.save();

                if (err < 0) return err;

                out = j + superblock.get_data_region_start_blk();

                return 0;
            }
        }
    }

    free(bitmap);

    return -ENOSPC;
}

error_t DataBlockManager::release_data_block(blk_t blk) {
    SuperblockManager& superblock = SuperblockManager::instance();
    StorageManager& storage = StorageManager::instance();

    assert(blk >= superblock.get_data_region_start_blk() && blk <= superblock.get_data_region_end_blk());

    uint32_t num_bits_per_block = storage.BLOCK_SIZE * 8;

    // Figure out which block in data bitmap region contains blk
    blk_t block = superblock.get_data_bm_start_blk() + ((blk - superblock.get_data_region_start_blk()) / num_bits_per_block);

    uint32_t bit_idx = (blk - superblock.get_data_region_start_blk()) % num_bits_per_block;

    bitmap_t bitmap = (bitmap_t)malloc(storage.BLOCK_SIZE);

    if (bitmap == nullptr) return -ENOMEM;

    error_t err = storage.block_read(block, bitmap);

    if (err < 0) {
        free(bitmap);

        return err;
    }

    Utilities::BitmapOps::set_bitmap(bitmap, bit_idx);

    err = storage.block_write(block, bitmap);

    if (err < 0) {
        free(bitmap);

        return err;
    } 

    free(bitmap);

    // Update superblock stat
    superblock.increase_free_data_blk_count();

    err = superblock.save();

    if (err < 0) return err;

    return 0;
}

error_t DataBlockManager::release_multi_data_blk(blk_t* blks, size_t len) {
    SuperblockManager& superblock = SuperblockManager::instance();
    StorageManager& storage = StorageManager::instance();

    error_t err;

    for (size_t i = 0; i < len; ++i) {
        assert(blks[i] >= superblock.get_data_region_start_blk() && blks[i] <= superblock.get_data_region_end_blk());

        // TODO: optimize this
        err = release_data_block(blks[i]);

        if (err < 0) return err;
    }

    return 0;
}