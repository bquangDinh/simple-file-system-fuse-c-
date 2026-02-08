#include <math.h>
#include <assert.h>

#include "myfs/superblock.hpp"
#include "myfs/inode.hpp"
#include "utilities.hpp"

error_t SuperblockManager::save() {
    return storage.block_write(SUPERBLOCK_BLK_NUM, &_superblock);
}

error_t SuperblockManager::init() {
    // Read superblock
    error_t err = storage.block_read(SUPERBLOCK_BLK_NUM, &_superblock);

    if (err < 0) return err;

    // Check if the block has the same magic number
    // If it is, that means the superblock has been initialized before
    if (_superblock.magic_num == SUPERBLOCK_MAGIC_NUM) {
        superblock_existed = true;

        return 0;
    }

    // TODO: should check if the total number of blocks exceed file size

    // Superblock has not yet existed, create new one
    _superblock.magic_num = SUPERBLOCK_MAGIC_NUM;
    _superblock.max_inum = MAX_INODE_NUM;
    _superblock.max_dnum = MAX_DATA_BLK_NUM;
    _superblock.free_blk_count = MAX_DATA_BLK_NUM;
    _superblock.free_ino_count = MAX_INODE_NUM;

    _superblock.i_bitmap_blk = 1; // after superblock's block

    // Calculate the total blocks required to store inode bitmap
    uint16_t total_required_blocks = (uint16_t)ceil(INODE_BITMAP_BYTES / (float)StorageManager::BLOCK_SIZE);

    assert(total_required_blocks >= 1);

    // Data bitmap block starts after the last inode bitmap block
    _superblock.d_bitmap_blk = _superblock.i_bitmap_blk + total_required_blocks;

    // Calculate the total blocks required to store data bitmap
    total_required_blocks = (uint16_t)ceil(DATA_BITMAP_BYTES / (float)StorageManager::BLOCK_SIZE);

    assert(total_required_blocks >= 1);

    // The inode region starts after the data bitmap last block
    _superblock.i_start_blk = _superblock.d_bitmap_blk + total_required_blocks;

    // Calculate the total blocks required to store inode region
    total_required_blocks = (uint16_t)ceil(_superblock.max_inum * sizeof(inode_t) / (float)StorageManager::BLOCK_SIZE);

    assert(total_required_blocks >= 1);

    // The data region starts after the last block of inode region
    _superblock.d_start_blk = _superblock.i_start_blk + total_required_blocks;

    return save();
}

uint32_t SuperblockManager::get_max_inum() const {
    return _superblock.max_inum;
}

uint32_t SuperblockManager::get_max_dnum() const {
    return _superblock.max_dnum;
}

uint32_t SuperblockManager::get_ino_bm_start_blk() const {
    return _superblock.i_bitmap_blk;
}

uint32_t SuperblockManager::get_ino_bm_end_blk() const {
    return _superblock.d_bitmap_blk - 1;
}

uint32_t SuperblockManager::get_data_bm_start_blk() const {
    return _superblock.d_bitmap_blk;
}

uint32_t SuperblockManager::get_data_bm_end_blk() const {
    return _superblock.i_start_blk - 1;
}

uint32_t SuperblockManager::get_ino_region_start_blk() const {
    return _superblock.i_start_blk;
}

uint32_t SuperblockManager::get_ino_region_end_blk() const {
    return _superblock.d_start_blk - 1;
}

uint32_t SuperblockManager::get_data_region_start_blk() const {
    return _superblock.d_start_blk;
}

uint32_t SuperblockManager::get_data_region_end_blk() const {
    return _superblock.d_start_blk + _superblock.max_dnum - 1;
}

uint32_t SuperblockManager::get_free_data_blk_count() const {
    return _superblock.free_blk_count;
}

uint32_t SuperblockManager::get_free_ino_count() const {
    return _superblock.free_ino_count;
}

uint16_t SuperblockManager::get_ino_bitmap_blks() const {
    return _superblock.d_bitmap_blk - _superblock.i_bitmap_blk;
}

uint16_t SuperblockManager::get_data_bitmap_blks() const {
    return _superblock.i_start_blk - _superblock.d_bitmap_blk;
}

uint16_t SuperblockManager::get_magic_num() const {
    return _superblock.magic_num;
}

void SuperblockManager::increase_free_data_blk_count() {
    _superblock.free_blk_count++;
}

void SuperblockManager::decrease_free_data_blk_count() {
    _superblock.free_blk_count--;
}

void SuperblockManager::increase_free_ino_count() {
    _superblock.free_ino_count++;
}

void SuperblockManager::decrease_free_ino_count() {
    _superblock.free_ino_count--;
}

void SuperblockManager::print_info() {
    DBG("magic num: %u", get_magic_num());
    DBG("max inum: %u", get_max_inum());
    DBG("max dnum: %u", get_max_dnum());
    DBG("ino bitmap start at block: %u", get_ino_bm_start_blk());
    DBG("ino bitmap end at block: %u", get_ino_bm_end_blk());
    DBG("data bitmap start at block: %u", get_data_bm_start_blk());
    DBG("data bitmap end at block: %u", get_data_bm_end_blk());
    DBG("ino region start at block: %u", get_ino_region_start_blk());
    DBG("ino region end at block: %u", get_ino_region_end_blk());
    DBG("data region start at block: %u", get_data_region_start_blk());
    DBG("data region end at block: %u", get_data_region_end_blk());
    DBG("Num of ino bitmap blocks: %u", get_ino_bitmap_blks());
    DBG("Num of data bitmap blocks: %u", get_data_bitmap_blks());
    DBG("Free data blocks count: %u", get_free_data_blk_count());
    DBG("Free ino count: %u", get_free_ino_count());
}