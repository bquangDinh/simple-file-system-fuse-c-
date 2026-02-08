#pragma once

#include <stdint.h>
#include <sys/types.h>
#include <cstdarg>
#include <cstdio>

#define DEBUG

#ifdef DEBUG
#define DBG(fmt, ...) \
    Utilities::debug(__FILE__, __LINE__, __func__, fmt, ##__VA_ARGS__)
#define DBG_RAW(...) printf(__VA_ARGS__)
#else
#define DBG(fmt, ...) do {} while (0)
#define DBG_RAW(...) do {} while (0)
#endif

/**
 * Permission Utilities
 */
#define PERM_CAN_READ(perm) (perm & 4)
#define PERM_CAN_WRITE(perm) (perm & 2)
#define PERM_CAN_EXECUTE(perm) (perm & 1)
#define STICKY_MODE(mode) (mode & S_ISVTX)

#define IS_ROOT(uid) (uid == 0)

#define IS_STR_EMPTY(str) (strcmp(str, "") == 0)

typedef unsigned char* bitmap_t;

namespace Utilities {
    struct path_split {
        char *buf;
        char *dir;
        char *base;
    };

    void print_hex(const char* str, size_t len);

    void print_bitmap_bits(const bitmap_t bitmap, size_t bytes);

    void debug(const char* file, int line, const char* func, const char* fmt, ...);

    struct timespec now(void);

    int split_path(const char* path, struct path_split *out);

    void free_path_split(struct path_split* p);

    template<typename T>
    void fill_array(T* arr, T value, size_t len) {
        for (size_t i = 0; i < len; ++i) {
            arr[i] = value;
        }
    }

    namespace BitmapOps {
        void set_bitmap(bitmap_t b, uint32_t i);

        void unset_bitmap(bitmap_t b, uint32_t i);

        uint8_t get_bitmap(bitmap_t b, uint32_t i);
    }

    namespace PermissionOps {
        mode_t get_user_perm(mode_t mode);

        mode_t get_group_perm(mode_t mode);

        mode_t get_other_perm(mode_t mode);
    }
}