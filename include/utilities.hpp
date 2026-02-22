#pragma once

#include <stdint.h>
#include <sys/types.h>
#include <cstdarg>
#include <cstdio>
#include <unordered_set>
#include <mutex>
#include <shared_mutex>

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
extern thread_local std::unordered_set<size_t> held_lock_idx;

namespace Utilities {

    struct path_split {
        char *buf;
        char *dir;
        char *base;
    };

    void print_hex(const char* str, size_t len);

    void print_bitmap_bits(const bitmap_t bitmap, size_t bits);

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

    namespace ProcessOps {
        // Check if a given process id's group list contain the given target group id
        bool pid_has_group(pid_t pid, gid_t target);

        // Check if the target group belongs to the user's group list or user's primary group id
        bool gid_belongs_to_user_group(pid_t pid, gid_t primary, gid_t target);
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

    namespace Mutex {
        struct DebugLockTracker {
            static void on_lock(size_t idx) { held_lock_idx.insert(idx); }
            static void on_unlock(size_t idx) { held_lock_idx.erase(idx); }
            static bool is_held(size_t idx) { return held_lock_idx.count(idx) > 0; }
        };

        class TrackedUniqueLock {
            public:
                TrackedUniqueLock() = default;

                TrackedUniqueLock(std::shared_mutex& mtx, size_t idx, bool defer = false);

                TrackedUniqueLock(std::shared_mutex& mtx, size_t idx, FILE* log, const char* op, bool defer = false);

                ~TrackedUniqueLock();

                TrackedUniqueLock& operator=(TrackedUniqueLock&& other) noexcept;

                TrackedUniqueLock(TrackedUniqueLock&& other) noexcept;

                void lock();

                void unlock();

                // Check if the lock currently owns the mutex
                bool owns_lock();
            private:
                std::unique_lock<std::shared_mutex> _lock;
                size_t _idx = 0;
                bool _defer = false;
                FILE* _log = nullptr;
                const char* _op = nullptr;
        };

        class TrackedSharedLock {
            public:
                TrackedSharedLock() = default;

                TrackedSharedLock(std::shared_mutex& mtx, size_t idx, bool defer = false);

                TrackedSharedLock(std::shared_mutex& mtx, size_t idx, FILE* log, const char* op, bool defer = false);

                TrackedSharedLock(TrackedSharedLock&& other) noexcept;

                ~TrackedSharedLock();

                TrackedSharedLock& operator=(TrackedSharedLock&& other) noexcept;

                void lock();

                void unlock();

                // Check if the lock currently owns the mutex
                bool owns_lock();

                void change_op(const char* op);
            private:
                std::shared_lock<std::shared_mutex> _lock;
                size_t _idx = 0;
                bool _defer = false;
                FILE* _log = nullptr;
                const char* _op = nullptr;
        };
    }
}