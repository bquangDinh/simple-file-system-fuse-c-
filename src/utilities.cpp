#include "utilities.hpp"

#include <ctime>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <cstdlib>

thread_local std::unordered_set<size_t> held_lock_idx;

void Utilities::print_hex(const char* str, size_t len) {
	for (size_t i = 0; i < len; ++i) {
		printf("%02x", str[i]);
	}

	printf("\n");
}

void Utilities::print_bitmap_bits(bitmap_t bitmap, size_t bits) {
	for (size_t i = 0; i < bits; ++i) {
		size_t byte = i / 8;
		size_t bit = i % 8;

		printf("%d", (bitmap[byte] >> bit) & 1);

		if ((i + 1) % 8 == 0) printf(" ");
	}

	printf("\n");
}

void Utilities::debug(const char* file, int line, const char* func, const char* fmt, ...) {
    std::printf("[%lu][%s:%d:%s] ", (unsigned long)pthread_self(), file, line, func);

    va_list args;
    va_start(args, fmt);
    std::vprintf(fmt, args);
    va_end(args);

    std::printf("\n");

	std::fflush(stdout);
}

struct timespec Utilities::now(void) {
    struct timespec ts;

	ts.tv_sec = time(nullptr);
	ts.tv_nsec = 0;

	return ts;
}

int Utilities::split_path(const char* path, path_split *out) {
    assert(path != NULL);
	assert(out != NULL);

	size_t len = strlen(path);
	char* buf = (char*)malloc(len + 1); // account for NULL-terminated character at the end

	if (buf == NULL) return -ENOMEM;

	// Since we cannot modify path (const), we memcpy it to buf
	memcpy(buf, path, len + 1);

	// Strip trailing slashes such as /a/b/, /a/b/////// -> /a/b
	while (len > 1 && buf[len - 1] == '/') buf[--len] = '\0';

	// Find the location of the last '/'
	// strrchr returns a pointer that points to the location of the last slash
	char* slash = strrchr(buf, '/');

	if (slash == NULL) {
		// There is no slash, just name
		// Then we make dir to be ".", then base is "name"
		out->buf = buf;
		out->dir = (char*)".";
		out->base = buf;

		return 0;
	}

	if (slash == buf) {
		// "/name" or "/"
		// slash and buf are both pointers point to the beginning of some string
		// thus, if slash == buf, it means they are both pointing to the beginning of the string
		// so the only cases are "/name" or "/"
		// Same as above, dir is "." and base is "name" OR base is empty
		out->buf = buf;
		out->dir = (char*)".";

		// slash + 1 makes base point to the next character which is the beginning of "name"
		out->base = (len == 1) ? (char*)"" : (slash + 1);

		return 0;
	}

	// In this case
	// we could have "/a/b/c"

	// slash points to the last slash
	// which mean /a/b/<HERE>c
	// changing it to NULL-terminated character will split dir and base into /a/b[\0]c
	*slash = '\0'; 
	out->buf = buf;
	out->dir = buf;
	out->base = slash + 1;

	return 0;
}

void Utilities::free_path_split(path_split* p) {
    if (p && p->buf) free(p->buf);
}

void Utilities::BitmapOps::set_bitmap(bitmap_t b, uint32_t i) {
	b[i / 8] |= 1 << (i & 7);
}

void Utilities::BitmapOps::unset_bitmap(bitmap_t b, uint32_t i) {
	b[i / 8] &= ~(1 << (i & 7));
}

uint8_t Utilities::BitmapOps::get_bitmap(bitmap_t b, uint32_t i) {
	return b[i / 8] & (1 << (i & 7)) ? 1 : 0;
}

mode_t Utilities::PermissionOps::get_user_perm(mode_t mode) {
	return (mode >> 6) & 7;
}

mode_t Utilities::PermissionOps::get_group_perm(mode_t mode) {
	return (mode >> 3) & 7;
}

mode_t Utilities::PermissionOps::get_other_perm(mode_t mode) {
	return mode & 7;
}

bool Utilities::ProcessOps::pid_has_group(pid_t pid, gid_t target) {
	char path[64];
	
	// Read process's status file
	snprintf(path, sizeof(path), "/proc/%d/status", pid);

	FILE* file = fopen(path, "r");

	if (!file) {
		return false;
	}

	char *line = nullptr;
	size_t n = 0;
	bool ok = false;

	// Keep reading new line until we reach line named "Group: ...."
	while (getline(&line, &n, file) != -1) {
		if (strncmp(line, "Groups:", 7) == 0) {
			// Start reading numbers after the "Groups:"
			char *p = line + 7;

			// Not yet end of line
			while (*p) {
				// Ignore space and tab
				if (*p == ' ' || *p == '\t') p++;

				// If we reached new line, then it is the end
				if (*p == '\n') {
					break;
				}

				// Convert string to number
				// The second parameter &p is used to store the first invalid chacracter
				// after the number, thus moving pointer forward
				unsigned long g = strtoul(p, &p, 10);

				printf("%lu ", g);

				if ((gid_t)g == target) {
					ok = true;

					break;
				}
			}

			break;
		}
	}

	free(line);

	fclose(file);

	return ok;
}

bool Utilities::ProcessOps::gid_belongs_to_user_group(pid_t pid, gid_t primary, gid_t target) {
	if (target == primary) return true;

	// Check if target belongs to the user's group
	return Utilities::ProcessOps::pid_has_group(pid, target);
}

Utilities::Mutex::TrackedUniqueLock::TrackedUniqueLock(std::shared_mutex& mtx, size_t idx, bool defer)
: _idx(idx), _defer(defer)
{
	if (_defer) {
		_lock = std::unique_lock<std::shared_mutex>(mtx, std::defer_lock);
	} else {
		_lock = std::unique_lock<std::shared_mutex>(mtx);
		Utilities::Mutex::DebugLockTracker::on_lock(idx);
	}
}

Utilities::Mutex::TrackedUniqueLock::TrackedUniqueLock(std::shared_mutex& mtx, size_t idx, FILE* log, const char* op, bool defer)
: _idx(idx), _defer(defer), _log(log), _op(op)
{
	if (_defer) {
		_lock = std::unique_lock<std::shared_mutex>(mtx, std::defer_lock);
	} else {
		if (_log != nullptr && op != nullptr) {
			fprintf(_log, "[%lu] [%s] TRY ACQUIRING [%s] WR LOCK on idx: %zu\n", (unsigned long)pthread_self(), op, _defer ? "NOT LOCKED" : "LOCKED", idx);
			fflush(_log);
		}

		_lock = std::unique_lock<std::shared_mutex>(mtx);
		Utilities::Mutex::DebugLockTracker::on_lock(idx);

		if (_log != nullptr && op != nullptr) {
			fprintf(_log, "[%lu] [%s] ACQUIRED [%s] WR LOCK on idx: %zu\n", (unsigned long)pthread_self(), op, _defer ? "NOT LOCKED" : "LOCKED", idx);
			fflush(_log);
		}
	}
}

Utilities::Mutex::TrackedUniqueLock::TrackedUniqueLock(Utilities::Mutex::TrackedUniqueLock&& other) noexcept
: _idx(other._idx), _defer(other._defer), _log(other._log), _op(other._op), _lock(std::move(other._lock))
{
}

Utilities::Mutex::TrackedUniqueLock& Utilities::Mutex::TrackedUniqueLock::operator=(Utilities::Mutex::TrackedUniqueLock&& other) noexcept {
	if (this == &other) return *this;

	unlock();

	_lock = std::move(other._lock);
	_idx = other._idx;
	_defer = other._defer;
	_log = other._log;
	_op = other._op;

	other._idx = 0;
	other._log = nullptr;
	other._op = nullptr;

	return *this;
}

Utilities::Mutex::TrackedUniqueLock::~TrackedUniqueLock() {
	unlock();
}

void Utilities::Mutex::TrackedUniqueLock::lock() {
	assert(_defer);

	if (_log != nullptr && _op != nullptr) {
		fprintf(_log, "[%lu] [%s] TRY ACQUIRING [%s] WR LOCK on idx: %zu\n", (unsigned long)pthread_self(), _op, _defer ? "NOT LOCKED" : "LOCKED", _idx);
		fflush(_log);
	}

	_lock.lock();
	Utilities::Mutex::DebugLockTracker::on_lock(_idx);

	if (_log != nullptr && _op != nullptr) {
		fprintf(_log, "[%lu] [%s] ACQUIRED [%s] WR LOCK on idx: %zu\n", (unsigned long)pthread_self(), _op, _defer ? "NOT LOCKED" : "LOCKED", _idx);
		fflush(_log);
	}
}

void Utilities::Mutex::TrackedUniqueLock::unlock() {
	if (_log != nullptr && _op != nullptr) {
		fprintf(_log, "[%lu] [%s] TRY RELEASING WR LOCK on idx %zu\n", (unsigned long)pthread_self(), _op, _idx);
		fflush(_log);
	}

	if (_lock.owns_lock()) {
		DebugLockTracker::on_unlock(_idx);

		_lock.unlock();

		if (_log != nullptr && _op != nullptr) {
			fprintf(_log, "[%lu] [%s] RELEASED WR LOCK on idx %zu\n", (unsigned long)pthread_self(), _op, _idx);
			fflush(_log);
		}
	}
}

bool Utilities::Mutex::TrackedUniqueLock::owns_lock() {
	return _lock.owns_lock();
}

/** */

Utilities::Mutex::TrackedSharedLock::TrackedSharedLock(std::shared_mutex& mtx, size_t idx, bool defer)
: _idx(idx), _defer(defer)
{
	if (_defer) {
		_lock = std::shared_lock<std::shared_mutex>(mtx, std::defer_lock);
	} else {
		_lock = std::shared_lock<std::shared_mutex>(mtx);
		Utilities::Mutex::DebugLockTracker::on_lock(idx);
	}
}

Utilities::Mutex::TrackedSharedLock::TrackedSharedLock(std::shared_mutex& mtx, size_t idx, FILE* log, const char* op, bool defer)
: _idx(idx), _defer(defer), _log(log), _op(op)
{
	if (_defer) {
		_lock = std::shared_lock<std::shared_mutex>(mtx, std::defer_lock);
	} else {
		if (_log != nullptr && op != nullptr) {
			fprintf(_log, "[%lu] [%s] TRY ACQUIRING [%s] RD LOCK on idx: %zu\n", (unsigned long)pthread_self(), op, _defer ? "NOT LOCKED" : "LOCKED", idx);
			fflush(_log);
		}

		_lock = std::shared_lock<std::shared_mutex>(mtx);
		Utilities::Mutex::DebugLockTracker::on_lock(idx);

		if (_log != nullptr && op != nullptr) {
			fprintf(_log, "[%lu] [%s] ACQUIRED [%s] RD LOCK on idx: %zu\n", (unsigned long)pthread_self(), op, _defer ? "NOT LOCKED" : "LOCKED", idx);
			fflush(_log);
		}
	}
}

Utilities::Mutex::TrackedSharedLock::TrackedSharedLock(Utilities::Mutex::TrackedSharedLock&& other) noexcept
: _idx(other._idx), _defer(other._defer), _log(other._log), _op(other._op), _lock(std::move(other._lock))
{
}

Utilities::Mutex::TrackedSharedLock& Utilities::Mutex::TrackedSharedLock::operator=(Utilities::Mutex::TrackedSharedLock&& other) noexcept {
	if (this == &other) return *this;

	unlock();

	_lock = std::move(other._lock);
	_idx = other._idx;
	_defer = other._defer;
	_log = other._log;
	_op = other._op;

	if (_lock.owns_lock()) {
		Utilities::Mutex::DebugLockTracker::on_lock(_idx);
	}

	other._idx = 0;
	other._log = nullptr;
	other._op = nullptr;

	return *this;
}

Utilities::Mutex::TrackedSharedLock::~TrackedSharedLock() {
	unlock();
}

void Utilities::Mutex::TrackedSharedLock::lock() {
	assert(_defer);

	if (_log != nullptr && _op != nullptr) {
		fprintf(_log, "[%lu] [%s] TRY ACQUIRING [%s] RD LOCK on idx: %zu\n", (unsigned long)pthread_self(), _op, _defer ? "NOT LOCKED" : "LOCKED", _idx);
		fflush(_log);
	}

	_lock.lock();
	Utilities::Mutex::DebugLockTracker::on_lock(_idx);

	if (_log != nullptr && _op != nullptr) {
		fprintf(_log, "[%lu] [%s] ACQUIRED [%s] RD LOCK on idx: %zu\n", (unsigned long)pthread_self(), _op, _defer ? "NOT LOCKED" : "LOCKED", _idx);
		fflush(_log);
	}
}

void Utilities::Mutex::TrackedSharedLock::unlock() {
	if (_log != nullptr && _op != nullptr) {
		fprintf(_log, "[%lu] [%s] TRY RELEASING RD LOCK on idx %zu\n", (unsigned long)pthread_self(), _op, _idx);
		fflush(_log);
	}

	if (_lock.owns_lock()) {
		DebugLockTracker::on_unlock(_idx);

		_lock.unlock();

		if (_log != nullptr && _op != nullptr) {
			fprintf(_log, "[%lu] [%s] RELEASED RD LOCK on idx %zu\n", (unsigned long)pthread_self(), _op, _idx);
			fflush(_log);
		}
	}
}

bool Utilities::Mutex::TrackedSharedLock::owns_lock() {
	return _lock.owns_lock();
}

void Utilities::Mutex::TrackedSharedLock::change_op(const char* op) {
	_op = op;
}