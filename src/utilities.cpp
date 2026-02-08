#include "utilities.hpp"

#include <ctime>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <cstdlib>

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
    std::printf("[%s:%d:%s] ", file, line, func);

    va_list args;
    va_start(args, fmt);
    std::vprintf(fmt, args);
    va_end(args);

    std::printf("\n");
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