#include "fzcw.h"

bool zcstream::mmap_buffer(size_t size) {
    // Create an anonymous memory mapping of twice the size requested.
    void* ptr = mmap(0, 2*size, PROT_NONE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if (!ptr) {
        return false;
    }

    int fd = memfd_create("zcstream", 0);
    if (fd < 0) {
        return false;
    }

    // Resize anonymous file to match size requested.
    if (ftruncate(fd, size) < 0) {
        return false;
    }

    // Map the file contents to the bottom half.
    const int kReadWrite = PROT_READ|PROT_WRITE;
    if (!mmap(ptr, size, kReadWrite, MAP_FIXED|MAP_SHARED, fd, 0)) {
        return false;
    }

    // Map the file contents again to the top half.
    if (!mmap(ptr+size, size, kReadWrite, MAP_FIXED|MAP_SHARED, fd, 0)) {
        return false;
    }






}
