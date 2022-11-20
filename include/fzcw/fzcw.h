#pragma once

#include <algorithm>
#include <memory>

// abseil
#include <absl/synchronization/mutex.h>

// linux
#include <signal.h>
#include <unistd.h>
#include <sys/mman.h>

template <typename Tinp> struct zcwriter;
template <typename Tout> struct zcreader;

struct zcstream {
    // 64K is a reasonable default size that balances performance and memory.
    static constexpr size_t DEFAULT_SIZE = 65536;

    zcstream() = default;

private:
    // A memory-mapped pointer and size that can unmap itself.
    struct MappedSizePtr {
        MappedSizePtr() = default;
        MappedSizePtr(void* ptr, size_t size)
            : ptr(ptr), size(size) {}

        // To prevent alising this class is move only.
        MappedSizePtr(const MappedSizePtr&) = delete;

        MappedSizePtr(MappedSizePtr&& b)
            : MappedSizePtr() {
            std::swap(ptr, b.ptr);
            std::swap(size, b.size);
        }

        MappedSizePtr& operator=(MappedSizePtr b) {
            unmap();
            std::swap(ptr, b.ptr);
            std::swap(size, b.size);
            return *this;
        }

        ~MappedSizePtr() {
            unmap();
        }

        void*  ptr  = nullptr;
        size_t size = 0;

      private:
        void unmap() {
            if (ptr) {
                munmap(ptr, size);
            }
            ptr = nullptr;
            size = 0;
        }
    };

    // Create a new memory mapping of the given size.
    bool mmap_buffer(size_t size);

    MappedSizePtr buffer_;
};
