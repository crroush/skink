// Copyright 2022 Google LLC
// Author: Sean McAllister

#pragma once

#include <atomic>
#include <algorithm>
#include <memory>

// abseil
#include <absl/container/flat_hash_map.h>
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
        MappedSizePtr(void* ptr, size_t size, ssize_t map_size=-1)
            : ptr(ptr), size(size), map_size_(map_size >= 0 ? map_size : size) {}

        // To prevent alising this class is move only.
        MappedSizePtr(const MappedSizePtr&) = delete;

        MappedSizePtr(MappedSizePtr&& b)
            : MappedSizePtr() {
            std::swap(ptr, b.ptr);
            std::swap(size, b.size);
            std::swap(map_size_, b.map_size_);
        }

        MappedSizePtr& operator=(MappedSizePtr b) {
            unmap();
            std::swap(ptr, b.ptr);
            std::swap(size, b.size);
            std::swap(map_size_, b.map_size_);
            return *this;
        }

        ~MappedSizePtr() {
            unmap();
        }

        void*  ptr  = nullptr;
        size_t size = 0;

      private:
        size_t map_size_ = 0;

        void unmap() {
            if (ptr) {
                munmap(ptr, map_size_);
            }
            ptr = nullptr;
            size = 0;
            map_size_ = 0;
        }
    };

    // A set of 64-bit offsets that can be updated from multiple threads.
    //
    // Individual offsets may be updated under a reader lock, but if we have to
    // recompute the minimum offset, an exclusive lock is obtained to prevent
    // further updates while the new value is computed.
    //
    // The current minimum can be read via min_offset() which doesn't take out a
    // lock, it instead uses an atomic to provide a memory fence so that partial
    // updates are never seen.
    //
    // The lock we use to synchronize access for writing the min read offset can
    // also be used to wait for more data to come in.  await_bytes() will take
    // out the lock and Await on it until the minimum offsets moves far enough
    // forward.  We use conditional critical sections for this which should be
    // more efficient than a condition variable.
    struct ConcurrentPositionSet {
        ConcurrentPositionSet()
            : oneup_cnt_(0) {}

        // Adds a new offset to the position set, set to the current minimum.
        //
        // Returns an integer identifier for the new offset.
        int add_offset() LOCKS_EXCLUDED(lock_);

        // Remove the given offset from the position set.
        void del_offset(int id) LOCKS_EXCLUDED(lock_);

        // Increments the given offset by some number of bytes.
        void inc_offset(int id, int nbytes) LOCKS_EXCLUDED(lock_);

        // Waits for a given number of bytes to become available.
        //
        // Returns true if the bytes became available or false if all offsets
        // we were tracking were removed.
        bool await_bytes(int nbytes) const LOCKS_EXCLUDED(lock_);

        // Returns the current minimum offset value.
        uint64_t min_offset() const {
            return min_offset_.load(std::memory_order_acquire);
        }

    private:
        mutable absl::Mutex lock_;
        absl::flat_hash_map<int, uint64_t> offsets_ GUARDED_BY(lock_);
        int oneup_cnt_                              GUARDED_BY(lock_);
        std::atomic<uint64_t> min_offset_;
    };

    // Create a new memory mapping of the given size.
    bool mmap_buffer(size_t size);

    MappedSizePtr buffer_;
    ConcurrentPositionSet readers_;
};
