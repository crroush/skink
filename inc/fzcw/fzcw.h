// Copyright 2022 Google LLC
// Author: Sean McAllister

#pragma once

#include <atomic>
#include <algorithm>
#include <memory>

// abseil
#include <spdlog/spdlog.h>
#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

// linux
#include <signal.h>
#include <unistd.h>
#include <sys/mman.h>

// Get page size once at process start.
static const int FZCW_PAGE_SIZE = getpagesize();

template <typename Tinp> struct zcwriter;
template <typename Tout> struct zcreader;

struct zcstream {
    // 64K is a reasonable default size that balances performance and memory.
    static constexpr size_t DEFAULT_SIZE = 65536;

    zcstream(size_t size=DEFAULT_SIZE)
        : failed_(mmap_buffer(size)) {
        // We're single writer, so just add one offset to the write set.
        writer_.add_offset();
    }

    // Returns current size of buffer, in bytes.
    ssize_t size() const LOCKS_EXCLUDED(lock_) {
        SPDLOG_DEBUG(!failed());
        absl::ReaderMutexLock lock(&lock_);
        return buffer_.size;
    }

    // Returns true if the zcbuffer has failed to map memory somehow.
    bool failed() const LOCKS_EXCLUDED(lock_) {
        absl::ReaderMutexLock lock(&lock_);
        return failed_;
    }

    // Close the write end of the buffer, signaling no more data.
    void wrclose() {
        writer_.del_offset(0);
    }

    // Adds a reader to the buffer and returns an integer identifying it.
    int add_reader() {
        return readers_.add_offset();
    }

    // Removes a given reader from the buffer.  Noop if no such reader exists.
    void del_reader(int id) {
        readers_.del_offset(id);
    }

    // Ensures that the buffer is large enough that it can accommodate borrowing
    // memory of at least the given size bytes.  The buffer size is never shrunk
    // so this may be a noop if it is already large enough.
    //
    // Returns true on success, false if the buffer couldn't be resized.
    bool resize(ssize_t nbytes) LOCKS_EXCLUDED(lock_, readers_) {
        // Roundup to page size.
        nbytes = (nbytes + FZCW_PAGE_SIZE - 1)/FZCW_PAGE_SIZE*FZCW_PAGE_SIZE;

        if (nbytes <= size()) {
            return true;
        }

        absl::WriterMutexLock buffer_lock(&lock_);
        readers_.Lock();

        // Steal the old mapped buffer, its destructor will unmap it.
        MappedSizePtr old_buffer = std::move(buffer_);

        // Try to map a new buffer, if it fails put the old one back.
        if (!mmap_buffer(nbytes)) {
            buffer_ = std::move(old_buffer);
            readers_.Unlock();
            return false;
        }

        uint64_t offset = writer_.get_offset(0);
        if (old_buffer.ptr != nullptr) {
            /// XXX: copy old contents to new buffer
        }

        // Releasing lock will automatically notify any waiting writers that
        // there's more space available now.
        readers_.Unlock();
        return true;
    }

private:
    // A memory-mapped pointer and size that can unmap itself.
    struct MappedSizePtr {
        MappedSizePtr() = default;
        MappedSizePtr(void* ptr, size_t size, ssize_t mmap_size=-1)
            : ptr(ptr), size(size), mmap_size_(mmap_size) {
            if (mmap_size < 0) {
                mmap_size_ = 2*size;
            }
        }

        // To prevent alising this class is move only.
        MappedSizePtr(const MappedSizePtr&) = delete;

        MappedSizePtr(MappedSizePtr&& b)
            : MappedSizePtr() {
            std::swap(ptr, b.ptr);
            std::swap(size, b.size);
            std::swap(mmap_size_, b.mmap_size_);
        }

        MappedSizePtr& operator=(MappedSizePtr b) {
            unmap();
            std::swap(ptr, b.ptr);
            std::swap(size, b.size);
            std::swap(mmap_size_, b.mmap_size_);
            return *this;
        }

        ~MappedSizePtr() {
            unmap();
        }

        void*  ptr  = nullptr;
        size_t size = 0;

      private:
        size_t mmap_size_ = 0;

        void unmap() {
            if (ptr) {
                munmap(ptr, mmap_size_);
            }
            ptr = nullptr;
            size = 0;
            mmap_size_ = 0;
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
    // The lock we use to synchronize access for writing the minimum offset can
    // also be used to wait for more data to come in.  await_bytes() will take
    // out the lock and Await on it until the minimum offsets moves far enough
    // forward.  We use conditional critical sections for this which should be
    // more efficient than a condition variable.
    struct LOCKABLE ConcurrentPositionSet {
        // Adds a new offset to the position set, set to the current minimum.
        //
        // Returns an integer identifier for the new offset.
        int add_offset() LOCKS_EXCLUDED(lock_);

        // Remove the given offset from the position set.
        void del_offset(int id) LOCKS_EXCLUDED(lock_);

        // Increments the given offset by some number of bytes.
        void inc_offset(int id, int nbytes) LOCKS_EXCLUDED(lock_);

        // Returns the current value of the given offset.
        //
        // Passing an invalid id is undefined behavior.
        uint64_t get_offset(int id) const LOCKS_EXCLUDED(lock_);

        // Waits for a given number of bytes to become available.
        //
        // Returns true if the bytes became available or false if all offsets
        // we were tracking were removed.
        bool await_bytes(int nbytes) const LOCKS_EXCLUDED(lock_);

        // Returns the current minimum offset value.
        uint64_t min_offset() const {
            return min_offset_.load(std::memory_order_acquire);
        }

        // Locks the entire position set for writing.
        void Lock() ABSL_EXCLUSIVE_LOCK_FUNCTION() {
            lock_.Lock();
        }

        // Unlocks the entire position set.
        void Unlock() ABSL_UNLOCK_FUNCTION() {
            lock_.Unlock();
        }

    private:
        // Atomics aren't movable by default, so we have to wrap one up.  This
        // lets us use it with absl::flat_hash_map which may need to resize and
        // move its contents.  We synchronize access to the offsets table using
        // a mutex so this is safe.
        struct Offset {
            Offset(uint64_t value=0) : value_(value) {}
            Offset(Offset&& b) {
                value_.store(
                    b.value_.load(std::memory_order_acquire),
                    std::memory_order_release);
            }

            uint64_t value() const {
                return value_.load(std::memory_order_acquire);
            }

            Offset& operator+=(uint64_t val) {
                value_.fetch_add(val, std::memory_order_acq_rel);
                return *this;
            }

        private:
            std::atomic<uint64_t> value_;
        };

        void update_min_offset() EXCLUSIVE_LOCKS_REQUIRED(lock_);

        mutable absl::Mutex lock_;
        GUARDED_BY(lock_) absl::flat_hash_map<int, Offset> offsets_;
        GUARDED_BY(lock_) int oneup_cnt_ = 0;
        std::atomic<uint64_t> min_offset_;
    };

    mutable absl::Mutex lock_;
    GUARDED_BY(lock_) MappedSizePtr buffer_;
    GUARDED_BY(lock_) bool failed_ = false;

    ConcurrentPositionSet readers_;
    ConcurrentPositionSet writer_;

    // Create a new memory mapping of the given size.
    bool mmap_buffer(size_t size) EXCLUSIVE_LOCKS_REQUIRED(lock_);
};
