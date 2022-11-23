// Copyright 2022 Google LLC
// Author: Sean McAllister

#pragma once

#include <atomic>
#include <algorithm>
#include <memory>
#include <optional>

// abseil
#include <spdlog/spdlog.h>
#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

// linux
#include <signal.h>
#include <unistd.h>
#include <sys/mman.h>

#ifndef NDEBUG
#define DEBUG(x) x
#define DCHECK(condition)                                     \
    do {                                                      \
        if (ABSL_PREDICT_FALSE(!(condition))) {               \
            SPDLOG_ERROR(#condition);                         \
            assert(false);                                    \
        }                                                     \
    } while(0)
#else
#define DEBUG(x)
#define DCHECK(condition)
#endif

using absl::WriterMutexLock;
using absl::ReaderMutexLock;

// Get page size once at process start.
static const int FZCW_PAGE_SIZE = getpagesize();

template <typename Tinp> struct zcwriter;
template <typename Tout> struct zcreader;

// absl::ReaderMutexLock doesn't take an arbitrary lockable, this does.
template <typename T>
struct SCOPED_LOCKABLE ReaderScopeLock {
    explicit ReaderScopeLock(T* lockable) SHARED_LOCK_FUNCTION(lockable->Mutex())
    : lockable_(lockable) {
        lockable_->Mutex().ReaderLock();
    }

    ReaderScopeLock(const ReaderScopeLock &) = delete;
    ReaderScopeLock(ReaderScopeLock&&) = delete;
    ReaderScopeLock& operator=(const ReaderScopeLock&) = delete;
    ReaderScopeLock& operator=(ReaderScopeLock&&) = delete;

    ~ReaderScopeLock() UNLOCK_FUNCTION() {
        lockable_->Mutex().ReaderUnlock();
    }

 private:
    T *const lockable_;
};

// absl::WriterMutexLock doesn't take an arbitrary lockable, this does.
template <typename T>
struct SCOPED_LOCKABLE WriterScopeLock {
    explicit WriterScopeLock(T* lockable) EXCLUSIVE_LOCK_FUNCTION(lockable->Mutex())
    : lockable_(lockable) {
        lockable_->Mutex().WriterLock();
    }

    WriterScopeLock(const WriterScopeLock &) = delete;
    WriterScopeLock(WriterScopeLock&&) = delete;
    WriterScopeLock& operator=(const WriterScopeLock&) = delete;
    WriterScopeLock& operator=(WriterScopeLock&&) = delete;

    ~WriterScopeLock() UNLOCK_FUNCTION() { lockable_->Mutex().WriterUnlock(); }

 private:
    T *const lockable_;
};

struct zstream {
    // 64K is a reasonable default size that balances performance and memory.
    static constexpr size_t kDefaultSize = 65536;

    zstream(size_t size=kDefaultSize)
        : buffer_(size) {}

    ~zstream() {
        // Remove writer handle and wait for readers to disconnect.
        wrclose();

        auto no_readers = [this]() {
            DEBUG(readers_.Mutex().AssertReaderHeld());
            return readers_.num_offsets() == 0;
        };

        readers_.Mutex().WriterLock();
        readers_.Mutex().Await(absl::Condition(&no_readers));
        readers_.Mutex().WriterUnlock();
    }

    // Returns current size of buffer, in bytes.
    ssize_t size() const LOCKS_EXCLUDED(lock_) {
        DCHECK(!failed());
        ReaderMutexLock lock(&lock_);
        return buffer_.size();
    }

    // Returns true if the zcbuffer has failed to map memory somehow.
    bool failed() const LOCKS_EXCLUDED(lock_) {
        ReaderMutexLock lock(&lock_);
        return buffer_.data() == nullptr;
    }

    // Close the write end of the buffer, signaling no more data.
    void wrclose() {
        wropen_.store(false, std::memory_order_release);
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
    bool resize(ssize_t nbytes) LOCKS_EXCLUDED(lock_, readers_.Mutex());

    // Writes bytes to the stream.  If not enough space is available
    // immediately, blocks until all the data is written.  If all the readers
    // are removed before finishing the write, then less data than requested
    // may be written.
    //
    // Returns number of bytes actually written (-1 on error).
    ssize_t write(const void* ptr, ssize_t nbytes) LOCKS_EXCLUDED(lock_, readers_.Mutex());

    // Reads a given number of bytes from the buffer using the given reader
    // offset.  Blocks until all data requested is read, unless the writer
    // is closed, in which case returns early.
    //
    // id     - The identifier for the reader.  If no such reader, returns -1.
    // ptr    - Pointer to memory to read into.
    // nbytes - Number of bytes to read.
    // ncons  - Number of bytes actually consumed from the buffer.
    //   This is an optimization that lets us advance the read pointer
    //   immediately instead of waiting for the next read call.  By default, the
    //   entire nbytes is consumed.  ncons may be less than or greater than the
    //   number of bytes read.  When its less, the effect is equivalent to
    //   performing overlapping reads.  When greater, disjoint read.s
    //
    // Returns the number of bytes actually read.
    ssize_t read(int id, void* ptr, ssize_t nbytes, ssize_t ncons=-1) LOCKS_EXCLUDED(lock_, readers_.Mutex());

private:
    // A memory-mapped pointer and size that can unmap itself.
    struct MappedBuffer {
        MappedBuffer() = default;
        MappedBuffer(size_t size) {
            map(size);
        }

        ~MappedBuffer() {
            unmap();
        }

        // To prevent alising this class is move only.
        MappedBuffer(const MappedBuffer&) = delete;

        MappedBuffer(MappedBuffer&& b)
            : MappedBuffer() {
            std::swap(ptr_, b.ptr_);
            std::swap(size_, b.size_);
            std::swap(mmap_size_, b.mmap_size_);
        }

        MappedBuffer& operator=(MappedBuffer b) {
            unmap();
            std::swap(ptr_, b.ptr_);
            std::swap(size_, b.size_);
            std::swap(mmap_size_, b.mmap_size_);
            return *this;
        }

              char* data()       { return static_cast<char*>(ptr_); }
        const char* data() const { return static_cast<char*>(ptr_); }

        // Return pointer at given absolute offset in the buffer.
              char* data(int64_t off)       { return data() + off % size_; }
        const char* data(int64_t off) const { return data() + off % size_; }

        ssize_t size() const { return size_; }

      private:
        void*   ptr_  = nullptr;
        ssize_t size_ = 0;
        ssize_t mmap_size_ = 0;

        void map(ssize_t size);
        void unmap();
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
    struct ConcurrentPositionSet {
        // Adds a new offset to the position set, set to the current minimum.
        //
        // Returns an integer identifier for the new offset.
        int add_offset() LOCKS_EXCLUDED(Mutex());

        // Remove the given offset from the position set.
        void del_offset(int id) LOCKS_EXCLUDED(Mutex());

        // Increments the given offset by some number of bytes.
        void inc_offset(int id, int nbytes) LOCKS_EXCLUDED(Mutex());

        ssize_t num_offsets() const SHARED_LOCKS_REQUIRED(Mutex()) {
            return offsets_.size();
        }

        // Returns the current value of the given offset.
        std::optional<int64_t> get_offset(int) const LOCKS_EXCLUDED(Mutex());

        // Waits for a given number of bytes to become available.
        //
        // Returns true if the bytes became available or false if all offsets
        // we were tracking were removed.
        bool await_bytes(ssize_t nbytes) const SHARED_LOCKS_REQUIRED(Mutex());

        // Returns the current minimum offset value.
        int64_t min_offset() const {
            return min_offset_.load(std::memory_order_acquire);
        }

        absl::Mutex& Mutex() const LOCK_RETURNED(lock_) { return lock_; }

    private:
        // Atomics aren't movable by default, so we have to wrap one up.  This
        // lets us use it with absl::flat_hash_map which may need to resize and
        // move its contents.  We synchronize access to the offsets table using
        // a mutex so this is safe.
        struct Offset {
            Offset(int64_t value=0) : value_(value) {}
            Offset(Offset&& b) {
                value_.store(
                    b.value_.load(std::memory_order_acquire),
                    std::memory_order_release);
            }

            int64_t value() const {
                return value_.load(std::memory_order_acquire);
            }

            Offset& operator+=(int64_t val) {
                value_.fetch_add(val, std::memory_order_acq_rel);
                return *this;
            }

        private:
            std::atomic<int64_t> value_;
        };

        void update_min_offset() EXCLUSIVE_LOCKS_REQUIRED(Mutex());

        mutable absl::Mutex lock_;
        GUARDED_BY(Mutex()) absl::flat_hash_map<int, Offset> offsets_;
        GUARDED_BY(Mutex()) int oneup_cnt_ = 0;

        std::atomic<int64_t> min_offset_ = 0;
    };

    mutable absl::Mutex lock_ ACQUIRED_BEFORE(readers_.Mutex());
    mutable GUARDED_BY(lock_) MappedBuffer buffer_;
    ConcurrentPositionSet readers_;

    mutable absl::Mutex wroff_lock_ ACQUIRED_AFTER(lock_);
    std::atomic<int64_t> wroffset_ = 0;
    std::atomic<bool>     wropen_   = true;

    bool wropen() const {
        return wropen_.load(std::memory_order_acquire);
    }

    // Returns the current write offset into the buffer.
    int64_t wroffset() const {
        return wroffset_.load(std::memory_order_acquire);
    }

    void wroff_advance(int64_t val) {
        WriterMutexLock lock(&wroff_lock_);
        wroffset_.fetch_add(val, std::memory_order_acq_rel);
    }

    // Returns the current space in bytes available for writing.
    int64_t wravail() const SHARED_LOCKS_REQUIRED(lock_) {
        return buffer_.size() - (wroffset() - readers_.min_offset());
    }

    // Wait for the given number of bytes to become available for writing.  If
    // all the readers are removed before space becomes available, returns -1.
    ssize_t write_wait(ssize_t min_bytes) SHARED_LOCKS_REQUIRED(lock_);

    // Wait for the given number of bytes to become available for reading.  If
    // the writer is closed before space becomes available, returns -1.
    ssize_t read_wait(int64_t offset, ssize_t min_bytes) LOCKS_EXCLUDED(wroff_lock_);
};
