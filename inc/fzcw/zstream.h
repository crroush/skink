// Copyright 2022 Google LLC
// Author: Sean McAllister

#pragma once

#include <atomic>
#include <algorithm>
#include <memory>
#include <optional>

#include <fzcw/sizeptr.h>

// third party libraries
#include <absl/base/optimization.h>
#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>
#include <spdlog/spdlog.h>

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

// Get page size once at process start.
static const int FZCW_PAGE_SIZE = getpagesize();

template <typename Tinp> struct zcwriter;
template <typename Tout> struct zcreader;

struct zstream {
    // 64K is a reasonable default size that balances performance and memory.
    static constexpr size_t kDefaultSize = 65536;

    zstream(size_t size=kDefaultSize)
        : buffer_(size) {}

    ~zstream();

    // Returns current size of buffer, in bytes.
    ssize_t size() const LOCKS_EXCLUDED(buffer_lock_) {
        DCHECK(!failed());
        absl::ReaderMutexLock lock(&buffer_lock_);
        return buffer_.size();
    }

    // Returns true if the zcbuffer has failed to map memory somehow.
    bool failed() const LOCKS_EXCLUDED(buffer_lock_) {
        absl::ReaderMutexLock lock(&buffer_lock_);
        return buffer_.data() == nullptr;
    }

    // Close the write end of the buffer, signaling no more data.
    void wrclose() {
        wroffset_.close();
    }

    // Adds a reader to the buffer and returns an integer identifying it.
    int add_reader() LOCKS_EXCLUDED(reader_lock_);

    // Removes a given reader from the buffer.  Noop if no such reader exists.
    void del_reader(int id) LOCKS_EXCLUDED(reader_lock_);

    // Ensures that the buffer is large enough that it can accommodate borrowing
    // memory of at least the given size bytes.  The buffer size is never shrunk
    // so this may be a noop if it is already large enough.
    //
    // Returns true on success, false if the buffer couldn't be resized.
    bool resize(ssize_t nbytes) LOCKS_EXCLUDED(buffer_lock_);

    // Writes bytes to the stream.  If not enough space is available, blocks
    // until all the data is written.  If all the readers are removed before
    // finishing the write, then less data than requested may be written.
    //
    // Returns number of bytes actually written (-1 on error).
    ssize_t write(const void* ptr, ssize_t nbytes) LOCKS_EXCLUDED(buffer_lock_);

    // Reads a given number of bytes from the buffer using the given reader
    // offset.  Blocks until all data requested is read, unless the writer is
    // closed, in which case returns early.
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
    ssize_t read(int id, void* ptr, ssize_t nbytes, ssize_t ncons=-1) LOCKS_EXCLUDED(buffer_lock_);

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

    // An atomic integer that's movable so it can be used in containers.  This
    // wraps a std::atomic<int64_t> and allows accessing it with acquire/release
    // semantics by default, to avoid having to specify memory ordering
    // everywhere.  This is aligned and padded so that each one will take up
    // one cacheline to avoid destructive sharing.
    //
    // X86 has acquire/release semantics natively so this has no overhead there.
    struct AtomicInt64 {
        AtomicInt64(int64_t value=0)
            : value_(value) {}

        AtomicInt64(AtomicInt64&& b) {
            *this = static_cast<int64_t>(b);
        }

        int64_t value() const {
            return static_cast<int64_t>(*this);
        }

        operator int64_t() const {
            return value_.load(std::memory_order_acquire);
        }

        AtomicInt64& operator=(int64_t value) {
            value_.store(value, std::memory_order_release);
            return *this;
        }
    private:
        std::atomic<int64_t> value_;
    };

    // Offsets are half-synchronized atomic values.  This means we rely on the
    // semantics of std::atomic to allow reading of the current value without
    // holding a mutex.  Modifying the value, however always requires holding an
    // exclusive lock.
    //
    // This allows us to safely use the value, but only synchronize via the
    // mutex when we need to wait on the offset to reach a certain value.
    //
    // We take advantage of this in several places to perform double-check
    // locking.  We can check the value first without the mutex, then if needed,
    // take out the mutex, check it again and proceed.
    struct Offset {
        // Sentinel value indicating this offset has been shut down.
        static constexpr int64_t kClosed = -1;

        Offset(int64_t value=0)
            : value_(value) {}

        Offset(const Offset& b) LOCKS_EXCLUDED(lock_, b.lock_)
            : value_(static_cast<int64_t>(b)) {}

        Offset(Offset&& b) LOCKS_EXCLUDED(lock_, b.lock_)
            : value_(static_cast<int64_t>(b)) {}

        // Get the underlying value atomicly without a lock.
        int64_t value() const NO_THREAD_SAFETY_ANALYSIS {
            return value_;
        }

        // Mark the offset as closed, no further updates are allowed.
        void close() LOCKS_EXCLUDED(lock_) {
            absl::WriterMutexLock lock(&lock_);
            set_atomic(kClosed);
        }

        // Returns true if the offset has been shutdown.
        bool closed() const {
            return value() == kClosed;
        }

        // Set the underlying value atomicly.  Requires that the lock is held.
        void set_atomic(int64_t value) EXCLUSIVE_LOCKS_REQUIRED(lock_) {
            value_ = value;
        }

        operator int64_t() const {
            return value();
        }

        Offset& operator=(int64_t value) LOCKS_EXCLUDED(lock_) {
            absl::WriterMutexLock lock(&lock_);
            value_ = value;
            return *this;
        }

        Offset& operator+=(int64_t val) LOCKS_EXCLUDED(lock_) {
            DCHECK(!closed());
            absl::WriterMutexLock lock(&lock_);
            value_ = value_ + val;
            return *this;
        }

        // Set the value to v if v is greater than the current value, otherwise
        // do nothing.  Return the current value.
        int64_t SetMax(int64_t v) LOCKS_EXCLUDED(lock_) {
            int64_t curval = value();
            if (v <= curval) {
                return curval;
            }

            lock_.WriterLock();
            curval = value();
            if (v > curval) {
                value_ = v;
                curval = v;
            }
            lock_.WriterUnlock();
            return curval;
        }

        // Blocks until the value is >= v, or if the offset was closed.  If
        // the value is already >= v, then no lock is taken.
        //
        // Returns the current value which is always >= v or kClosed.
        int64_t AwaitGe(int64_t v) const LOCKS_EXCLUDED(lock_) {
            int64_t curval = value();
            if (curval >= v || curval == kClosed) {
                return curval;
            }

            lock_.ReaderLock();
            curval = value_;
            if (curval < v) {
                const auto ready = [this, v]() {
                    DEBUG(lock_.AssertReaderHeld());
                    return value_ >= v || value_ == kClosed;
                };
                lock_.Await(absl::Condition(&ready));
                curval = value_;
            }
            lock_.ReaderUnlock();
            return curval;
        }

        void Lock() const EXCLUSIVE_LOCK_FUNCTION(lock_) {
            lock_.WriterLock();
        }

        void Unlock() const UNLOCK_FUNCTION(lock_) {
            lock_.WriterUnlock();
        }

    private:
        mutable absl::Mutex lock_;
        GUARDED_BY(lock_) AtomicInt64 value_;
    };

    ABSL_CACHELINE_ALIGNED mutable absl::Mutex buffer_lock_;
    ABSL_CACHELINE_ALIGNED mutable absl::Mutex reader_lock_;
    ABSL_CACHELINE_ALIGNED Offset wroffset_ = 0;
    ABSL_CACHELINE_ALIGNED Offset min_read_offset_ = 0;

    GUARDED_BY(buffer_lock_) MappedBuffer buffer_;
    GUARDED_BY(reader_lock_) absl::flat_hash_map<int, AtomicInt64> readers_;
    GUARDED_BY(reader_lock_) int reader_oneup_ = 0;

    // Returns true if the stream is open for writing.
    bool wropen() {
        return !wroffset_.closed();
    }

    void inc_reader(int id, int64_t nbytes) LOCKS_EXCLUDED(reader_lock_);

    // Return current space in bytes available for writing.
    int64_t wravail() const SHARED_LOCKS_REQUIRED(buffer_lock_) {
        return buffer_.size() - (wroffset_ - min_read_offset_);
    }

    // Return the number of bytes available for reading starting at offset.
    int64_t rdavail(int64_t offset) const {
        return wroffset_ - offset;
    }

    // Block until the given number of bytes are available for writing.  If all
    // the readers are removed before space becomes available, returns -1.
    ssize_t await_write_space(ssize_t min_bytes) SHARED_LOCKS_REQUIRED(buffer_lock_);

    // Wait for the given number of bytes to become available for reading.  If
    // the writer is closed before space becomes available, returns -1.
    ssize_t await_data(int64_t offset, ssize_t min_bytes) SHARED_LOCKS_REQUIRED(buffer_lock_);
};
