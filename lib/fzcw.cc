// Copyright 2022 Google LLC
// Author: Sean McAllister

#include <fzcw/fzcw.h>

#include <absl/base/attributes.h>

#include <limits>

// Return current wall clock time, in seconds.
inline double stopwatch() {
    struct timespec tv;
    clock_gettime(CLOCK_MONOTONIC, &tv);
    return tv.tv_sec + (double)tv.tv_nsec/1e9;
}

// Return time elapsed since a given start time, in seconds.
inline double stopwatch(double start) {
    return stopwatch()-start;
}

void zstream::MappedBuffer::map(ssize_t size) {
    SPDLOG_DEBUG(ptr_ == nullptr);

    int fd = memfd_create("zstream", 0);
    if (fd < 0) {
        return;
    }

    // Create an anonymous memory mapping of twice the size requested.
    char* ptr = (char*)mmap(0, 2*size, PROT_NONE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if (!ptr) {
        close(fd);
        return;
    }

    // Resize anonymous file to match size requested.
    if (ftruncate(fd, size) < 0) {
        close(fd);
        munmap(ptr, 2*size);
        return;
    }

    // Map the file contents to the bottom half.
    const int kReadWrite = PROT_READ|PROT_WRITE;
    if (!mmap(ptr, size, kReadWrite, MAP_FIXED|MAP_SHARED, fd, 0)) {
        close(fd);
        munmap(ptr, 2*size);
        return;
    }

    // Map the file contents again to the top half.
    if (!mmap(ptr+size, size, kReadWrite, MAP_FIXED|MAP_SHARED, fd, 0)) {
        close(fd);
        munmap(ptr, 2*size);
        return;
    }

    ptr_ = ptr;
    size_ = size;
    mmap_size_ = 2*size;
}

void zstream::MappedBuffer::unmap() {
    if (ptr_) {
        munmap(ptr_, mmap_size_);
    }
    ptr_ = nullptr;
    size_ = 0;
    mmap_size_ = 0;
}

bool zstream::resize(ssize_t nbytes) {
    // Roundup to page size.
    nbytes = (nbytes + FZCW_PAGE_SIZE - 1)/FZCW_PAGE_SIZE*FZCW_PAGE_SIZE;

    if (nbytes <= size()) {
        return true;
    }

    // We need exclusive access to resize the buffer contents, but can just
    // take out a reader lock for notifying waiting writers.
    WriterMutexLock buffer_lock(&lock_);

    // Steal the old mapped buffer, its destructor will unmap it.
    MappedBuffer old_buffer = std::move(buffer_);

    // Try to map a new buffer, if it fails put the old one back.
    MappedBuffer new_buffer(nbytes);
    if (new_buffer.data() == nullptr) {
        buffer_ = std::move(old_buffer);
        return false;
    }

    int64_t wroffset = zstream::wroffset();
    if (old_buffer.data() != nullptr) {
        ssize_t new_size = new_buffer.size();
        ssize_t old_size = old_buffer.size();

        void* dst = new_buffer.data() + (wroffset - old_size) % new_size;
        void* src = old_buffer.data() + (wroffset - old_size) % old_size;
        memcpy(dst, src, old_size);
    }

    // Save the new memory mapped buffer.
    buffer_ = std::move(new_buffer);

    // Releasing the lock on readers_ will notify any waiting writers that
    // there's more space available now.
    return true;
}

ssize_t zstream::write(const void* ptr, ssize_t nbytes) {
    // We're not modifying the buffer object itself, just the memory it points
    // to, so we only need a read lock.
    ReaderMutexLock lock(&lock_);

    // Cast to char pointer so we can do arithmetic.
    const char* cptr = static_cast<const char*>(ptr);
    ssize_t remain = nbytes;
    while (remain) {
        DEBUG(fprintf(stderr, "[w] waiting for more write space\n"));
        ssize_t navail = write_wait(1);
        DEBUG(fprintf(stderr, "[w] done, navail: %ld\n", navail));
        if (navail < 0) {
            break;
        }

        ssize_t nwrite = std::min(remain, navail);
        memcpy(buffer_.data(wroffset()), cptr, nwrite);
        cptr   += nwrite;
        remain -= nwrite;

        // Increment the write offset, this will notify any waiting readers
        // that more data is available.
        DEBUG(fprintf(stderr, "[w] incrementing write offset\n"));
        wroff_advance(nwrite);
    }
    DEBUG(fprintf(stderr, "[w] wrote %ld bytes\n", nbytes-remain));
    return nbytes-remain;
}

// Wait for the given number of bytes to become available for writing.  If
// all the readers are removed before space becomes available, returns -1.
ssize_t zstream::write_wait(ssize_t min_bytes) {
    // Check if we have space already.
    ssize_t navail = wravail();
    if (navail >= min_bytes) {
        return navail;
    }

    // Spin for up to 1ms before falling back to mutex.
    double start = stopwatch();
    do {
        // Throttle how often we hit clock_gettime since it has to call out to
        // to the VDSO.  The empty asm() call here provides a compiler barrier
        // so that this loop isn't optimized out.
        for (int i=0; i < 1000; i++) {
            asm("");
        }

        navail = wravail();
        if (navail >= min_bytes) {
            return navail;
        }
    } while (stopwatch(start) < 1e-3);

    // Finally fall back to using the mutex to sleep.
    ReaderScopeLock readers_lock(&readers_);
    DEBUG(fprintf(stderr, "[w] write avail: %zd\n", navail));
    if (!readers_.await_bytes(min_bytes-navail)) {
        return -1;
    }
    return wravail();
}

ssize_t zstream::read(int id, void* ptr, ssize_t nbytes, ssize_t ncons) {
    if (ncons < 0) {
        ncons = nbytes;
    }

    // Ensure we have room to leave nbytes-ncons bytes in the buffer without
    // blocking the writer.
    if (ncons < nbytes) {
        resize(2*(nbytes-ncons));
    }

    ReaderMutexLock stream_lock(&lock_);
    std::optional<int64_t> ans = readers_.get_offset(id);
    if (!ans) {
        return -1;
    }
    int64_t offset = ans.value();

    // Cast to char pointer so we can do arithmetic.
    char* cptr = static_cast<char*>(ptr);
    ssize_t consumed = 0;
    ssize_t remain   = nbytes;
    while (remain) {
        DEBUG(fprintf(stderr, "[r] waiting for more read space\n"));
        ssize_t navail = read_wait(offset, 1);
        DEBUG(fprintf(stderr, "[r] done, navail: %ld\n", navail));
        if (navail < 0) {
            break;
        }

        ssize_t nread = std::min(navail, remain);

        memcpy(cptr, buffer_.data(offset), nread);
        cptr   += nread;
        remain -= nread;
        offset += nread;

        ssize_t nconsume = std::min(ncons-consumed, nread);
        if (nconsume > 0) {
            readers_.inc_offset(id, nconsume);
            consumed += nconsume;
        }
    }

    DEBUG(fprintf(stderr, "[r] read %ld bytes\n", nbytes-remain));
    return nbytes-remain;
}

ssize_t zstream::read_wait(int64_t offset, ssize_t min_bytes) {
    // const auto readavail = [this, offset]() {
    //     return wroffset() - offset;
    // };

    //ssize_t navail = readavail();
    ssize_t navail = wroffset() - offset;
    if (navail >= min_bytes) {
        return navail;
    }

    // Spin for up to 1ms before falling back to mutex.
    double start = stopwatch();
    do {
        // Throttle how often we hit clock_gettime since it has to call out to
        // to the VDSO.  The empty asm() call here provides a compiler barrier
        // so that this loop isn't optimized out.
        for (int i=0; i < 1000; i++) {
            asm("");
        }

        navail = wroffset() - offset;
        if (navail >= min_bytes) {
            return navail;
        }
    } while (stopwatch(start) < 1e-3);

    // Finally fall back to using the mutex to wait.
    wroff_lock_.ReaderLock();
    {
        const int64_t min_offset = offset + (min_bytes - navail);
        auto ready = [this, min_offset]() {
            DEBUG(wroff_lock_.AssertReaderHeld());
            return wroffset() >= min_offset || !wropen();
        };
        wroff_lock_.Await(absl::Condition(&ready));
    }
    wroff_lock_.ReaderUnlock();

    // If we woke back up because the writer closed, return error.
    if (!wropen()) {
        return -1;
    }
    return wroffset() - offset;
}

int zstream::ConcurrentPositionSet::add_offset() {
    WriterScopeLock lock(this);
    offsets_.emplace(oneup_cnt_, min_offset());
    return oneup_cnt_++;
}

void zstream::ConcurrentPositionSet::del_offset(int id) {
    WriterScopeLock lock(this);

    auto iter = offsets_.find(id);
    if (iter == offsets_.end()) {
        return;
    }

    int64_t old_offset = iter->second.value();

    // If our old offset matches the minimum offset we might have been the
    // bottleneck, update the minimum offset value.
    if (old_offset == min_offset()) {
        update_min_offset();
    }
}

void zstream::ConcurrentPositionSet::update_min_offset() {
    int64_t min_offset = std::numeric_limits<int64_t>::max();
    for (const auto& pair : offsets_) {
        min_offset = std::min(min_offset, pair.second.value());
    }
    min_offset_.store(min_offset, std::memory_order_release);
}

void zstream::ConcurrentPositionSet::inc_offset(int id, int nbytes) {
    lock_.ReaderLock();

    auto iter = offsets_.find(id);
    if (iter == offsets_.end()) {
        lock_.ReaderUnlock();
        return;
    }

    int64_t old_offset = iter->second.value();
    iter->second += nbytes;

    // If our old offset matches the minimum offset we might have been the
    // bottleneck, update the minimum offset value.
    if (old_offset == min_offset()) {
        // Upgrade to an exclusive lock.
        lock_.ReaderUnlock();
        lock_.WriterLock();

        // Make sure minimum wasn't updated while we waited.
        if (old_offset == min_offset()) {
            update_min_offset();
        }

        // Releasing lock will wake any threads waiting on data.
        lock_.WriterUnlock();
        return;
    }
    lock_.ReaderUnlock();
}

std::optional<int64_t> zstream::ConcurrentPositionSet::get_offset(int id) const {
    ReaderMutexLock lock(&lock_);
    auto iter = offsets_.find(id);
    if (iter == offsets_.end()) {
        return {};
    }
    return iter->second.value();
}

bool zstream::ConcurrentPositionSet::await_bytes(ssize_t nbytes) const {
    DEBUG(Mutex().AssertReaderHeld());
    if (offsets_.empty()) {
        return false;
    }

    int64_t old_offset = min_offset();
    const auto bytes_ready = [this, old_offset, nbytes]() {
        DEBUG(Mutex().AssertReaderHeld());
        return (min_offset() - old_offset >= nbytes) || offsets_.empty();
    };
    lock_.Await(absl::Condition(&bytes_ready));

    // We stopped either because we got bytes or all the offsets were removed,
    // return false in the second case.
    return !offsets_.empty();
}
