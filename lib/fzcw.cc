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

    // Take out an exclusive lock on the buffer to resize it.
    WriterMutexLock lock(&buffer_lock_);

    // Steal the old mapped buffer, its destructor will unmap it.
    MappedBuffer old_buffer = std::move(buffer_);

    // Try to map a new buffer, if it fails put the old one back.
    MappedBuffer new_buffer(nbytes);
    if (new_buffer.data() == nullptr) {
        buffer_ = std::move(old_buffer);
        return false;
    }

    int64_t wroffset = wroffset_;
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
    buffer_lock_.ReaderLock();

    // Cast to char pointer so we can do arithmetic.
    const char* cptr = static_cast<const char*>(ptr);
    ssize_t remain = nbytes;
    while (remain) {
        DEBUG(fprintf(stderr, "[w] waiting for more write space\n"));
        ssize_t navail = await_write_space(1);
        DEBUG(fprintf(stderr, "[w] done, navail: %ld\n", navail));
        if (navail < 0) {
            break;
        }

        ssize_t nwrite = std::min(remain, navail);
        memcpy(buffer_.data(wroffset_), cptr, nwrite);
        cptr   += nwrite;
        remain -= nwrite;

        // Increment the write offset, this will notify any waiting readers
        // that more data is available.
        DEBUG(fprintf(stderr, "[w] incrementing write offset\n"));
        wroffset_ += nwrite;
    }
    DEBUG(fprintf(stderr, "[w] wrote %ld bytes\n", nbytes-remain));
    buffer_lock_.ReaderUnlock();
    return nbytes-remain;
}

ssize_t zstream::await_write_space(ssize_t min_bytes) {
    // Check if we have space already.
    ssize_t navail = wravail();
    DEBUG(fprintf(stderr, "[w] navail: %zd\n", navail));
    if (navail >= min_bytes) {
        return navail;
    }

    // Spin for up to 1ms before falling back to mutex.
    DEBUG(fprintf(stderr, "[w] spinning\n"));
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
    } while (stopwatch(start) < spin_limit_.load(std::memory_order_acquire));

    // Finally fall back to using the mutex to sleep.
    DEBUG(fprintf(stderr, "[w] sleeping\n"));
    int64_t wroffset = wroffset_;
    int64_t minread  = min_read_offset_;
    navail = buffer_.size() - (wroffset - minread);
    if (navail < min_bytes) {
        int64_t min_offset = minread + (min_bytes-navail);
        DEBUG(fprintf(stderr, "[w] buffer size: %zd  write offset: %zd   min_rd_offset: %zd\n", buffer_.size(), (int64_t)wroffset_, (int64_t)min_read_offset_));
        DEBUG(fprintf(stderr, "[w] waiting until offset: %zd\n", min_offset));

        buffer_lock_.ReaderUnlock();
        min_read_offset_.AwaitGe(min_offset);
        buffer_lock_.ReaderLock();

        navail = wravail();
        DEBUG(fprintf(stderr, "[w] awake, navail: %zd\n", navail));
    }
    return navail;
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

    ReaderMutexLock lock(&buffer_lock_);
    int64_t offset;
    {
        ReaderMutexLock lock(&reader_lock_);
        auto iter = readers_.find(id);
        if (iter == readers_.end()) {
            return -1;
        }
        offset = iter->second.value();
    }

    // Cast to char pointer so we can do arithmetic.
    char* cptr = static_cast<char*>(ptr);
    ssize_t consumed = 0;
    ssize_t remain   = nbytes;
    while (remain) {
        DEBUG(fprintf(stderr, "[r] waiting for more read space\n"));
        ssize_t navail = await_data(offset, 1);
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
            DEBUG(fprintf(stderr, "[r] incrementing read offset\n"));
            inc_reader(id, nconsume);
            consumed += nconsume;
        }
    }

    DEBUG(fprintf(stderr, "[r] read %ld bytes\n", nbytes-remain));
    return nbytes-remain;
}

ssize_t zstream::await_data(int64_t offset, ssize_t min_bytes) {
    // See if we have data available already.
    ssize_t navail = rdavail(offset);
    DEBUG(fprintf(stderr, "[r] navail: %zd\n", navail));
    if (navail >= min_bytes) {
        return navail;
    }

    // Spin for up to 1ms before falling back to mutex.
    DEBUG(fprintf(stderr, "[r] spinning\n"));
    double start = stopwatch();
    do {
        // Throttle how often we hit clock_gettime since it has to call out to
        // to the VDSO.  The empty asm() call here provides a compiler barrier
        // so that this loop isn't optimized out.
        for (int i=0; i < 1000; i++) {
            asm("");
        }

        navail = rdavail(offset);
        if (navail >= min_bytes) {
            return navail;
        }
    } while (stopwatch(start) < spin_limit_.load(std::memory_order_acquire));

    // Finally fall back to using the mutex to wait.
    DEBUG(fprintf(stderr, "[r] sleeping\n"));
    navail = rdavail(offset);

    DEBUG(fprintf(stderr, "[r] write offset: %zd  offset: %zd  navail: %zd\n", (int64_t)wroffset_, (int64_t)offset, navail));
    if (navail < min_bytes) {
        int64_t min_offset = offset + (min_bytes - navail);
        DEBUG(fprintf(stderr, "[r] waiting for offset: %zd\n", min_offset));
        buffer_lock_.ReaderUnlock();
        navail = wroffset_.AwaitGe(min_offset) - offset;
        buffer_lock_.ReaderLock();
        DEBUG(fprintf(stderr, "[r] awake, navail: %zd\n", navail));

        // If we woke back up because the writer closed, return error.
        if (!wropen()) {
            return -1;
        }
    }
    return navail;
}

int zstream::add_reader() {
    WriterMutexLock lock(&reader_lock_);
    readers_.emplace(reader_oneup_, static_cast<int64_t>(min_read_offset_));
    return reader_oneup_++;
}

void zstream::del_reader(int id) {
    WriterMutexLock lock(&reader_lock_);

    auto iter = readers_.find(id);
    if (iter == readers_.end()) {
        return;
    }
    readers_.erase(iter);

    // If our old offset matches the minimum offset we might have been the
    // bottleneck, update the minimum offset value.
    min_read_offset_.Lock();
    int64_t min = std::numeric_limits<int64_t>::max();
    for (const auto& pair: readers_) {
        min = std::min(min, pair.second.value());
    }
    min_read_offset_.set_atomic(min);
    min_read_offset_.Unlock();
}

void zstream::inc_reader(int id, int64_t nbytes) {
    ReaderMutexLock lock(&reader_lock_);

    auto iter = readers_.find(id);
    if (iter == readers_.end()) {
        return;
    }

    // Increment the offset.
    int64_t offset = iter->second;
    iter->second = offset + nbytes;

    // If our old offset matches the minimum offset we might have been the
    // bottleneck, update the minimum offset value.
    if (min_read_offset_ == offset) {
        min_read_offset_.Lock();
        if (min_read_offset_ == offset) {
            int64_t min = std::numeric_limits<int64_t>::max();
            for (const auto& pair: readers_) {
                min = std::min(min, pair.second.value());
            }
            DEBUG(fprintf(stderr, "[r] new minimum: %zd\n", min));
            DEBUG(fprintf(stderr, "[r] incrementing minimum read offset\n"));
            min_read_offset_.set_atomic(min);
        }
        min_read_offset_.Unlock();
    }
}

// void zstream::set_minrdoff(int64_t offset) {
//     WriterMutexLock lock(&min_rdoff_lock_);
//     min_read_offset_ = offset;
// }

// std::optional<int64_t> zstream::ConcurrentPositionSet::get_offset(int id) const {
//     ReaderMutexLock lock(&lock_);
//     auto iter = readers_.find(id);
//     if (iter == readers_.end()) {
//         return {};
//     }
//     return iter->second.value();
// }

// bool zstream::ConcurrentPositionSet::await_bytes(ssize_t nbytes) const {
//     DEBUG(Mutex().AssertReaderHeld());
//     if (readers_.empty()) {
//         return false;
//     }

//     int64_t next_offset = min_offset() + nbytes;
//     const auto bytes_ready = [this, next_offset]() {
//         DEBUG(Mutex().AssertReaderHeld());
//         return min_offset() >= next_offset || readers_.empty();
//     };
//     lock_.Await(absl::Condition(&bytes_ready));

//     // We stopped either because we got bytes or all the offsets were removed,
//     // return false in the second case.
//     return !readers_.empty();
// }
