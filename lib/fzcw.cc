// Copyright 2022 Google LLC
// Author: Sean McAllister

#include <fzcw/fzcw.h>

#include <limits>

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
    ReaderScopeLock reader_lock(&readers_);

    // Steal the old mapped buffer, its destructor will unmap it.
    MappedBuffer old_buffer = std::move(buffer_);

    // Try to map a new buffer, if it fails put the old one back.
    MappedBuffer new_buffer(nbytes);
    if (new_buffer.data() == nullptr) {
        buffer_ = std::move(old_buffer);
        return false;
    }

    uint64_t wroffset = writer_.get_offset(0);
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
        ReaderScopeLock readers_lock(&readers_);
        ssize_t navail = write_wait(1);
        if (navail < 0) {
            break;
        }

        ssize_t nwrite = std::min(remain, navail);
        memcpy(wrptr(), cptr, nwrite);
        cptr   += nwrite;
        remain -= nwrite;

        // Increment the write offset, this will notify any waiting readers
        // that more data is available.
        writer_.inc_offset(0, nwrite);
    }
    return nbytes-remain;
}

// Wait for the given number of bytes to become available for writing.  If
// all the readers are removed before space becomes available, returns -1.
ssize_t zstream::write_wait(ssize_t min_bytes) {
    if (readers_.num_offsets() == 0) {
        return -1;
    }

    ssize_t navail = wravail();
    while (navail < min_bytes) {
        readers_.await_bytes(min_bytes);
        if (readers_.num_offsets() == 0) {
            return -1;
        }
    }
    return navail;
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

    uint64_t old_offset = iter->second.value();

    // If our old offset matches the minimum offset we might have been the
    // bottleneck, update the minimum offset value.
    if (old_offset == min_offset()) {
        update_min_offset();
    }
}

void zstream::ConcurrentPositionSet::update_min_offset() {
    uint64_t min_offset = std::numeric_limits<uint64_t>::max();
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

    uint64_t old_offset = iter->second.value();
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

uint64_t zstream::ConcurrentPositionSet::get_offset(int id) const {
    ReaderScopeLock lock(this);
    auto iter = offsets_.find(id);
    if (iter == offsets_.end()) {
        return 0;
    }
    return iter->second.value();
}

bool zstream::ConcurrentPositionSet::await_bytes(int nbytes) const {
    uint64_t old_offset = min_offset();
    auto bytes_ready = [this, old_offset, nbytes]() {
        SPDLOG_DEBUG(lock_.AssertReaderHeld());
        return (min_offset() - old_offset >= nbytes) || offsets_.empty();
    };
    lock_.Await(absl::Condition(&bytes_ready));

    // We stopped either because we got bytes or all the offsets were removed,
    // return false in the second case.
    return !offsets_.empty();
}
