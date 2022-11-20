// Copyright 2022 Google LLC
// Author: Sean McAllister

#include <fzcw/fzcw.h>

#include <limits>

bool zcstream::mmap_buffer(size_t size) {
    int fd = memfd_create("zcstream", 0);
    if (fd < 0) {
        return false;
    }

    // Create an anonymous memory mapping of twice the size requested.
    char* ptr = (char*)mmap(0, 2*size, PROT_NONE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    if (!ptr) {
        close(fd);
        return false;
    }

    // Resize anonymous file to match size requested.
    if (ftruncate(fd, size) < 0) {
        close(fd);
        munmap(ptr, 2*size);
        return false;
    }

    // Map the file contents to the bottom half.
    const int kReadWrite = PROT_READ|PROT_WRITE;
    if (!mmap(ptr, size, kReadWrite, MAP_FIXED|MAP_SHARED, fd, 0)) {
        close(fd);
        munmap(ptr, 2*size);
        return false;
    }

    // Map the file contents again to the top half.
    if (!mmap(ptr+size, size, kReadWrite, MAP_FIXED|MAP_SHARED, fd, 0)) {
        close(fd);
        munmap(ptr, 2*size);
        return false;
    }

    buffer_ = MappedSizePtr(ptr, size, 2*size);
    return true;
}

int zcstream::ConcurrentPositionSet::add_offset() {
    absl::WriterMutexLock lock(&lock_);
    offsets_.emplace(oneup_cnt_, min_offset());
    return oneup_cnt_++;
}

void zcstream::ConcurrentPositionSet::del_offset(int id) {
    absl::WriterMutexLock lock(&lock_);
    offsets_.erase(id);
}

void zcstream::ConcurrentPositionSet::inc_offset(int id, int nbytes) {
    lock_.ReaderLock();

    auto iter = offsets_.find(id);
    if (iter == offsets_.end()) {
        lock_.ReaderUnlock();
        return;
    }

    uint64_t old_offset = iter->second;
    iter->second += nbytes;

    // If our old offset matches the minimum offset we might have been the
    // bottleneck, update the minimum offset value.
    if (old_offset == min_offset()) {
        // Upgrade to an exclusive lock.
        lock_.ReaderUnlock();
        lock_.WriterLock();

        // Make sure minimum wasn't updated while we waited.
        if (old_offset == min_offset()) {
            uint64_t min_offset = std::numeric_limits<uint64_t>::max();
            for (const auto pair : offsets_) {
                min_offset = std::min(min_offset, pair.second);
            }
            min_offset_.store(min_offset, std::memory_order_release);
        }

        // Releasing lock will wake any threads waiting on data.
        lock_.WriterUnlock();
        return;
    }
    lock_.ReaderUnlock();
}

bool zcstream::ConcurrentPositionSet::await_bytes(int nbytes) const {
    lock_.ReaderLock();

    uint64_t old_offset = min_offset();
    auto bytes_ready = [this,old_offset,nbytes]() {
        lock_.AssertReaderHeld();
        return (min_offset() - old_offset >= nbytes) || offsets_.empty();
    };
    lock_.Await(absl::Condition(&bytes_ready));

    // We stopped either because we got bytes or all the offsets were removed,
    // return false in the second case.
    bool empty = offsets_.empty();
    lock_.ReaderUnlock();
    return !empty;
}
