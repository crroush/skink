// Copyright 2022 Google LLC
// Author: Sean McAllister

#include <absl/base/attributes.h>
#include <skink/memcpy.h>
#include <skink/zstream.h>

#include <limits>

namespace sk {

zstream::~zstream() {
  // Close write end of the pipe.
  wrclose();

  // Wait for readers to detach.
  const auto no_readers = [this]() {
    DEBUG(reader_lock_.AssertReaderHeld());
    return readers_.empty();
  };

  reader_lock_.WriterLock();
  reader_lock_.Await(absl::Condition(&no_readers));
  reader_lock_.WriterUnlock();
}

void zstream::MappedBuffer::map(ssize_t size) {
  DCHECK(ptr_ == nullptr);

  int fd = memfd_create("zstream", 0);
  if (fd < 0) {
    return;
  }

  // Create an anonymous memory mapping of twice the size requested.
  char *ptr =
      (char *)mmap(0, 2 * size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (!ptr) {
    close(fd);
    return;
  }

  // Resize anonymous file to match size requested.
  if (ftruncate(fd, size) < 0) {
    close(fd);
    munmap(ptr, 2 * size);
    return;
  }

  // Map the file contents to the bottom half.
  const int kReadWrite = PROT_READ | PROT_WRITE;
  if (!mmap(ptr, size, kReadWrite, MAP_FIXED | MAP_SHARED, fd, 0)) {
    close(fd);
    munmap(ptr, 2 * size);
    return;
  }

  // Map the file contents again to the top half.
  if (!mmap(ptr + size, size, kReadWrite, MAP_FIXED | MAP_SHARED, fd, 0)) {
    close(fd);
    munmap(ptr, 2 * size);
    return;
  }

  ptr_       = ptr;
  size_      = size;
  mmap_size_ = 2 * size;
}

void zstream::MappedBuffer::unmap() {
  if (ptr_) {
    munmap(ptr_, mmap_size_);
  }
  ptr_       = nullptr;
  size_      = 0;
  mmap_size_ = 0;
}

bool zstream::resize(ssize_t nbytes) {
  // Roundup to page size.
  nbytes = (nbytes + SK_PAGE_SIZE - 1) / SK_PAGE_SIZE * SK_PAGE_SIZE;

  if (nbytes <= size()) {
    return true;
  }

  // Take out an exclusive lock on the buffer to resize it.
  absl::WriterMutexLock lock(&buffer_lock_);

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

    int ncopy = wroffset_ - min_read_offset_;
    DCHECK(ncopy <= wroffset_);

    void *dst = new_buffer.data() + (wroffset - ncopy) % new_size;
    void *src = old_buffer.data() + (wroffset - ncopy) % old_size;
    __folly_memcpy(dst, src, old_size);
  }

  // Save the new memory mapped buffer.
  buffer_ = std::move(new_buffer);

  // Releasing the lock on readers_ will notify any waiting writers that
  // there's more space available now.
  absl::ReaderMutexLock rdlock(&reader_lock_);
  return true;
}

ssize_t zstream::write(const void *ptr, ssize_t nbytes) {
  absl::ReaderMutexLock lock(&buffer_lock_);

  // Cast to char pointer so we can do arithmetic.
  const char *cptr = static_cast<const char *>(ptr);
  ssize_t remain   = nbytes;
  while (remain) {
    ssize_t navail = await_write_space(1);
    if (navail < 0) {
      break;
    }

    ssize_t nwrite = std::min(remain, navail);
    __folly_memcpy(buffer_.data(wroffset_), cptr, nwrite);
    cptr += nwrite;
    remain -= nwrite;

    // Increment the write offset, this will notify any waiting readers
    // that more data is available.
    wroffset_ += nwrite;
  }
  return nbytes - remain;
}

void *zstream::wborrow(ssize_t size) {
  DCHECK(size > 0);
  resize(2 * size);

  buffer_lock_.ReaderLock();
  ssize_t navail = await_write_space(size);
  if (navail < size) {
    buffer_lock_.ReaderUnlock();
    return nullptr;
  }

  // Note we intentionally leave buffer_lock_ locked here.
  return buffer_.data(wroffset_);
}

void zstream::wrelease(ssize_t size) {
  DEBUG(buffer_lock_.AssertReaderHeld());
  wroffset_ += size;
  buffer_lock_.ReaderUnlock();
}

ssize_t zstream::await_write_space(ssize_t min_bytes) {
  // No readers, allow free-wheeling write.
  if (min_read_offset_.closed()) {
    return buffer_.size();
  }

  // Check if we have space available already.
  ssize_t navail = wravail();
  if (navail >= min_bytes) {
    return navail;
  }

  // Spin a little bit to see if the space becomes available.
  const int limit = spin_limit_;
  for (int i = 0; i < limit && navail < min_bytes; i++) {
    navail = wravail();
  }

  // Finally fall back to using the mutex to sleep.
  int64_t wroffset = wroffset_;
  int64_t rdoffset = min_read_offset_;

  navail = buffer_.size() - (wroffset - rdoffset);
  if (navail < min_bytes) {
    rdoffset = min_read_offset_.AwaitGe(rdoffset + (min_bytes - navail));
    navail   = buffer_.size() - (wroffset - rdoffset);
  }
  return navail;
}

ssize_t zstream::read(int id, void *ptr, ssize_t nbytes, ssize_t ncons) {
  if (ncons < 0) {
    ncons = nbytes;
  }

  // Ensure we have room to leave nbytes-ncons bytes in the buffer without
  // blocking the writer.
  if (ncons < nbytes) {
    resize(2 * (nbytes - ncons));
  }

  absl::ReaderMutexLock lock0(&buffer_lock_);

  // Lookup current offset value.
  int64_t offset;
  {
    absl::ReaderMutexLock lock1(&reader_lock_);
    auto iter = readers_.find(id);
    if (iter == readers_.end()) {
      return -1;
    }
    offset = iter->second.value();
  }

  // Cast to char pointer so we can do arithmetic.
  char *cptr       = static_cast<char *>(ptr);
  ssize_t consumed = 0;
  ssize_t remain   = nbytes;
  while (remain) {
    ssize_t navail = await_data(offset, 1);
    if (navail <= 0) {
      break;
    }

    ssize_t nread = std::min(navail, remain);
    __folly_memcpy(cptr, buffer_.data(offset), nread);
    cptr += nread;
    remain -= nread;
    offset += nread;

    ssize_t nconsume = std::min(ncons - consumed, nread);
    if (nconsume > 0) {
      inc_reader(id, nconsume);
      consumed += nconsume;
    }
  }

  return nbytes - remain;
}

bool zstream::skip(int id, ssize_t nbytes) {
  if (nbytes > 0) {
    absl::ReaderMutexLock lock0(&buffer_lock_);
    inc_reader(id, nbytes);
  }
  return true;
}

sizeptr<const void> zstream::rborrow(int id, ssize_t size) {
  // Make sure the buffer is large enough to service the borrow.
  DCHECK(size > 0);
  resize(2 * size);

  buffer_lock_.ReaderLock();

  int64_t offset;
  {
    absl::ReaderMutexLock lock(&reader_lock_);
    auto iter = readers_.find(id);
    if (iter == readers_.end()) {
      buffer_lock_.ReaderUnlock();
      return {};
    }
    offset = iter->second.value();
  }

  ssize_t navail = await_data(offset, size);
  if (navail < size) {
    // Not enough data, report what we could borrow.
    buffer_lock_.ReaderUnlock();
    return { nullptr, navail };
  }

  // Note we intentionally leave buffer_lock_ locked here.
  return { buffer_.data(offset), size };
}

void zstream::rrelease(int id, ssize_t size) {
  DEBUG(buffer_lock_.AssertReaderHeld());
  buffer_lock_.ReaderUnlock();
  skip(id, size);
}

ssize_t zstream::await_data(int64_t offset, ssize_t min_bytes) {
  // See if we have data available already.
  ssize_t navail = rdavail(offset);
  if (navail >= min_bytes || wrclosed()) {
    return navail;
  }

  // Spin briefly before falling back to mutex.
  const int limit = spin_limit_;
  for (int i = 0; i < limit && navail < min_bytes; i++) {
    navail = rdavail(offset);
  }

  // Finally fall back to using the mutex to wait.
  navail = rdavail(offset);
  if (navail < min_bytes) {
    int64_t min_offset = offset + (min_bytes - navail);

    buffer_lock_.ReaderUnlock();
    navail = wroffset_.AwaitGe(min_offset) - offset;
    buffer_lock_.ReaderLock();
  }
  return navail;
}

int zstream::add_reader() {
  absl::WriterMutexLock lock(&reader_lock_);

  bool no_readers = readers_.empty();

  // Position the reader at the current minimum read offset or at the current
  // current write position if there are no other readers.
  int64_t offset = min_read_offset_;
  if (no_readers) {
    offset = wroffset_;
  }
  readers_.emplace(reader_oneup_, offset);

  // Update the minimum read offset if there were no other readers until now.
  if (no_readers) {
    min_read_offset_.open();
    min_read_offset_ = offset;
  }

  return reader_oneup_++;
}

void zstream::del_reader(int id) {
  absl::WriterMutexLock lock(&reader_lock_);

  auto iter = readers_.find(id);
  if (iter == readers_.end()) {
    return;
  }
  int64_t offset = iter->second;
  readers_.erase(iter);

  // Update the minimum read offset in case this reader was the blocker.
  if (!readers_.empty() && offset == min_read_offset_) {
    reader_scan_lock_.lock();
    if (offset == min_read_offset_) {
      int64_t min_offset = std::numeric_limits<int64_t>::max();
      for (const auto &pair : readers_) {
        min_offset = std::min(min_offset, pair.second.value());
      }
      min_read_offset_.SetMax(min_offset);
    }
    reader_scan_lock_.unlock();
  } else {
    // Mark minimum read offset as closed to allow writer to free wheel.
    min_read_offset_.close();
  }
}

void zstream::inc_reader(int id, int64_t nbytes) {
  absl::ReaderMutexLock lock(&reader_lock_);
  auto iter = readers_.find(id);
  if (iter == readers_.end()) {
    return;
  }

  // Read and increment the current offset value.
  int64_t offset = iter->second.value();
  iter->second   = offset + nbytes;

  // If our offset is equal to the minimum offset then we might be the slow
  // reader so we need to recompute the minimum read offset and notify the
  // writer that there's (maybe) more space available.
  //
  // We have to serialize the individual readers when they scan the offset
  // table, otherwise there's potential for a race condition.
  //
  // If we have two readers both at the minimum value X: [X, X] and both
  // incrementing to a new value Y: [Y, Y], it's possible for the readers
  // readers to see [Y, X], or [X, Y] and recompute a minimum value of X
  // again.  By serializaing them, one or the other is guaranteed to see both
  // changes and compute the correct value.
  //
  // This extends to N minimum readers, one will always be the last in the
  // serialization order and compute the correct minimum value.
  reader_scan_lock_.lock();
  if (offset == min_read_offset_) {
    // Compute new minimum offset.
    int64_t min_offset = std::numeric_limits<int64_t>::max();
    for (const auto &pair : readers_) {
      min_offset = std::min(min_offset, pair.second.value());
    }
    min_read_offset_.SetMax(min_offset);
  }
  reader_scan_lock_.unlock();
}

}  // namespace sk
