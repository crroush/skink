// Copyright 2022 Google LLC
// Author: Sean McAllister

#pragma once

#include <atomic>
#include "absl/base/thread_annotations.h"

namespace sk {

// A test-and-test-and-set (TATAS) spinlock.  The nested load checks when the
// lock _might_ be available before attempting to acquire it.  Since the cache
// coherency protocol can service multiple readers, this prevents the atomic
// exchange operation from continuously changing cores.
//
// Spinlocks in general shouldn't be used in userspace since the kernel is
// unaware when a thread is holding a spin lock and can deschedule it at any
// time, blocking other waiting threads.
//
// They should be used very rare for very small critical sections that need to
// be serialized.

struct ABSL_LOCKABLE spinlock {
  spinlock() = default;

  spinlock(const spinlock &) = delete;
  spinlock(spinlock &&)      = delete;

  ~spinlock() ABSL_LOCKS_EXCLUDED(*this) {}

  void operator=(spinlock) = delete;

  void lock() const ABSL_EXCLUSIVE_LOCK_FUNCTION() {
    while (flag_.exchange(true, std::memory_order_acquire)) {
      while (flag_.load(std::memory_order_relaxed)) {
        __builtin_ia32_pause();
      }
    }
  }

  void unlock() const ABSL_UNLOCK_FUNCTION() {
    flag_.store(false, std::memory_order_release);
  }

 private:
  mutable std::atomic<bool> flag_ = false;
};

}  // namespace sk
