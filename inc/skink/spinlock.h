#pragma once

#include <atomic>
#include "absl/base/thread_annotations.h"

struct ABSL_LOCKABLE spinlock {
    spinlock() = default;

    spinlock(const spinlock&) = delete;
    spinlock(spinlock&&) = delete;

    ~spinlock() ABSL_LOCKS_EXCLUDED(*this) {}

    void operator =(spinlock) = delete;

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
