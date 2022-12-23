#pragma once

#include <unistd.h>
#include <type_traits>

// A simple pointer + size type.  Does not manage the pointer memory, just
// supports tying the underlying memory size with the pointer itself.

template <typename T, typename Enable = void>
struct sizeptr {
    using type = T;

    constexpr sizeptr(T* ptr=nullptr, ssize_t size=0)
        : ptr_(ptr), size_(size) {}

    // Implicit conversion operator, works whenever T t = U(); would work.
    template <class U, std::enable_if_t<std::is_convertible_v<T*,U*>, int> = 0>
    operator sizeptr<U>() const {
        return {get(), size()};
    }

    // Explicit conversion operator, works the rest of the time.
    template <class U, std::enable_if_t<!std::is_convertible_v<T*,U*>, int> = 0>
    explicit operator sizeptr<U>() const {
        return {reinterpret_cast<U*>(get()), size()};
    }

    constexpr       T* get()  const { return ptr_;  }
    constexpr ssize_t  size() const { return size_; }

    explicit operator bool() {
        return ptr_ != nullptr && size_ > 0;
    }

       operator T*() const { return  get(); }
    T& operator  *() const { return *get(); }
    T* operator ->() const { return  get(); }

private:
    T* ptr_;
    ssize_t size_;
};

// Specialization for [const] void pointer without the dereference operators.
template <typename T>
struct sizeptr<T, std::enable_if_t<std::is_void_v<T>>> {
    using type = T;

    constexpr sizeptr(void* ptr=nullptr, ssize_t size=0)
        : ptr_(ptr), size_(size) {}

    // Explicit conversion operator.
    template <class U>
    explicit operator sizeptr<U>() const {
        return {static_cast<U*>(get()), size()};
    }

    explicit operator bool() {
        return ptr_ != nullptr && size_ > 0;
    }

    constexpr       T* get()  const { return ptr_;  }
    constexpr ssize_t  size() const { return size_; }

private:
    T* ptr_;
    ssize_t size_;
};
