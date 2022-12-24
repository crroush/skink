// Copyright 2022 Google LLC
// Author: Sean McAllister

#pragma once

#include <cstdint>
#include <memory>
#include <type_traits>

// A flexible pointer class that can be instantiated from a unique, shared or
// raw pointer.  This lets the caller determine how they want lifetime semantics
// to be handled.
//
// This is particularly useful when supporting things like python extensions
// where we may need to "stash" a python object to keep it alive but are forced
// to return back to the interpreter.
//
// In the case of a raw pointer, no deletion is done when the flexptr is
// destroyed to mimic the semantics of passing a raw pointer, thus all the
// lifetime concerns are the same and one must be careful to ensure that data
// lives as long as needed.
//
// Since std::unique_ptr can't hold a void pointer, neither can flexptr.

enum class FlexType : uint8_t { kUnique, kShared, kRawPtr };

template <typename T>
struct flexptr {
  static_assert(!std::is_void_v<T>, "Flexptr cannot hold void pointers");

  flexptr() : flexptr(nullptr) {}

  flexptr(std::unique_ptr<T> &&ptr)
      : u(std::move(ptr)), type_(FlexType::kUnique) {}

  flexptr(const std::shared_ptr<T> &ptr) : s(ptr), type_(FlexType::kShared) {}

  flexptr(T *ptr) : r(ptr), type_(FlexType::kRawPtr) {}

  // flexptr isn't copyable because its constituents might not be.
  flexptr(const flexptr &b)               = delete;
  flexptr &operator=(const flexptr<T> &b) = delete;

  flexptr(flexptr &&b) : type_(b.type_) {
    using std::move;
    switch (type_) {
      case FlexType::kUnique:
        new (&u) std::unique_ptr<T>(move(b.u));
        break;
      case FlexType::kShared:
        new (&s) std::shared_ptr<T>(move(b.s));
        break;
      case FlexType::kRawPtr:
        r   = b.r;
        b.r = nullptr;
        break;
    }
  }

  // If we have "const T" as our type, this will allow assignment from
  // flexptr<T> as well, since that's a valid implicit conversion in C++
  flexptr &operator=(flexptr<std::remove_const_t<T>> &&b) {
    using std::move;
    destruct();
    type_ = b.type_;
    switch (type_) {
      case FlexType::kUnique:
        new (&u) std::unique_ptr<T>(move(b.u));
        break;
      case FlexType::kShared:
        new (&s) std::shared_ptr<T>(move(b.s));
        break;
      case FlexType::kRawPtr:
        r   = b.r;
        b.r = nullptr;
        break;
    }
    return *this;
  }

  ~flexptr() { destruct(); }

  // Resets the flexptr and releases any managed memory.
  void clear() {
    destruct();
    r     = nullptr;
    type_ = FlexType::kRawPtr;
  }

  explicit operator bool() const {
    switch (type_) {
      case FlexType::kUnique:
        return static_cast<bool>(u);
      case FlexType::kShared:
        return static_cast<bool>(s);
      case FlexType::kRawPtr:
        return r != nullptr;
    }
  }

  constexpr T *get() const {
    switch (type_) {
      case FlexType::kUnique:
        return u.get();
      case FlexType::kShared:
        return s.get();
      case FlexType::kRawPtr:
        return r;
    }
  }

  T &operator[](size_t i) const { return get()[i]; }

  operator T *() const { return get(); }
  T &operator*() const { return *get(); }
  T *operator->() const { return get(); }

 private:
  template <class U>
  friend struct flexptr;

  void destruct() {
    switch (type_) {
      case FlexType::kUnique:
        u.~unique_ptr();
        break;
      case FlexType::kShared:
        s.~shared_ptr();
        break;
      case FlexType::kRawPtr:
        break;
    }
  }

  union {
    std::unique_ptr<T> u;
    std::shared_ptr<T> s;
    T *r;
  };

  FlexType type_;
};
