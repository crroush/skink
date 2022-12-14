// Copyright 2022 Google LLC
// Author: Sean McAllister

#pragma once

#include <cstring>

namespace sk {

// Bindings for folly memcpy implementation.
extern "C" {
void *__folly_memcpy(  //
    void *__restrict dst,
    const void *__restrict src,
    size_t size);

#ifndef __AVX2__
inline void *__folly_memcpy(  //
    void *__restrict dst,
    const void *__restrict src,
    size_t size) {
  return memcpy(dst, src, size);
}
#endif
}  // extern "C"

}  // namespace sk
