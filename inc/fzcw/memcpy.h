#pragma once

#include <cstdlib>

// Bindings for folly memcpy implementation.
extern "C" {
void* __folly_memcpy(
    void* __restrict dst, const void* __restrict src, size_t size);

#ifndef __AVX2__
inline void* __folly_memcpy(
    void* __restrict dst, const void* __restrict src, size_t size) {
    return memcpy(dst, src, size);
}
#endif

} // extern "C"
