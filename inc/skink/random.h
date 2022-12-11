#pragma once

// General utilities for generating random data.  We use an xoroshiro256** PRNG
// which provides good high speed general purpose random data.  It is not
// cryptographically secure and should not be used for any crytographic purpose.
//
// Attribution
// ‾‾‾‾‾‾‾‾‾‾‾
// Written in 2019 by David Blackman and Sebastiano Vigna (vigna@acm.org)
//
// To the extent possible under law, the author has dedicated all copyright
// and related and neighboring rights to this software to the public domain
// worldwide. This software is distributed without any warranty.
//
// See <http://creativecommons.org/publicdomain/zero/1.0/>.

#include <stdint.h>
#include <string.h>

#include <absl/numeric/bits.h>
#include <absl/base/attributes.h>

// We don't actually use this variable but we'll use its pointer as a random
// per-process seed value to use in the prng by default.  This let's us have a
// default seed but avoids people accidentally relying on exactly what it is.
static int __rand;


// splitmix64 PRNG.  This is the default PRNG used in Java and is "good enough"
// for many purposes.  We use it to bootstrap the state of larger PRNGs from a
// 64 bit seed.
struct splitmix64 {
    splitmix64(uint64_t seed=0) {
        if (seed) {
            s_ = seed;
            return;
        }

        // Seed from __rand address.
        uintptr_t rnd = reinterpret_cast<uintptr_t>(&__rand);
        memcpy(&s_, &rnd, sizeof(intptr_t));
        if (sizeof(intptr_t) == 4) {
            memcpy(reinterpret_cast<char*>(&s_) + 4, &rnd, 4);
        }
    }

    // Generates a new random 64-bit value.
    uint64_t operator()() {
        s_ += 0x9e3779b97f4a7c15ul;

        uint64_t val = s_;
        val = (val ^ (val >> 30)) * 0xbf58476d1ce4e5b9ul;
        val = (val ^ (val >> 27)) * 0x94d049bb133111ebul;
        val = (val ^ (val >> 31));
        return val;
    }

private:
    uint64_t s_;
};


// Mixin for PRNGs to inherit from to get pre-defined generator functions.
template <typename T>
struct prng_base {
    // Generates a uniform random float value in the range [0,1).
    float uniform_flt() {
        static_assert(
            std::numeric_limits<float>::is_iec559, "Need IEEE754 floats");

        T& prng = *static_cast<T*>(this);
        const union {
            uint32_t i;
            float    f;
        } u = { 0x7Fu << 23 | static_cast<uint32_t>(prng()) >> 9 };
        return u.f - 1;
    }

    // Generates a uniform random double value in the range [0,1)
    double uniform_dbl() {
        static_assert(
            std::numeric_limits<double>::is_iec559, "double is not IEEE754!");

        T& prng = *static_cast<T*>(this);
        const union {
            uint64_t i;
            double   d;
        } u = { 0x3FFul << 52 | (uint64_t)prng() >> 12 };
        return u.d - 1.0;
    }

    // Generate a uniform float value in the range [lo, hi)
    float uniform_flt(float lo, float hi) { return uniform<float>(lo, hi); }

    // Generate a uniform double value in the range [lo, hi)
    double uniform_dbl(double lo, double hi) { return uniform<double>(lo, hi); }

    // Generates a uniform random integer.
    template <typename I>
    std::enable_if_t<std::is_integral_v<I>,I> uniform_int() {
        static_assert(sizeof(I) <= sizeof(uint64_t));

        T& prng = *static_cast<T*>(this);
        return static_cast<I>(prng());
    }

    uint64_t uniform_u64() { return uniform<uint64_t>(); }
    uint32_t uniform_u32() { return uniform<uint32_t>(); }
    uint16_t uniform_u16() { return uniform<uint16_t>(); }
     uint8_t uniform_u8()  { return uniform<uint8_t>();  }

    int64_t  uniform_i64() { return uniform<int64_t>(); }
    int32_t  uniform_i32() { return uniform<int32_t>(); }
    int16_t  uniform_i16() { return uniform<int16_t>(); }
     int8_t  uniform_i8()  { return uniform<int8_t>();  }

    // Generates a uniform integer value in the range [lo, hi)
    template <typename I>
    std::enable_if_t<std::is_integral_v<I>, I> uniform(I lo, I hi) {
        if (hi < lo) {
            std::swap(lo, hi);
        }
        return uniform_int<I>() % (hi - lo + 1) + lo;
    }

    uint64_t uniform_u64(uint64_t lo, uint64_t hi) { return uniform(lo, hi); }
    uint32_t uniform_u32(uint32_t lo, uint32_t hi) { return uniform(lo, hi); }
    uint16_t uniform_u16(uint16_t lo, uint16_t hi) { return uniform(lo, hi); }
     uint8_t uniform_u8 (uint8_t  lo, uint8_t  hi) { return uniform(lo, hi); }

    int64_t  uniform_i64(int64_t lo, int64_t hi) { return uniform(lo, hi); }
    int32_t  uniform_i32(int32_t lo, int32_t hi) { return uniform(lo, hi); }
    int16_t  uniform_i16(int16_t lo, int16_t hi) { return uniform(lo, hi); }
     int8_t  uniform_i8 (int8_t  lo, int8_t  hi) { return uniform(lo, hi); }

private:
    // Generates a uniform floating point value in the range [lo, hi)
    template <typename F>
    std::enable_if_t<std::is_floating_point_v<F>, F> uniform(F lo, F hi) {
        if (hi < lo) {
            std::swap(lo, hi);
        }
        return uniform<F>()*(hi - lo) + lo;
    }
};

// xoroshiro256** PRNG.
struct xoros256ss : prng_base<xoros256ss> {
    xoros256ss(uint64_t seed=0) {
        splitmix64 prng(seed);
        for (int ii=0; ii < 4; ++ii) {
            s_[ii] = prng();
        }
    }

    // Generates a new random 64-bit value.
    uint64_t operator()() {
        uint64_t val  = absl::rotl(s_[1] * 5, 7) * 9;

        uint64_t tt = s_[1] << 17;
        s_[2] ^= s_[0];
        s_[3] ^= s_[1];
        s_[1] ^= s_[2];
        s_[0] ^= s_[3];
        s_[2] ^= tt;
        s_[3]  = absl::rotl(s_[3], 45);
        return val;
    }

    // Generates a copy of the PRNG that has been moved ahead the equivalent of
    // 2^128 calls to operator().  Used to generate multiple non-overlapping
    // sequences from the same seed.
    xoros256ss jump() const {
        static constexpr uint64_t kJump[] = {
            0x180ec6d33cfd0aba,
            0xd5a61266f0c9392c,
            0xa9582618e03fc9aa,
            0x39abdc4529b1661c
        };

        // Copy our current state.
        xoros256ss prng = *this;

        // Generate new state.
        uint64_t s[4] = {0};
        for(int ii = 0; ii < 4; ++ii) {
            for(int bit = 0; bit < 64; ++bit) {
                if (kJump[ii] & (uint64_t{1} << bit)) {
                    s[0] ^= prng.s_[0];
                    s[1] ^= prng.s_[1];
                    s[2] ^= prng.s_[2];
                    s[3] ^= prng.s_[3];
                }
                prng();
            }
        }

        prng.s_[0] = s[0];
        prng.s_[1] = s[1];
        prng.s_[2] = s[2];
        prng.s_[3] = s[3];
        return prng;
    }

private:
    // Build directly from state values.
    xoros256ss(uint64_t s0, uint64_t s1, uint64_t s2, uint64_t s3)
        : s_{s0,s1,s2,s3} {}

    uint64_t s_[4];
};

// Expose xoroshiro256** as the default PRNG.
using prng = xoros256ss;
