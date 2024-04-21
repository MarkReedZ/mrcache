#pragma once

#include <stdint.h>

namespace rand_utils {

#define ZFPRNG_SEED_INIT64 std::chrono::system_clock::now().time_since_epoch().count()
#define FPRNG_SEED_INIT64 time(null)


inline static uint64_t splitMix64(const uint64_t val) {
    uint64_t z = val    + 0x9e3779b97f4a7c15;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

// 32/64 bit rotation func
inline static uint64_t rotl(const uint64_t x, const int k) { return (x << k) | (x >> (sizeof(uint64_t)*8 - k)); } 

#define XOSHIRO256\
    const uint64_t t = s1 << 17;\
    s2 ^= s0;\
    s3 ^= s1;\
    s1 ^= s2;\
    s0 ^= s3;\
    s2 ^= t;\
    s3 = rotl(s3, 45);\
    return result;

#define XOSHIRO256_STATIC(FUNC)\
    static const uint64_t seed = uint64_t(FPRNG_SEED_INIT64);\
    static uint64_t s0 = splitMix64(seed), s1 = splitMix64(s0), s2 = splitMix64(s1), s3 = splitMix64(s2);\
    FUNC; XOSHIRO256

inline static uint64_t rand64()  { XOSHIRO256_STATIC(const uint64_t result = s0 + s3) }

void rand_bytes (char *buf, int num_bytes)
{
  uint64_t *p = (uint64_t*)buf;
  for (int i = 0; i < num_bytes; p++, i+=8 ) {
    *p = rand64();
  }
  buf[num_bytes] = 0;
}

}

#undef XOSHIRO256
#undef XOSHIRO256_STATIC

