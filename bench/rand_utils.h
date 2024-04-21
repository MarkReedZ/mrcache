#pragma once

#include <stdint.h>


#define ZFPRNG_SEED_INIT64 std::chrono::system_clock::now().time_since_epoch().count()


inline static uint64_t ru_splitMix64(uint64_t val) {
    uint64_t z = val    + 0x9e3779b97f4a7c15;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) * 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

// 32/64 bit rotation func
inline static uint64_t ru_rotl( uint64_t x,  int k) { return (x << k) | (x >> (sizeof(uint64_t)*8 - k)); } 

inline static uint64_t ru_rand64()  { 
  static uint64_t seed = 0;
  if ( seed == 0 ) seed = time(NULL);
  static uint64_t s0,s1,s2,s3;
  s0 = ru_splitMix64(seed); 
  s1 = ru_splitMix64(s0); 
  s2 = ru_splitMix64(s1); 
  s3 = ru_splitMix64(s2);
  uint64_t result = s0+s3;
  const uint64_t t = s1 << 17;
  s2 ^= s0;
  s3 ^= s1;
  s1 ^= s2;
  s0 ^= s3;
  s2 ^= t;
  s3 = ru_rotl(s3, 45);
  return result;
}

void ru_rand_bytes (char *buf, int num_bytes)
{
  uint64_t *p = (uint64_t*)buf;
  for (int i = 0; i < num_bytes; p++, i+=8 ) {
    *p = ru_rand64();
  }
  buf[num_bytes] = 0;
}


#undef XOSHIRO256
#undef XOSHIRO256_STATIC

