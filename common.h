
#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <sys/uio.h>

#include "hashtable.h"

#define DBG if(0) 

typedef struct hashtable hashtable_t;


// globals
extern hashtable_t *mrq_ht;

typedef struct config_t {
  int port;
  int max_memory; // mb
  int disk_size;  // gb
  int index_size;  // mb
  int block_size;  // mb
  int max_connections;
  uint32_t flags;

  // Stats // TODO move
  uint64_t tot_reads;
  uint64_t read_shifts;
  uint64_t tot_writes;
  uint64_t write_shifts;
  uint32_t max_shift;
  uint64_t misses;
  uint64_t value_bytes, compressed_bytes;
} config_t;


#define LIKELY(x)   (__builtin_expect(!!(x), 1))
#define UNLIKELY(x) (__builtin_expect(!!(x), 0))


#define IS_POWER_OF_TWO(x)  (x > 0 && ((x & (x - 1)) == 0))
