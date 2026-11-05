#pragma once

#include <stddef.h>
#include <stdint.h>

#define STORE_OK      0
#define STORE_TOO_BIG -1
#define STORE_BLOCKED -2

typedef struct store store_t;

struct store_stats {
    uint64_t gets, hits, misses, sets, dels, evicts, live;
};

int  store_init(store_t **out, size_t arena_bytes, size_t block_size, size_t index_bytes);
void store_free(store_t *s);

/* Largest value that can be stored next to a key of klen bytes. */
uint32_t store_max_value(const store_t *s, uint16_t klen);

/* 0 stored, STORE_TOO_BIG, or STORE_BLOCKED (oldest block is pinned). */
int store_set(store_t *s, const uint8_t *key, uint16_t klen,
              const uint8_t *val, uint32_t vlen);

/* Hit: pointer at the reply prefix (status byte), *rlen is the reply size,
 * *pin_slot is the arena slot that must stay pinned until the reply is sent.
 * Miss: NULL. */
uint8_t *store_get(store_t *s, const uint8_t *key, uint16_t klen,
                   uint32_t *rlen, uint32_t *pin_slot);

/* 0 deleted, 1 not found. */
int store_del(store_t *s, const uint8_t *key, uint16_t klen);

void store_pin(store_t *s, uint32_t slot);
void store_unpin(store_t *s, uint32_t slot);
void store_stat(const store_t *s, struct store_stats *out);
size_t store_block_size(const store_t *s);
