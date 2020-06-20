
#pragma once

#include "common.h"
#include "hashtable.h"

typedef struct _item item;

typedef struct hashtable {
  uint64_t *tbl, *newtbl;
  uint32_t mask;
  uint32_t size;
  uint32_t indexSize;
  uint32_t maxSize;
  //uint64_t lastHash;
  uint64_t lastBlock;

} hashtable_t;

void   ht_init  (hashtable_t *ht, uint32_t sz);
int    ht_find  (hashtable_t *ht, char *key, uint16_t keylen, uint64_t hv, void **outptr);
void   ht_insert(hashtable_t *ht, uint64_t blockAddr, char *key, uint16_t keylen, uint64_t hv);
void   ht_delete(hashtable_t *ht, uint64_t hv);
void   ht_stat  (hashtable_t *ht);
void   ht_verify(hashtable_t *ht, uint32_t idx, uint32_t stop);
void   ht_decrement(hashtable_t *ht, int num);
void   ht_clear_lru(hashtable_t *ht, uint32_t idx, uint32_t stop);
