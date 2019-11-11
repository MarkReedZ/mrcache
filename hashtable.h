
#pragma once

#include "common.h"
#include "hashtable.h"

typedef struct _item item;

typedef struct {
  uint64_t *tbl;
  uint32_t mask;
  uint32_t size;
  uint32_t maxSize;
  uint32_t growthSize;
  uint64_t lastHash;
} hashtable_t;

void   ht_init  (hashtable_t *ht);
item  *ht_find  (hashtable_t *ht, char *key, uint16_t keylen, uint64_t hv);
void   ht_insert(hashtable_t *ht, uint64_t blockAddr, char *key, uint16_t keylen, uint64_t hv);
void   ht_delete(hashtable_t *ht, uint64_t hv);
void   ht_resize(hashtable_t *ht );
void   ht_decrement(hashtable_t *ht, int num);

