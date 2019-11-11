
#include "hashtable.h"
#include "mrcache.h"
#include "blocks.h"
#include "city.h"

static int num_lru_move = 0;

void ht_init(hashtable_t *ht) {

  ht->size = 0;
  ht->maxSize = (uint64_t)1 << 13;
  ht->mask = ht->maxSize - 1;
  ht->growthSize = ht->maxSize * 0.80;

  ht->tbl = calloc( ht->maxSize, sizeof(uint64_t));
  if (!ht->tbl) {
    fprintf(stderr, "Failed to init hashtable.\n");
    exit(EXIT_FAILURE);
  }
}


void ht_resize(hashtable_t *ht) {

  uint32_t oldsz = ht->maxSize;
  ht->maxSize <<= 1;

  uint64_t *p = calloc( ht->maxSize, sizeof(uint64_t));
  if ( !p ) {
    ht->maxSize >>= 1;
    printf("oom\n");
    exit(-1);
  }

  uint64_t *oldtbl = ht->tbl;
  ht->tbl = p;

  ht->mask = ht->maxSize - 1;
  ht->size = 0;
  ht->growthSize = ht->maxSize * 0.80;

  // Copy items..
  double start_time = clock();

  for ( int i = 0; i < oldsz; i++ ) {
    item *it = blocks_translate(oldtbl[i]);
    if (it) {
      char *itkey = it->data+it->size;
      unsigned long hv = CityHash64(itkey, it->keysize);
      ht_insert( ht, oldtbl[i], itkey, it->keysize, hv );
    }
  }
  free(oldtbl);

  double taken = ((double)(clock()-start_time))/CLOCKS_PER_SEC;
  printf( " resize time taken %f \n ", taken);

}


item *ht_find(hashtable_t *ht, char *key, uint16_t keylen, uint64_t hv) {

  uint32_t hash = hv & ht->mask;
  uint32_t perturb = hash;

  item *it = blocks_translate(ht->tbl[hash]);
  while (it) {
    char *itkey = it->data+it->size;
    if ( it->keysize == keylen && (memcmp(key, itkey, keylen) == 0)) {
      /*
      // If near lru block update ht->tbl[hash] and return it
      if ( blocks_isNearLru(ht->tbl[hash])) {
        blocks_decrement(ht->tbl[hash]);
        num_lru_move += 1;
        DBG printf(" near lru move num %d\n",num_lru_move );
        uint64_t blockAddr = blocks_alloc( sizeof(item) + it->size );
        ht->tbl[hash] = blockAddr; 
        item *itnew = blocks_translate( blockAddr );
        itnew->key = it->key;
        itnew->size = it->size;
        memcpy( itnew->data, it->data, it->size );
        return itnew;
      }
      */
      ht->lastHash = hash; // TODO still need this?
      return it;
    }
    perturb >>= 5;
    hash = (hash*5 + perturb + 1) & ht->mask;
    if ( ht->tbl[hash] == 0 ) break;
    it = blocks_translate(ht->tbl[hash]);
  }

  return NULL;
}

// TODO Can pass in the item to avoid an extra translate
void ht_insert(hashtable_t *ht, uint64_t blockAddr, char *key, uint16_t keylen, uint64_t hv) {

  uint32_t hash = hv & ht->mask;
  uint32_t perturb = hash;

  // TODO if dummy slot we can install
  item *it = blocks_translate(ht->tbl[hash]);
  while (it) {
    char *itkey = it->data+it->size;
    if ( it->keysize == keylen && (memcmp(key, itkey, keylen) == 0)) break;
    perturb >>= 5;
    hash = (hash*5 + perturb + 1) & ht->mask;
    if ( ht->tbl[hash] == 0 ) break;
    it = blocks_translate(ht->tbl[hash]);
  }
  // TODO remove old item if we are overwriting
  ht->tbl[hash] = blockAddr;
  ht->size += 1;
  if ( ht->size > ht->growthSize ) ht_resize(ht);
}

// Called after we lru a block with the number of items in that block
void ht_decrement(hashtable_t *ht, int n) {
  ht->size -= n;
}


// TODO
/*
void ht_delete(hashtable_t *ht, uint64_t hv) {
  uint32_t hash = hv & ht->mask;
  uint32_t perturb = hash;

  item *it = blocks_translate(ht->tbl[hash]);
  while (it) {
    char *itkey = it->data+it->size;
    if ( it->keysize == keylen && (memcmp(key, itkey, keylen) == 0)) {
      ht->tbl[hash] = 0;      
      ht->size -= 1;
      free(it);
    }
    perturb >>= 5;
    hash = (hash*5 + perturb + 1) & ht->mask;
    if ( ht->tbl[hash] == 0 ) break;
    it = blocks_translate(ht->tbl[hash]);
  }

}
*/


