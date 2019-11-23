
#include "hashtable.h"
#include "mrcache.h"
#include "blocks.h"
#include "city.h"

//static int num_lru_move = 0;

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


int ht_resize_timer( void *htt ) {
  hashtable_t *ht = htt;
  double start_time = clock();
  int max = ht->new_idx + 50000;
  if ( max > ht->maxSize ) max = ht->maxSize;
  for ( int i = ht->new_idx; i < max; i++ ) {

    // TODO if item is on disk still need to copy
    item *it = blocks_translate(ht->tbl[i]);
    if (it) {
      char *itkey = it->data+it->size;
      unsigned long hv = CityHash64(itkey, it->keysize);
      ht_insert_new( ht, ht->tbl[i], itkey, it->keysize, hv );
    }
  }
  ht->new_idx = max;
  double taken = ((double)(clock()-start_time))/CLOCKS_PER_SEC;
  printf( " resize timer tick took %f max %d sz %d \n ", taken, max, ht->maxSize);

  if ( max >= ht->maxSize ) {
    // We're done
    free(ht->tbl); ht->tbl = ht->newtbl; ht->newtbl = NULL;
    ht->maxSize <<= 1;
    ht->growthSize = ht->maxSize * 0.80;
    ht->size = ht->new_size;
    ht->mask = ht->new_mask;
    ht->resizing = false;
    printf(" done resizing %d %d\n", ht->size, ht->growthSize);
    return 0; // Stop the timer
  }
  return 1; 
}

void ht_resize(hashtable_t *ht) {

  if ( ht->resizing ) return;
  ht->resizing = true;
  //uint32_t oldsz = ht->maxSize;
  //ht->maxSize <<= 1;

  uint64_t *p = calloc( ht->maxSize << 1, sizeof(uint64_t));
  if ( !p ) {
    ht->maxSize >>= 1;
    printf("oom\n");  // TODO
    exit(-1);
  }
  ht->newtbl = p;
  ht->new_mask = (ht->maxSize<<1) - 1;
  ht->new_idx  = 0;
  ht->new_size = 0;

  //ht->mask = ht->maxSize - 1;
  //ht->size = 0;
  //ht->growthSize = ht->maxSize * 0.80;

  // Resize, and if we're not done add a timer to continue
  if (ht_resize_timer( ht )) mr_add_timer( settings.loop, 0.01, ht_resize_timer, (void*)ht );

/*
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
*/
}


item *ht_find(hashtable_t *ht, char *key, uint16_t keylen, uint64_t hv) {

  uint32_t hash = hv & ht->mask;
  uint32_t perturb = hash;
  DBG_SET printf("ht_find hash 0x%08x \n", hash );

  item *it = blocks_translate(ht->tbl[hash]);
  while (it) {
    DBG_SET printf("ht_find hash 0x%08x \n", hash );
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
      //ht->lastHash = hash; // TODO still need this?
      ht->lastBlock = GET_BLOCK(ht->tbl[hash]);
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
  DBG_SET printf("ht_insert blkadr 0x%lx key >%.*s<\n", blockAddr, keylen, key);

  if ( unlikely(ht->newtbl) ) ht_insert_new( ht, blockAddr, key, keylen, hv );

  uint32_t hash = hv & ht->mask;
  uint32_t perturb = hash;

  item *it = blocks_translate(ht->tbl[hash]);
  while (it) {
    char *itkey = it->data+it->size;
    if ( it->keysize == keylen && (memcmp(key, itkey, keylen) == 0)) {
      ht->tbl[hash] = blockAddr; // Use the new item location
      return;
    }
    perturb >>= 5;
    hash = (hash*5 + perturb + 1) & ht->mask;

    // if 0 or an LRU'd block we can install here TODO wouldn't blocks translate just return null breaking us out?
    if ( !blocks_isvalid(ht->tbl[hash])  ) break;
    it = blocks_translate(ht->tbl[hash]);
  }
  DBG_SET printf("ht_insert hash 0x%08x \n", hash );
  ht->tbl[hash] = blockAddr;
      
  ht->size += 1;
  if ( ht->size > ht->growthSize ) ht_resize(ht);

}
void ht_insert_new(hashtable_t *ht, uint64_t blockAddr, char *key, uint16_t keylen, uint64_t hv) {

  uint32_t hash = hv & ht->new_mask;
  uint32_t perturb = hash;

  item *it = blocks_translate(ht->newtbl[hash]);

  while (it) {
    char *itkey = it->data+it->size;
    if ( it->keysize == keylen && (memcmp(key, itkey, keylen) == 0)) {
      ht->newtbl[hash] = blockAddr; // Use the new item location
      return;
    }
    perturb >>= 5;
    hash = (hash*5 + perturb + 1) & ht->new_mask;

    it = blocks_translate(ht->newtbl[hash]);
  }

  ht->newtbl[hash] = blockAddr;
  ht->new_size += 1;

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


