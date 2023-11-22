
#include "hashtable.h"
#include "mrcache.h"
#include "blocks.h"
#include "city.h"

//static int num_lru_move = 0;


void ht_init(hashtable_t *ht, uint32_t sz) {

  int index_len = sz >> 3;
  ht->mask = index_len - 1;
  ht->max_size = index_len * 0.70;
  ht->size = 0;
  ht->index_size = index_len;

  ht->tbl = calloc( index_len+1, sizeof(uint64_t));
  if (!ht->tbl) {
    fprintf(stderr, "Failed to init hashtable.\n");
    exit(EXIT_FAILURE);
  }
}

// TODO
//   Use if block_valid / block_mem 
//   

int ht_find(hashtable_t *ht, char *key, uint16_t keylen, uint64_t hv, void **outptr) {

  uint32_t hash = hv & ht->mask;
  DBG_SET printf("ht_find hash 0x%08x \n", hash );
  bool disk = false;

  uint64_t blockAddress = ht->tbl[hash];
  int shift = 0;

  // While we have a non zero addr and we haven't gone past the max shift
  while( blockAddress && (shift <= settings.max_shift) ) {

    item *it = blocks_translate(blockAddress);
    shift += 1;
 
    // If the item is in memory
    if ( it ) {
      settings.read_shifts += 1;
      char *itkey = it->data+it->size;
      if ( it->keysize == keylen && (memcmp(key, itkey, keylen) == 0)) {
        ht->last_block = GET_BLOCK(blockAddress);
        *outptr = it;
        return 1;
      }
    } else {
      
      // Disk or invalid
      if ( blocks_is_disk(blockAddress) ) {

        // Only read from disk if the key hash bits match
        if ( (hash&0xFFFull) == GET_KEY(blockAddress) ) {

          disk = true;
          if ( mrq_disk_reads < 3 ) {
            mrq_disk_blocks[mrq_disk_reads++] = blockAddress; // TODO test and number of disk blocks..
          }

        }

      } else {

        // We could remove the lru here and shift things left

      }
    }

    hash = (hash + 1) & ht->mask;
    blockAddress = ht->tbl[hash];
    
  }

  if ( disk ) return 2;
  
  return 0;

}

static int num_inserts = 0;
// TODO Can pass in the item to avoid an extra translate
void ht_insert(hashtable_t *ht, uint64_t blockAddr, char *key, uint16_t keylen, uint64_t hv) {
  DBG_SET printf("ht_insert blkadr 0x%lx key >%.*s<\n", blockAddr, keylen, key);

  num_inserts += 1;

  uint32_t hash = hv & ht->mask;
  uint32_t ohash = hv & ht->mask;

  uint64_t shift = 0;
  item *it = blocks_translate(ht->tbl[hash]);
  while (it) {
    char *itkey = it->data+it->size;
    if ( it->keysize == keylen && (memcmp(key, itkey, keylen) == 0)) {
      blocks_decrement(ht->tbl[hash]);
  
      SET_KEY( blockAddr, ohash );

      ht->tbl[hash] = blockAddr; // Use the new item location
      return;
    }
    hash = (hash + 1) & ht->mask;
    shift += 1;
    if ( shift > settings.max_shift ) settings.max_shift = shift;

    // if 0 or an LRU'd block we can install here TODO wouldn't blocks translate just return null breaking us out?
    // TODO This will become if disk increment until not?
    if ( !blocks_is_mem(ht->tbl[hash])  ) break;
    it = blocks_translate(ht->tbl[hash]);
  }

  DBG_SET printf("ht_insert hash 0x%08x \n", hash );
  SET_KEY( blockAddr, ohash );
  ht->tbl[hash] = blockAddr;
      
  ht->size += 1;


  // LRU if full
  if ( ht->size > ht->max_size ) {
    blocks_lru(); 
  } 

}

// Called after we lru a block with the number of items in that block
void ht_decrement(hashtable_t *ht, int n) {
  ht->size -= n;
}

void ht_stat(hashtable_t *ht) {

  int zero = 0;
  int mem  = 0;
  int lru  = 0;
  int disk = 0;
  for(int i = 0; i < ht->index_size; i++) {
    uint64_t a = ht->tbl[i];
    if ( a == 0 ) zero += 1;
    else if ( blocks_is_mem(a) ) mem += 1;
    else if ( blocks_is_disk(a) ) disk += 1;
    else lru += 1;
  }

  printf("Hashtable\n");
  printf("  zero %d\n", zero);
  printf("  mem  %d\n", mem);
  printf("  lru  %d\n", lru);
  printf("  disk %d\n\n", disk);
  printf("  tot  %d\n\n", mem+lru+disk);

}

// TODO
/*
void ht_delete(hashtable_t *ht, uint64_t hv) {
  uint32_t hash = hv & ht->mask;

  item *it = blocks_translate(ht->tbl[hash]);
  while (it) {
    char *itkey = it->data+it->size;
    if ( it->keysize == keylen && (memcmp(key, itkey, keylen) == 0)) {
      ht->tbl[hash] = 0;      
      ht->size -= 1;
      free(it);
    }
    hash = (hash + 1) & ht->mask;
    if ( ht->tbl[hash] == 0 ) break;
    it = blocks_translate(ht->tbl[hash]);
  }

}
*/


