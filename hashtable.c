
#include "hashtable.h"
#include "mrcache.h"
#include "blocks.h"
#include "city.h"

//static int num_lru_move = 0;


void ht_init(hashtable_t *ht, uint32_t sz) {

  int index_len = sz >> 3;
  printf(" index_len %d\n", index_len);
  ht->mask = index_len - 1;
  printf(" htmask %x\n", ht->mask);
  ht->max_size = index_len * 0.70;
  printf(" max items %d\n", ht->max_size);
  ht->size = 0;
  ht->index_size = index_len;

  ht->tbl = calloc( index_len+1, sizeof(uint64_t));
  if (!ht->tbl) {
    fprintf(stderr, "Failed to init hashtable.\n");
    exit(EXIT_FAILURE);
  }
}

int ht_find(hashtable_t *ht, char *key, uint16_t keylen, uint64_t hv, void **outptr) {

  uint32_t hash = hv & ht->mask;
  DBG_SET printf("ht_find hash 0x%08x \n", hash );
  bool disk = false;

  uint64_t blockAddress = ht->tbl[hash];

  while( blockAddress ) {

    item *it = blocks_translate(blockAddress);
    if ( it ) {
      settings.read_shifts += 1;
      char *itkey = it->data+it->size;
      if ( it->keysize == keylen && (memcmp(key, itkey, keylen) == 0)) {
        ht->last_block = GET_BLOCK(blockAddress);
        *outptr = it;
        return 1;
      }
    } else {
      
      // Disk
      if ( blocks_is_disk(blockAddress) ) {

        // Only read from disk if the index key bits match
        if ( (key[keylen-1]&0xF) == GET_KEY(blockAddress) ) {

          disk = true;
          if ( mrq_disk_reads < 3 ) {
            mrq_disk_blocks[mrq_disk_reads++] = blockAddress;
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
  bool print = false;

  num_inserts += 1;
  if ((num_inserts%100000) == 0) printf(" max shift %d \n",settings.max_shift);

  uint32_t hash = hv & ht->mask;
  uint32_t ohash = hv & ht->mask;
  if ( hash == 113977 ) print = true;
  if ( hash == 11977 ) print = true;
  if ( hash == 1221977 ) print = true;
  if ( hash == 12331977 ) print = true;
  //if ( hash == 18349 ) print = true;


  uint64_t shift = 0;
  item *it = blocks_translate(ht->tbl[hash]);
  while (it) {
    char *itkey = it->data+it->size;
    if ( print ) { printf("ohash %d hash %d %.*s it key %.*s max shift %d\n",ohash, hash, keylen, key, it->keysize, itkey, settings.max_shift ); }
    if ( it->keysize == keylen && (memcmp(key, itkey, keylen) == 0)) {
      blocks_decrement(ht->tbl[hash]);
  
      SET_SHIFT( blockAddr, shift );
      SET_KEY( blockAddr, key[keylen-1] );

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
  SET_SHIFT( blockAddr, shift );
  SET_KEY( blockAddr, key[keylen-1] );
  ht->tbl[hash] = blockAddr;
  if ( print ) { printf("ohash %d hash %d %.*s shift %ld %ld\n",ohash, hash, keylen, key, shift, GET_SHIFT(blockAddr)); }
      
  ht->size += 1;
  // Random LRU an item if full
  if ( ht->size > ht->max_size ) {
    ht->size -= 1;

    uint32_t i = rand() % ht->index_size;
    while ( ht->tbl[i] == 0 ) i = rand() % ht->index_size;
    ht->tbl[i] = 0;

//i-1 39018 != hash 16463 + shift 6


    // Shift items over if they belong 
    i+=1;
    uint64_t tmp = ht->tbl[i];
    shift = GET_SHIFT(tmp);

      item *it = blocks_translate(tmp);
      if ( it ) {
        char *itkey = it->data+it->size;
        unsigned long zhv = CityHash64(itkey, it->keysize);
        hash = zhv & ht->mask;
  
        if ( i != ((hash+shift)&ht->mask) ) {
          printf("i %d != hash %d + shift %ld\n",i, hash, shift);
          printf("i %08x != %08lx\n",i, (hash+shift)&ht->mask);
          exit(1);
        }
      }

    while ( shift ) {
      shift -= 1;
      SET_SHIFT(tmp, shift);
      ht->tbl[i-1] = tmp;
      ht->tbl[i] = 0;

/*
      item *it = blocks_translate(tmp);
      char *itkey = it->data+it->size;
      unsigned long zhv = CityHash64(itkey, it->keysize);
      hash = zhv & ht->mask;

      if ( i-1 != hash+shift ) {
        printf("i-1 %d != hash %d + shift %d\n",i-1, hash, shift);
        printf("max shift %d!\n",settings.max_shift);
        exit(1);
      }
*/

      if ( shift == 0 ) {

        item *it = blocks_translate(tmp);
        char *itkey = it->data+it->size;
        unsigned long hv = CityHash64(itkey, it->keysize);
        hash = hv & ht->mask;

        if ( hash != i-1 ) {
          printf("i %d != hash %d\n",i, hash);
          printf("max shift %d!\n",settings.max_shift);
          exit(1);
        }

    	}

      i+=1;
      tmp = ht->tbl[i];
      shift = GET_SHIFT(tmp);
      //if ( GET_SHIFT(ht->tbl[i+1]) ) {
        //printf(" YAY shifted! %ld %08lx\n", GET_SHIFT(ht->tbl[i+1]), ht->tbl[i+1]);
        //exit(1);
      //}
    }
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

void ht_verify(hashtable_t *ht, uint32_t idx, uint32_t stop) {

  for (int i = idx; i < stop; i++ ) {
    uint64_t b = ht->tbl[idx];
    item *it = blocks_translate(b);
    if (it == NULL) continue;
    char *itkey = it->data+it->size;
    unsigned long hv = CityHash64(itkey, it->keysize);
    uint64_t hash = hv & ht->mask;

    if ( hash != idx ) {
      printf("i %d != hash %ld\n",i, hash);
      exit(1);
    }
    
  }
}

void ht_clear_lru(hashtable_t *ht, uint32_t idx, uint32_t stop) {
  while ( idx < stop ) {
    //printf(" idx %d %d %d\n",idx,stop, ht->index_size);


    uint64_t b = ht->tbl[idx];
    if ( b != 0 && blocks_is_lru(b) ) {
      ht->tbl[idx] = 0;

      int shift = 1;
      while(b) {
        while( b != 0 && blocks_is_lru(b) ) {
          ht->tbl[idx] = 0;
          idx = (idx + 1) & ht->mask;
          b = ht->tbl[idx];
          shift += 1;
        }
        if ( b == 0 ) break;
        int bshift = GET_SHIFT(b);
        if ( bshift == 0 ) {
          idx = (idx + 1) & ht->mask;
          break;
        }
        //if ( bshift ) { printf("bshift! idx %d\n", idx); exit(1); }
        if ( bshift < shift ) shift = bshift;
        SET_SHIFT(b, (bshift-shift));
        ht->tbl[(idx-shift) & ht->mask] = b;
        ht->tbl[idx] = 0;
        idx = (idx + 1) & ht->mask;
        b = ht->tbl[idx];
      }
     
    }

    idx = (idx + 1) & ht->mask;
    b = ht->tbl[idx];

  }
} 
void ht_clear_lru_full(hashtable_t *ht, uint32_t idx, uint32_t stop) {
  idx = 0;
  stop = ht->index_size-1;

  while ( idx < stop ) {
    //printf(" idx %d %d %d\n",idx,stop, ht->index_size);
    uint64_t b = ht->tbl[idx];
    item *it = blocks_translate(b);
    if ( it ) {
      char *itkey = it->data+it->size;
      unsigned long hv = CityHash64(itkey, it->keysize);
      uint64_t hash = hv & ht->mask;
      printf(" %.*s %08lx\n", it->keysize, itkey, hash);
    }
    idx += 1;
  }

/*
      
      int shift = 1;
      while(b) {
        while( blocks_is_lru(b) ) {
          ht->tbl[idx] = 0;
          idx = (idx + 1) & ht->mask;
          b = ht->tbl[idx];
          shift += 1;
        }
        int bshift = GET_SHIFT(b);
        if ( bshift == 0 ) {
          idx = (idx + 1) & ht->mask;
          break;
        }
        //if ( bshift ) { printf("bshift! idx %d\n", idx); exit(1); }
        if ( bshift < shift ) shift = bshift;
        SET_SHIFT(b, (bshift-shift));

        if ( blocks_is_mem(ht->tbl[(idx-shift) & ht->mask]) ) {
          printf("NOOO shift %d\n", shift);
          int a = idx-shift-2;
          for (int z = 0; z < 10; z++ ) {
            if ( ht->tbl[a] == 0 ) printf("0 ");
            else if ( blocks_is_mem(ht->tbl[a]) ) printf("M ");
            else if ( blocks_is_lru(ht->tbl[a]) ) printf("L ");
            else printf("Z "); 
            a+=1;
          }
          printf("\n");
          exit(1);
        }

        ht->tbl[(idx-shift) & ht->mask] = b;
        ht->tbl[idx] = 0;
        idx = (idx + 1) & ht->mask;
        b = ht->tbl[idx];
      }
      
    }

    idx = (idx + 1) & ht->mask;
    b = ht->tbl[idx];

  }
*/
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


