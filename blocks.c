


#include "mrcache.h"
#include "blocks.h"
#include "hashtable.h"

static int num_blocks;
static int min_block;
static void *base;
static void *cur;
static uint32_t cur_block;
static uint32_t cur_block_size;
static bool full;
static int *items_in_block;

//typedef struct {
  //void *cur;
  //uint32_t curBlock;
  //uint32_t curBlockSize;
//} block_t;

/*
void block_init(block_t *blk) {
  blk->curBlock = blocks_getNextBlock();
  blk->curBlockSize = 0;
  blk->cur = base + ((blk->curBlock%num_blocks)<<20);
}
uint64_t block_alloc( block_t *blk, int sz ) {

  // If we're going into the next block
  if ( (blk->curBlockSize+sz) > 0xFFFFF ) {

    blk->curBlock = blocks_getNextBlock();
    blk->cur = base + ((blk->curBlock%num_blocks)<<20);
    blk->curBlockSize = 0;

  }

  blk->cur += sz;
  blk->curBlock_size += sz;
  items_in_block[ blk->curBlock%num_blocks ] += 1;

  return (blk->curBlock << 20) | (blk->curBlockSize-sz);
}

uint32_t blocks_getNextBlock() {
    blocks_getNextBlock();
    //if ( full ) blocks_lru();
    //else if ( cur_block == num_blocks-1 ) full = true;
}
*/

void blocks_init() {
  printf(" settings %d\n", settings.max_memory);
  num_blocks = settings.max_memory;
  base = malloc( num_blocks * 1024 * 1024 );
  min_block = 1;
  cur = base;
  cur_block = 1;
  cur_block_size = 0;
  items_in_block = calloc( num_blocks, sizeof(int) );

  if ( !base ) {
    fprintf(stderr, "Failed to allocate memory of %d mb\n", num_blocks);
    exit(EXIT_FAILURE);
  }

}

uint64_t blocks_alloc( int sz ) {

  // If we're going into the next block
  if ( (cur_block_size+sz) > 0xFFFFF ) {

    //printf("DELME crossing to a new block old block %d num items %d\n", cur_block%num_blocks, items_in_block[ cur_block%num_blocks ]);
    if ( full ) blocks_lru();
    else if ( cur_block == num_blocks-1 ) full = true;

    cur_block += 1;
    cur = base + ((cur_block%num_blocks)<<20);
    cur_block_size = 0;

  }

  cur += sz;
  cur_block_size += sz;
  items_in_block[ cur_block%num_blocks ] += 1;

  return (cur_block << 20) | (cur_block_size-sz);
}

void blocks_lru() {
  int n = items_in_block[ min_block%num_blocks ];
  //printf("DELME lru block %d num items %d\n", min_block,n );

  // Which ht to decrement?
  ht_decrement(mrq_ht, n); 
  items_in_block[ min_block%num_blocks ] = 0;
  min_block += 1; 
}

void *blocks_translate( uint64_t blockAddr ) {
  uint64_t blk = blockAddr >> 20;
  if ( blk < min_block ) return NULL;
  uint32_t off = blockAddr & 0xFFFFF;
  return (base + ((blk%num_blocks)<<20)) + off;
}

bool blocks_isvalid( int block ) {
  if ( block < min_block ) return false;
  return true;
}

bool blocks_isNearLru( uint64_t blockAddr ) {
  if ( !full ) return false;
  uint64_t blk = blockAddr >> 20;
  //printf(" DELME blk %d min %d\n", blk, min_block);
  if ( blk < (min_block + 4) ) return true;
  return false;
}

void blocks_decrement( uint64_t blockAddr ) {
  uint64_t blk = blockAddr >> 20;
  items_in_block[ blk ] -= 1;
}



bool blocks_isInvalid( uint64_t blockAddr ) {
  uint64_t blk = blockAddr >> 20;
  if ( blk < min_block ) return true;
  return false;
}

