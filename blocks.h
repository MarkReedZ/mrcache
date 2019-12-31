
#pragma once

/*
typedef struct {
  void *cur;
  uint32_t curBlock;
  uint32_t curBlockSize;
} block_t;

void block_init(block_t *blk);
uint64_t block_alloc( block_t *blk, int sz );
*/

#define FSBLOCK_SIZE 64
#define BLOCK_SHIFT 25

#define GET_BLOCK(x) ((x) >> BLOCK_SHIFT);


void blocks_init();
uint64_t blocks_alloc( int sz );
bool blocks_isvalid( uint64_t blockAddr );

void *blocks_translate( uint64_t blockAddr );


void blocks_lru();
bool blocks_isNearLru( uint64_t blockAddr ); 
void blocks_decrement( uint64_t blockAddr ); 
bool blocks_isInvalid( uint64_t blockAddr );

//DELME
uint32_t blocks_num( uint64_t blockAddr ); 

void blocks_fs_write( int blk );
