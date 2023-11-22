
#pragma once

// TODO Block ids will loop back to 0 after 128 TB of writes

#define FSBLOCK_SIZE 128

// 28 bits block id, 12 bits key hash, 24 bits value
#define BLOCK_BITMASK  0xFFFFFFull

#define BLOCK_SHIFT 36
#define KEY_SHIFT 24
#define GET_BLOCK(x) ((x) >> BLOCK_SHIFT)
#define GET_KEY(x) (((x) >> KEY_SHIFT)&0xFFFull)
#define SET_KEY(x,val) do { x&=~(0xFFFull << KEY_SHIFT); x |= ((val&0xFFFull)<<KEY_SHIFT); } while(0)


void blocks_init();
uint64_t blocks_alloc( int sz );
bool blocks_isvalid( uint64_t blockAddr );

void *blocks_translate( uint64_t blockAddr );


void blocks_lru();
bool blocks_is_near_lru( uint64_t blockAddr ); 
void blocks_decrement( uint64_t blockAddr ); 
bool blocks_is_invalid( uint64_t blockAddr );
bool blocks_is_disk( uint64_t blockAddr );
bool blocks_is_mem( uint64_t blockAddr );
bool blocks_is_lru( uint64_t blockAddr );

uint32_t blocks_num( uint64_t blockAddr ); 

void blocks_fs_write( int blk );
void blocks_fs_read( uint64_t blockAddr, disk_item_t *di );
