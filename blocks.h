
#pragma once

#define FSBLOCK_SIZE 64

// 39 bits of block id, 5 bits of storage, 20 bits of value length 
// 30 bits block id, 4 bits shift distance, 4 bits last key byte, 5 bits storage, 21 bits value
#define BLOCK_SHIFT 38
#define SHIFT_SHIFT 30
#define KEY_SHIFT 26
#define DISK_SHIFT 21
#define GET_BLOCK(x) ((x) >> BLOCK_SHIFT)
#define GET_SHIFT(x) (((x) >> SHIFT_SHIFT)&0xFF)
#define SET_SHIFT(x,shft) do { x&=~(0xFFull << SHIFT_SHIFT); x |= ((shft&0xFF)<<SHIFT_SHIFT); } while(0)
#define GET_DISK_SIZE(x) (((x) >> DISK_SHIFT)&0x1F)
#define SET_DISK_SIZE(x,val) do { x&=~(0x1Full << DISK_SHIFT); x |= ((val&0x1F)<<DISK_SHIFT); } while(0)
#define GET_KEY(x) (((x) >> KEY_SHIFT)&0xF)
#define SET_KEY(x,val) do { x&=~(0xFull << KEY_SHIFT); x |= ((val&0xF)<<KEY_SHIFT); } while(0)


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

//DELME
uint32_t blocks_num( uint64_t blockAddr ); 

void blocks_fs_write( int blk );
void blocks_fs_read( uint64_t blockAddr, disk_item_t *di );
