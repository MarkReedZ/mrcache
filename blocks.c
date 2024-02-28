

//  Memory is statically allocated at startup as N blocks
//    When full we just drop the oldest block
//
//  When using disk we write the dropped blocks to disk and truncate the oldest when out of space
//

#include "mrcache.h"
#include "blocks.h"
#include "hashtable.h"

#include <fcntl.h>
#include <unistd.h>
//#include <sys/types.h>
#include <sys/stat.h>
#include "mrloop.h"

static int num_blocks;
static int min_block;
static uint32_t blocks_bitlen;
static uint32_t blocks_bytelen;
static void *base;
static void *cur;
static uint64_t cur_block;
static uint32_t cur_block_size;
static bool full;
static int *items_in_block;

static int *fsblock_fds;
static int fsblock_index;
static int fsblock_size;
static int fsblock_min_block;
static int num_fs_blocks;

void blocks_init() {
  blocks_bitlen = 24;
  blocks_bytelen = 0x1ull << blocks_bitlen;
  num_blocks = settings.max_memory / settings.block_size;
  base = malloc( settings.max_memory * 1024 * 1024 );
  min_block = 1;
  cur = base;
  cur_block = 1;
  cur_block_size = 0;
  items_in_block = calloc( num_blocks, sizeof(int) );

  if ( !base ) {
    fprintf(stderr, "Failed to allocate memory of %d mb\n", num_blocks);
    exit(EXIT_FAILURE);
  }

  if ( settings.disk_size ) {
    num_fs_blocks = (settings.disk_size*1024) / FSBLOCK_SIZE; 
    fsblock_fds = calloc( num_fs_blocks, sizeof(int) );  

    mkdir( "fsblocks", 0700 );
    for ( int i = 0; i < num_fs_blocks; i++ ) {
      char fn[128];
      sprintf( fn, "fsblocks/fsblock.%d", i );
      int fd = open(fn, O_RDWR | O_CREAT | O_TRUNC | O_APPEND, 0600);
      if (fd < 0) { exit(EXIT_FAILURE); }
      fsblock_fds[i] = fd;
    }
    fsblock_index = 0;
    fsblock_size = 0;
    fsblock_min_block = min_block;
  }
}

// Allocate memory of sz bytes 
uint64_t blocks_alloc( int sz ) {

  // If the current block cannot hold the new item 
  if ( (cur_block_size+sz) > ((blocks_bytelen)-1) ) { // TODO make this a constant.  No variable block size

    if ( full ) blocks_lru();
    else if ( cur_block == num_blocks-1 ) full = true; // Full is set once ever

    cur_block += 1;
    items_in_block[ cur_block%num_blocks ] = 0;
    cur = base + ((cur_block%num_blocks)<<blocks_bitlen);
    cur_block_size = 0;

  }

  cur += sz;
  cur_block_size += sz;
  items_in_block[ cur_block%num_blocks ] += 1; // So we can tell the hashtable how many items were dropped on LRU

  return (cur_block << BLOCK_SHIFT) | (cur_block_size-sz);
}

void blocks_lru() {
  int i = min_block%num_blocks;
  int n = items_in_block[ i ];

  if ( settings.disk_size ) { 
    blocks_fs_write(i);
  } else {
    fsblock_min_block = min_block+1;
  }

  ht_decrement(mrq_ht, n); // Tell the hashtable how many items were dropped TODO what about items on disk?
  items_in_block[ i ] = 0;
  min_block += 1;  // Items dropped are still in the hashtable. Min block tells us if they are still valid in memory or not
  
}

void *blocks_translate( uint64_t blockAddr ) {
  uint64_t blk = GET_BLOCK(blockAddr);
  if ( blk < min_block ) return NULL;
  uint32_t off = blockAddr & BLOCK_BITMASK; 
  return (base + ((blk%num_blocks)<<blocks_bitlen)) + off;
}

bool blocks_isvalid( uint64_t blockAddr ) {
  uint64_t blk = GET_BLOCK(blockAddr);
  if ( blk < min_block ) return false;
  return true;
}

bool blocks_is_near_lru( uint64_t blockAddr ) {
  if ( !full ) return false;
  uint64_t blk = GET_BLOCK(blockAddr);
  if ( blk < (min_block + 4) ) return true;
  return false;
}

void blocks_decrement( uint64_t blockAddr ) {
  uint64_t blk = GET_BLOCK(blockAddr);
  items_in_block[ blk%num_blocks ] -= 1;
}
uint32_t blocks_num( uint64_t blk ) {
  return items_in_block[ blk%num_blocks ];
}

bool blocks_is_invalid( uint64_t blockAddr ) {
  uint64_t blk = GET_BLOCK(blockAddr);
  if ( blk < min_block ) return true;
  return false;
}

// Only call this if you know its not mem
bool blocks_is_disk( uint64_t blockAddr ) {
  uint64_t blk = GET_BLOCK(blockAddr);
  //if ( blk >= fsblock_min_block ) return true;
  if ( blk < min_block && blk >= fsblock_min_block ) return true;
  return false;
}

bool blocks_is_mem( uint64_t blockAddr ) {
  uint64_t blk = GET_BLOCK(blockAddr);
  if ( blk >= min_block ) return true;
  return false;
}

bool blocks_is_lru( uint64_t blockAddr ) {
  uint64_t blk = GET_BLOCK(blockAddr);
  //if ( blk < min_block && blk >  ) return true; // TODO rollover?
  if ( blk < fsblock_min_block ) return true; // When not using disk fsblock min needs to be equal to min_block
  return false;
}

void blocks_on_write_done( void *iov, int res ) {
  free(((struct iovec*)iov)->iov_base);
  free(iov);
}
void blocks_fs_write( int blk ) {

  char *p = malloc(blocks_bytelen);
  memcpy(p, base + ((blk%num_blocks)<<blocks_bitlen), blocks_bytelen);

  struct iovec *iov = malloc(sizeof(struct iovec));
  iov->iov_base = p;
  iov->iov_len = settings.block_size*1024*1024;

  if ( fsblock_min_block == -1 ) { 
    fsblock_min_block = blk; 
  } 
  fsblock_size += 1;

  int fd = fsblock_fds[fsblock_index];
  mr_writevcb( settings.loop, fd, iov, 1, (void*)iov, blocks_on_write_done  );
  mr_flush(settings.loop);

  if ( fsblock_size == 64 ) {
    fsblock_index = (fsblock_index+1)%num_fs_blocks;
    fsblock_size = 0;
    fsblock_min_block += 64;
    int rc = ftruncate( fsblock_fds[fsblock_index], 0 );// TODO test return code?
  }

}

void blocks_on_read_done( void *ptr, int res ) {

  //disk_item_t *di = (disk_item_t*)ptr;
  //getq_item_t *qi = di->qi;
  //qi->reads_done += 1;
  //conn_process_queue( di->conn );

  //free(((struct iovec*)iov)->iov_base);
  //free(iov);
}

void blocks_fs_read( uint64_t blockAddr, disk_item_t *di ) {
  uint64_t blk = GET_BLOCK(blockAddr);
  
  int fsblk = (blk/64)%num_fs_blocks;
  int fd = fsblock_fds[fsblk];

  int sz = 32 * 1024;
  di->iov.iov_base = malloc( sz );
  di->iov.iov_len = sz;

  off_t off = ((blk-1)*blocks_bytelen) + (blockAddr & BLOCK_BITMASK);

  // Passing di works as the iov is the first part of the struct
  mr_readvcb( settings.loop, fd, (struct iovec*)di, 1, off, di, blocks_on_read_done  );
  mr_flush(settings.loop);


}

