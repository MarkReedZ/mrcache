


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
static void *base;
static void *cur;
static uint32_t cur_block;
static uint32_t cur_block_size;
static bool full;
static int *items_in_block;

static int *fsblock_fds;
static int fsblock_index;
static int fsblock_size;
static int fsblock_min_block;
static int num_fs_blocks;

#define FSBLOCK_SIZE 64

void blocks_init() {
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

  if ( settings.disk_size ) {
    num_fs_blocks = settings.disk_size / FSBLOCK_SIZE; 
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
    fsblock_min_block = -1;
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
  int i = min_block%num_blocks;
  int n = items_in_block[ i ];
  //printf("DELME lru block %d num items %d\n", min_block,n );

  if ( settings.disk_size ) {
    blocks_fs_write(i);
  }

  // Which ht to decrement?
  ht_decrement(mrq_ht, n); 
  items_in_block[ i ] = 0;
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

void blocks_on_write_done( void *iov ) {
  free(iov);
}
void blocks_fs_write( int blk ) {
  char *p =   base + ((blk%num_blocks)<<20);

  struct iovec *iov = malloc(8);//sizeof(struct iovec));
  iov->iov_base = p;
  iov->iov_len = 1024*1024;

  if ( fsblock_min_block == -1 ) { 
    fsblock_min_block = blk; 
  } 
  fsblock_size += 1;

  int fd = fsblock_fds[fsblock_index];
  mrWritevcb( settings.loop, fd, iov, 1, (void*)iov, blocks_on_write_done  );
  mrFlush(settings.loop);

  if ( fsblock_size == 64 ) {
    printf(" DELME fsblock %d full\n", fsblock_index);
    fsblock_index = (fsblock_index+1)%num_fs_blocks;
    fsblock_size = 0;
    fsblock_min_block += 64;
    int rc = ftruncate( fsblock_fds[fsblock_index], 0 );
  }

}

// TODO
// Read - blk-low/64 is the fsblock. %64 is the 1mb then addr
//      - Read into the output buffer? Sure keep count of reads outstanding and don't flush until its 0. 
//      - If buffer full read into temp buffer and queue.  When we get to one of these and the read is outstanding we stop and set flag.
//        In the read done callback if the flag is set we copy into the output buffer and continue processing that queue. 
//        

