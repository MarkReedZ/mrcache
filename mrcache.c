
#include <zstd.h>
#include <signal.h>
#include <getopt.h>
#include <stdarg.h>
#include <netinet/in.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>

#include "mrloop.h"

#include "common.h"
#include "mrcache.h"
#include "hashtable.h"
#include "blocks.h"
#include "city.h"

//Debug
static uint64_t bytes = 0;
static uint64_t fbytes = 0;

#define BUFFER_SIZE 32*1024

// debug
unsigned long nw = 0;
unsigned long wsz = 0;
unsigned long nr = 0;
unsigned long rsz = 0;
unsigned long tot = 0;

static mr_loop_t *loop = NULL;
struct settings settings;

uint64_t mrq_disk_blocks[3];
int      mrq_disk_reads;

hashtable_t *mrq_ht, *mrq_htnew;

#define NUM_IOVEC 256
typedef struct _conn
{
  char *buf, *recv_buf, *out_buf, *out_p;
  int out_max_sz, out_cur_sz, out_written;
  int max_sz;
  int cur_sz;
  int needs;
  int fd;

  getq_item_t *getq_head, *getq_tail;
  bool stalled;

  struct iovec iovs[NUM_IOVEC];
  int iov_start, iov_end, num_iov;
  int write_in_progress;
} my_conn_t;

struct sockaddr_in addr;

//static int total_clients = 0;  // Total number of connected clients
//static int total_mem = 0;
static int num_writes = 0;
//static int num_items = 0;

//static char item_buf[1024] = {0,1};
//static int item_len = 0;
static char resp_get[2] = {0,1};
static char resp_get_not_found[6] = {0,1,0,0,0,0};
static char resp_get_not_found_len = 6;

uint64_t num_bits64 (uint64_t x) { return 64 - __builtin_clzll(x); }

static char *zstd_buffer;
static void setup() {
  mrq_ht = malloc( sizeof(hashtable_t) );
  ht_init(mrq_ht, settings.index_size * 1024 * 1024);
  blocks_init();

  srand((int)time(NULL));

  zstd_buffer = malloc( 16 * 1024 * 1024 + 128 );

}

static void tear_down() {
  free(zstd_buffer);
}

static void print_buffer( char* b, int len ) {
  for ( int z = 0; z < len; z++ ) {
    printf( "%02x ",(int)b[z]);
    //printf( "%c",b[z]);
  }
  printf("\n");
}

void *setup_conn(int fd, char **buf, int *buflen ) {
  my_conn_t *c = calloc( 1, sizeof(my_conn_t));
  c->fd = fd;
  c->buf = malloc( BUFFER_SIZE*2 );
  c->recv_buf = malloc( BUFFER_SIZE );
  c->max_sz = BUFFER_SIZE*2;
  c->out_buf = c->out_p = malloc( BUFFER_SIZE );
  c->out_max_sz = BUFFER_SIZE;
  *buf = c->recv_buf;
  *buflen = BUFFER_SIZE;
  return c;
}

void free_conn( my_conn_t *c ) {
  mr_close( loop, c->fd );
  free(c->buf);
  free(c->recv_buf);
  free(c->out_buf);
  // TODO free the output queue items?
  free(c);
}

static void conn_append( my_conn_t* c, char *data, int len ) {
  DBG printf(" append cur %d \n", c->cur_sz);

  if ( (c->cur_sz + len) > c->max_sz ) {
    while ( (c->cur_sz + len) > c->max_sz ) c->max_sz <<= 1;
    c->buf = realloc( c->buf, c->max_sz );
  }

  memcpy( c->buf + c->cur_sz, data, len ); 
  c->cur_sz += len;

  DBG printf(" append cur now %d %p \n", c->cur_sz, c->buf);

}

void on_write_done(void *user_data, int res) {
  //printf("on_write_done - res %d conn %p\n", res, user_data);

  if ( res < 0 ) {
    printf(" err: %s\n",strerror(-res));
    exit(-1); // TODO
  }
  my_conn_t *conn = (my_conn_t*)user_data;
  conn->write_in_progress = 0;
  conn->iov_start = 0;
  conn->iov_end = 0;
}

void finishOnData( my_conn_t *conn ) {
  if ( conn->write_in_progress == 1 ) {
    printf("Got more cmds while writes are still in progress\n");
    exit(1);
  }
  if ( conn->iov_end > conn->iov_start ) {
    //printf("DELME writing %d items\n", conn->iov_end-conn->iov_start);
    mr_writevcb( loop, conn->fd, &(conn->iovs[conn->iov_start]), conn->iov_end-conn->iov_start , (void*)conn, on_write_done  );
    mr_flush(loop);
    conn->write_in_progress = 1;
  }
}



int on_data(void *c, int fd, ssize_t nread, char *buf) {
  my_conn_t *conn = (my_conn_t*)c;
  char *p = NULL;
  int data_left = nread;

  if ( nread == 0 ) { 
    free_conn(c);
    return 1;
  }

  if ( nread < 0 ) { 
    free_conn(c);
    return 1;
  }
  bytes += nread; 

  // If we have partial data for this connection
  if ( conn->cur_sz ) {
    conn_append( conn, buf, nread );
    if ( conn->cur_sz >= conn->needs ) {
      p = conn->buf;
      data_left = conn->cur_sz;
      conn->cur_sz = 0;
    } else {
      return 0;
    }
  } else {
    p = buf;
  }

  while ( data_left > 0 ) {

    if ( data_left < 2 ) {
      conn_append( conn, p, data_left );
      conn->needs = 2;
      finishOnData(conn);
      return 0;
    }

    // TODO check the first byte and bail if its bad?
    int cmd   = (unsigned char)p[1];

    DBG printf(" num %d left %d\n", num_writes, data_left );
    DBG print_buffer( p, (data_left>32) ? 32 : data_left  );

    if ( cmd == GET ) {

      //num_items += 1; if ( num_items%1000==0 ) printf(" num_items %d\n", num_items);
      if ( data_left < 4 ) {
        conn_append( conn, p, data_left );
        conn->needs = 4;
        finishOnData(conn);
        return 0;
      }
      uint16_t keylen = *((uint16_t*)(p+2));
      char *key = p+4;
      DBG_READ printf("get keylen %d\n", keylen );

      if ( data_left < 4+keylen ) {
        conn_append( conn, p, data_left );
        conn->needs = 4+keylen;
        finishOnData(conn);
        return 0;
      }

      DBG_READ printf("get key %d >%.*s<\n", keylen, keylen, key );
      unsigned long hv = CityHash64(key, keylen);      
      item *it = NULL;
      int rc = ht_find(mrq_ht, key, keylen, hv, (void*)&it);
      DBG_READ printf("get ht_find rc %d\n", rc );

      settings.tot_reads += 1;

      if ( rc == 1 && it ) { // Found

	conn->iovs[conn->iov_end].iov_base = ((char*)it)+2;
	conn->iovs[conn->iov_end].iov_len  = it->size+4;
	conn->iov_end += 1;

        //DBG_READ printf("getkey: found >%.*s<\n", it->size, it->data);
        //DBG_READ print_buffer( it->data, it->size );

      } else {

	conn->iovs[conn->iov_end].iov_base = resp_get_not_found;
	conn->iovs[conn->iov_end].iov_len  = resp_get_not_found_len;
	conn->iov_end += 1;

        settings.misses += 1;
        //DBG_READ printf("getkey - not found\n");
      }

      p += 4 + keylen;
      data_left -= 4 + keylen;
      num_writes += 1;

    } else if ( cmd == SET ) {

      if ( data_left < 8 ) {
        conn_append( conn, p, data_left );
        conn->needs = 8;
        finishOnData(conn);
        return 0;
      }
      uint16_t keylen = *((uint16_t*)(p+2));
      uint32_t vlen = *((uint32_t*)(p+4));
      DBG printf("set keylen %d vlen %d\n", keylen, vlen );

      if ( data_left < 8+keylen+vlen ) {
        conn_append( conn, p, data_left );
        conn->needs = 8+keylen+vlen;
        finishOnData(conn);
        return 0;
      }

      char *key = p+8;
      DBG printf("set key %d >%.*s<\n", keylen, keylen, key );
      char *val = p+8+keylen;
      DBG printf("set val %d >%.*s<\n", vlen, vlen, val );
      DBG print_buffer( val, vlen );

      uint64_t blockAddr;
      item *it;

      settings.tot_writes += 1;

        blockAddr = blocks_alloc( sizeof(item) + vlen + keylen );
        it = blocks_translate( blockAddr );
        DBG_SET printf(" set - it %p baddr %lx\n", it, blockAddr);
        it->size = vlen;
        memcpy( it->data, p+8+keylen, vlen ); // Val

        it->keysize = keylen;
        memcpy( it->data+vlen, p+8, keylen ); // Key goes after val

        data_left -= (8 + keylen + vlen);
        p += 8 + keylen + vlen;

        unsigned long hv = CityHash64(key, keylen);      
        ht_insert( mrq_ht, blockAddr, key, keylen, hv );

      num_writes += 1;

    } else if ( cmd == GETZ ) {

      if ( data_left < 4 ) {
        conn_append( conn, p, data_left );
        conn->needs = 4;
        finishOnData(conn);
        return 0;
      }
      uint16_t keylen = *((uint16_t*)(p+2));
      char *key = p+4;
      DBG_READ printf("get keylen %d\n", keylen );

      if ( data_left < 4+keylen ) {
        conn_append( conn, p, data_left );
        conn->needs = 4+keylen;
        finishOnData(conn);
        return 0;
      }

      DBG_READ printf("get key %d >%.*s<\n", keylen, keylen, key );
      unsigned long hv = CityHash64(key, keylen);      
      item *it = NULL;
      int rc = ht_find(mrq_ht, key, keylen, hv, (void*)&it);
      DBG_READ printf("get ht_find rc %d\n", rc );

      settings.tot_reads += 1;

      if ( rc == 1 && it ) { // Found

	      /*
          if ( conn->getq_head ) conn_queue_item( conn, it, 1 );
          else { 

            unsigned long long const decomp_sz = ZSTD_getFrameContentSize(it->data, it->size);
            // Write cmd bytes or if that fails queue the item
            if ( conn_write_command( conn ) ) {
              // Decompress to a newly malloc'd buffer and flush chunks TODO
              char *dbuf = malloc( decomp_sz+4 );
              int decomp_size = ZSTD_decompress( dbuf+4, decomp_sz, it->data, it->size );
              uint32_t *p32 = (uint32_t*)(dbuf);
              *p32 = decomp_size;

              int n = conn_write_buffer( conn, dbuf, decomp_size+4 );
              DBG_READ printf(" GET conn_write_buffer n %d\n", n);
              if ( n ) conn_queue_buffer2( conn, dbuf, decomp_size+4, n ); // TODO If we Q up we need to free later
              else     free(dbuf);

            } else {
              conn_queue_item( conn, it, 1 );
            }

          }
	  */

      } else {

        //if ( conn->getq_head ) conn_queue_buffer( conn, resp_get_not_found, resp_get_not_found_len );
        //else {
          //int n =  conn_write_buffer( conn, resp_get_not_found,   resp_get_not_found_len );
          //if ( n ) conn_queue_buffer( conn, resp_get_not_found+n, resp_get_not_found_len-n );
        //}

        settings.misses += 1;
        DBG_READ printf("getkey - not found\n");
      }

      p += 4 + keylen;
      data_left -= 4 + keylen;
      num_writes += 1;

    } else if ( cmd == SETZ ) {

      if ( data_left < 8 ) {
        conn_append( conn, p, data_left );
        conn->needs = 8;
        finishOnData(conn);
        return 0;
      }
      uint16_t keylen = *((uint16_t*)(p+2));
      uint32_t vlen = *((uint32_t*)(p+4));
      DBG printf("setz keylen %d vlen %d\n", keylen, vlen );

      if ( data_left < 8+keylen+vlen ) {
        conn_append( conn, p, data_left );
        conn->needs = 8+keylen+vlen;
        finishOnData(conn);
        return 0;
      }

      char *key = p+8;
      DBG printf("setz key %d >%.*s<\n", keylen, keylen, key );
      char *val = p+8+keylen;
      DBG printf("setz val %d >%.*s<\n", vlen, vlen, val );
      DBG print_buffer( val, vlen );

      uint64_t blockAddr;
      item *it;

      settings.tot_writes += 1;

        int cmplen = ZSTD_compress( zstd_buffer, vlen+64, p+8+keylen, vlen, 3 ); // instead of 64 use ZSTD_COMPRESSBOUND?

        //if ( ZSTD_isError(rc) ) {
          //const char *err = ZSTD_getErrorName(rc);
          //printf("TODO zstd error %s\n", err );
          //exit(1);
        //}

        // If successful store it otherwise ignore this cmd
        if ( cmplen > 0 ) {

          blockAddr = blocks_alloc( sizeof(item) + cmplen + keylen );
          it = blocks_translate( blockAddr );
          it->keysize = keylen;
          it->size = cmplen;
          memcpy( it->data, zstd_buffer, cmplen ); // Copy in the compressed value
          memcpy( it->data+cmplen, p+8, keylen ); // Key goes after val

          data_left -= (8 + keylen + vlen);
          p += 8 + keylen + vlen;

          unsigned long hv = CityHash64(key, keylen);      
          ht_insert( mrq_ht, blockAddr, key, keylen, hv );

        } 

      num_writes += 1;

    } else if ( cmd == STAT ) {

      printf("STAT\n");
      printf("Total reads  %ld\n", settings.tot_reads);
      printf("Total misses %ld\n", settings.misses);
      printf("Total writes %ld\n", settings.tot_writes);
      printf("Avg shift %.2f\n", (double)settings.read_shifts/settings.tot_reads);
      printf("Max shift %d\n", settings.max_shift);
      ht_stat(mrq_ht);

      data_left -= 2;
      p += 2;

    } else { 
      // Invalid cmd
      data_left = 0;
      free_conn(conn);
      return 1; // Tell mrloop we closed the connection
    }
  } 

  finishOnData(conn);

  return 0;
}

static void sig_handler(const int sig) {
  printf("Signal handled: %s.\n", strsignal(sig));
  exit(EXIT_SUCCESS);
}

static void usage(void) {
  printf( "Mrcache Version 0.1\n"
          "    -h, --help                    This help\n"
          "    -p, --port=<num>              TCP port to listen on (default: 7000)\n"
          "    -m, --max-memory=<mb>         Maximum amount of memory in mb (default: 256)\n"
          "    -d, --max-disk=<gb>           Maximum amount of disk in gb (default: 1)\n"
          "    -i, --index-size=<mb>         Index size in mb (must be a power of 2 and sz/14 is the max number of items)\n"
          "\n"
        );
}

// ./mrcache -m 1024 -d 20 -i 256

int main (int argc, char **argv) {

  signal(SIGPIPE, SIG_IGN);

  char *shortopts =
    "h"
    "m:"
    "d:"
    "i:"
    "p:"
    ;
  const struct option longopts[] = {
    {"help",             no_argument, 0, 'h'},
    {"max-memory", required_argument, 0, 'm'},
    {"max-disk",   required_argument, 0, 'd'},
    {"index-size", required_argument, 0, 'i'},
    {"port",       required_argument, 0, 'p'},
    {"zstd",             no_argument, 0, 'z'},
    {0, 0, 0, 0}
  };

  settings.port = 7000;
  settings.max_memory = 512;
  settings.flags = 0;
  settings.disk_size = 0;
  settings.index_size = 0;
  settings.block_size = 16;

  settings.read_shifts  = 0;
  settings.tot_reads    = 0;
  settings.tot_writes   = 0;
  settings.write_shifts = 0;

  int optindex, c;
  while (-1 != (c = getopt_long(argc, argv, shortopts, longopts, &optindex))) {
    switch (c) {
    case 'p':
      settings.port = atoi(optarg);
      break;
    case 'm':
      settings.max_memory = atoi(optarg);
      break;
    case 'd':
      settings.disk_size = atoi(optarg);
      break;
    case 'i':
      settings.index_size = atoi(optarg);
      break;
    case 'h':
      usage();
      return(2);
      break;
    default:
      usage();
      return(2);
    }
  }

  if ( settings.index_size == 0 ) {
    // By default the index size is 10% of memory rounded up to a power of 2
    settings.index_size = (settings.max_memory * 0.1);
    int power = 1;
    while(power < settings.index_size) {
        power <<= 1;
    }
    settings.index_size = power;
  }
  if ( !IS_POWER_OF_TWO(settings.index_size) ) {
    printf("The index size must be a power of two\n\n");
    usage();
    return(2);
  }

  setup();

  double max_items = (double)(settings.index_size>>3)*0.70; // Each entry is 8B max 70% full
  if ( settings.disk_size ) {
    printf("Mrcache starting up on port %d with %dmb of memory and %dgb of disk allocated.  The max number of items is %0.1fm based on the index size of %dm\n", settings.port, settings.max_memory, settings.disk_size, max_items, settings.index_size );
  } else {
    printf("Mrcache starting up on port %d with %dmb allocated. Maximum items is %0.1fm\n", settings.port, settings.max_memory+settings.index_size, max_items );
  }
  printf("DELME YAY\n");
  loop = mr_create_loop(sig_handler);
  settings.loop = loop;
  mr_tcp_server( loop, settings.port, setup_conn, on_data );
  //mr_add_timer(loop, 0.1, clear_lru_timer, NULL);
  mr_run(loop);
  mr_free(loop);

  tear_down();

  return 0;
}



