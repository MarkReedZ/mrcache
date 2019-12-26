
// TODO need a way to destroy and free a connection
    //conn_destroy(conn);
    //free(conn);


#include <zstd.h>
#include <signal.h>
#include <getopt.h>
#include <stdarg.h>
#include "common.h"
#include "mrcache.h"
#include <netinet/in.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>

#include "mrloop.h"

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
  int iov_index;

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

static void setup() {
  mrq_ht = malloc( sizeof(hashtable_t) );
  ht_init(mrq_ht);
  blocks_init();

  char *k = "0123456";
  printf("0: %lx\n", CityHash64(k, 1));      
  printf("1: %lx\n", CityHash64(k+1, 1));      
  printf("2: %lx\n", CityHash64(k+2, 1));      
  printf("3: %lx\n", CityHash64(k+3, 1));      
  printf("4: %lx\n", CityHash64(k+4, 1));      
  printf("01: %lx\n", CityHash64(k, 2));      
}

static void print_buffer( char* b, int len ) {
  for ( int z = 0; z < len; z++ ) {
    printf( "%02x ",(int)b[z]);
    //printf( "%c",b[z]);
  }
  printf("\n");
}

void *setup_conn(int fd, char **buf, int *buflen ) {
  //printf("New Connection\n");
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
  free(c->buf);
  free(c->recv_buf);
  free(c->out_buf);
  free(c);
}

// TODO delete?
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

/*
static int bytes_written = 0;
static int items_written = 0;
static void test_data( my_conn_t *c, char *in, int sz ) {
  static char buf[512*1024*1024];
  static char *bp = buf;
  static char *p = buf;
  static int blen = 0;
  bytes_written += sz;

  memcpy( bp, in, sz );
  blen += sz;
  bp += sz;

  if ( blen > 256*1024*1024 ) { printf(" DELME more than 256m \n"); exit(1); }

  if ( blen < 6 ) return;

  //DELME printf("  blen %d sz %d\n", blen, sz);
  while ( (p-buf) < (blen-6) ) {

    if ( p[0] != 0 ) { printf(" p0 is not 0\n"); goto prt; }
    if ( p[1] != 1 ) { printf(" p1 is not 1\n"); goto prt; }
  
    unsigned int  l  = *((unsigned int*)(p+2));
    if ( l != 5000 ) { printf(" sz is not 5000 its %d\n", l ); goto prt; }
    if ( (p-buf) + l > blen ) break;
    //if ( blen < 6 + l ) break;
  
    p += l+6;
    //blen -= l + 6;
    items_written += 1;
  }

  //if ( blen ) memcpy( buf, p, blen ); 
 
  return;
prt:
  print_buffer( p, 16 );
  printf("\n blen %d num items %d\n",blen,items_written);
  FILE *fp = fopen( "err.out", "wb" );
  fwrite(buf, 1, blen, fp);
  fclose(fp);
  exit(1); 
}
*/

static int conn_flush( my_conn_t *c ) {
  ssize_t nwritten = 0;

  while ( c->out_cur_sz > 0 ) {
    nwritten = write(c->fd, c->out_p, c->out_cur_sz);
    if ( nwritten <= 0 ) {
      if ( nwritten == -1 && errno == EAGAIN ) {
        mr_add_write_callback( loop, can_write, c, c->fd );
      }
      c->stalled = true;
      return 0; // TODO -1 and not EAGAIN? Close connection? Make socket blocking and kill client
    }
    //test_data( c, c->out_p, nwritten );
    c->out_p += nwritten;
    c->out_cur_sz -= nwritten;
  }
  c->out_p = c->out_buf;
  return 1;
}
static int conn_append_out( my_conn_t* c, char *p, int sz ) {
  //DBG printf(" append out %d \n", c->out_cur_sz);
  if ( c->out_p != c->out_buf ) return sz;
  int rlen = BUFFER_SIZE - c->out_cur_sz;
  int len = sz < rlen ? sz : rlen;
  memcpy( c->out_buf + c->out_cur_sz, p, len ); 
  c->out_cur_sz += len;

  //if ( sz != len ) printf("YAY partial out buffer append sz %d len %d\n", sz, len);
  
  return sz - len;
}
static int conn_write_command( my_conn_t *conn ) {
  if ( conn->out_cur_sz < 2 && !conn_flush(conn) ) return 0; 
  conn_append_out( conn, resp_get, 2 );
  return 1;
}
// Returns the number of bytes written if partial otherwise 0 for success
//  TODO this can probably return 0 if we're already stalled!
static int conn_write_buffer( my_conn_t* conn, char *buf, int bsz ) {
  // If we didn't append all of our data to the output buffer
  // flush it and continue until done or stalled
  int sz = conn_append_out( conn, buf, bsz );

  char *p = buf + bsz-sz;
  while ( sz ) {
    if ( !conn_flush(conn) ) {
      // If we were unable to write out the whole buffer do so later
      if ( bsz-sz == 0 ) {
        printf("DELME Yeah we can't return 0 here!\n");
        exit(1);
      }
      return bsz-sz; 
    }

    int rem = conn_append_out( conn, p, sz );
    p += sz-rem;
    sz = rem;
  }
  return 0;
}



static void conn_process_queue_comp( my_conn_t *conn ) {

  while ( conn->getq_head ) {
    getq_item_t *qi = conn->getq_head;
    if ( qi->item ) {
      item *it = qi->item;
      unsigned long long const decomp_sz = ZSTD_getFrameContentSize(it->data, it->size);

      // Write cmd bytes or if that fails bail
      if ( conn_write_command( conn ) ) {
        char *dbuf = malloc( decomp_sz+4 );
        int decomp_size = ZSTD_decompress( dbuf+4, decomp_sz, it->data, it->size );
        uint32_t *p32 = (uint32_t*)(dbuf);
        *p32 = decomp_size;
        int n = conn_write_buffer( conn, dbuf, decomp_size+4 );

        if ( n ) {
          qi->item = NULL;
          qi->buf = dbuf;
          qi->cur = n;
          qi->sz  = decomp_size+4-n;
          return;
        } 
        else free(dbuf);
      } else {
        return;
      }

    } else if ( qi->buf ) {

      int n = conn_write_buffer( conn, qi->buf+qi->cur, qi->sz );
      if ( n ) {
        qi->cur += n;
        qi->sz -= n;
        return;
      }
      
      free(qi->buf);
    }

    conn->getq_head = qi->next;
    if ( conn->getq_head == NULL ) conn->getq_tail = NULL;
    free(qi);

  }

  // Flush the buffer when done
  conn_flush(conn);
     
}

static void conn_process_queue( my_conn_t *conn ) {
  while ( conn->getq_head ) {
    getq_item_t *qi = conn->getq_head;

    if ( qi->item ) {

      // Make sure we have room for the cmd bytes
      if ( conn->out_cur_sz < 2 && !conn_flush(conn) ) return;

      item *it = qi->item;
      conn_append_out( conn, resp_get, 2 );
      char *p = ((char*)it)+2;
      int n = conn_write_buffer( conn, p, it->size+4 );
      // If we stalled n is non zero and the number of bytes written
      if ( n ) {
        qi->item = NULL;
        qi->buf = p;
        qi->cur = n;
        qi->sz  = it->size+4-n;
        //printf("DELME return from pq due to stall\n");
        return;
      } 

    } else if ( qi->buf ) {
      
      int n = conn_write_buffer( conn, qi->buf + qi->cur, qi->sz );
      if ( n ) { 
        qi->cur += n;
        qi->sz -= n;
        return;
      }

    }

    conn->getq_head = qi->next;
    if ( conn->getq_head == NULL ) conn->getq_tail = NULL;// TODO do we need this
    // TODO free? We need a flag to free or not a buffer

  }
  // Flush the buffer when done
  conn_flush(conn);
}

void can_write( void *conn, int fd ) {

  my_conn_t *c = conn;
  if ( !conn_flush(c) ) return;
  c->stalled = false;

  // Add any queue'd items to the buffer
  if ( COMPRESSION_ENABLED ) conn_process_queue_comp( c );
  else                       conn_process_queue( c );

}

static void conn_queue_item( my_conn_t* conn, item *it ) {
  getq_item_t *qi = calloc( 1, sizeof(getq_item_t) );
  qi->item = it; 
  qi->block = mrq_ht->lastBlock;
  if ( conn->getq_head == NULL ) {
    conn->getq_head = conn->getq_tail = qi;
    return;
  }
  conn->getq_tail->next = qi;
  conn->getq_tail = qi;
}

static void conn_queue_buffer( my_conn_t* conn, char *buf, int sz ) {
  getq_item_t *qi = calloc( 1, sizeof(getq_item_t) );
  // TODO last block may not be accurate ( processing queue ) so pass in the block id
  qi->block = mrq_ht->lastBlock; // If we manage to LRU the block don't try to access the data later
  qi->buf = buf;
  qi->sz = sz; 
  if ( conn->getq_head == NULL ) {
    conn->getq_head = conn->getq_tail = qi;
    return;
  }
  conn->getq_tail->next = qi;
  conn->getq_tail = qi;
}
static void conn_queue_buffer2( my_conn_t* conn, char *buf, int sz, int nwritten ) {
  getq_item_t *qi = calloc( 1, sizeof(getq_item_t) );
  // TODO last block may not be accurate ( processing queue ) so pass in the block id
  qi->block = mrq_ht->lastBlock; // If we manage to LRU the block don't try to access the data later
  qi->buf = buf;
  qi->cur = nwritten;
  qi->sz = sz-nwritten; 
  if ( conn->getq_head == NULL ) {
    conn->getq_head = conn->getq_tail = qi;
    return;
  }
  conn->getq_tail->next = qi;
  conn->getq_tail = qi;
}

static void conn_write_item( my_conn_t* conn, item *it ) {

  if ( conn->out_cur_sz < 2 ) {
    if ( !conn_flush(conn) ) {
      conn_queue_item( conn, it );
      return;
    }
  }
  conn_append_out( conn, resp_get, 2 );
  int n =  conn_write_buffer( conn, ((char*)it)+2, it->size+4 );
  if ( n ) conn_queue_buffer( conn, ((char*)it)+2+n, it->size+4-n );

}

void finishOnData( my_conn_t *conn ) {
  if ( !conn->stalled ) {
    if (conn->getq_head) {
      if ( COMPRESSION_ENABLED ) conn_process_queue_comp( conn );
      else                       conn_process_queue( conn );
    }
    if ( conn->out_cur_sz > 0 ) {
      conn_flush(conn);
    }
  }
}



void on_data(void *c, int fd, ssize_t nread, char *buf) {
  my_conn_t *conn = (my_conn_t*)c;
  char *p = NULL;
  int data_left = nread;

  // TODO close in mr loop and have a closed callback
  if ( nread < 0 ) { 
    printf(" DELME client closed the connection \n");
    free_conn(c);
    return;
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
      return;
    }
  } else {
    p = buf;
  }

  while ( data_left > 0 ) {

    if ( data_left < 2 ) {
      conn_append( conn, p, data_left );
      conn->needs = 2;
      finishOnData(conn);
      return;
    }

    int cmd   = (unsigned char)p[1];


    DBG printf(" num %d left %d\n", num_writes, data_left );
    DBG print_buffer( p, (data_left>32) ? 32 : data_left  );

    if ( cmd == GET ) {

      //num_items += 1; if ( num_items%1000==0 ) printf(" num_items %d\n", num_items);
      if ( data_left < 4 ) {
        conn_append( conn, p, data_left );
        conn->needs = 4;
        finishOnData(conn);
        return;
      }
      uint16_t keylen = *((uint16_t*)(p+2));
      char *key = p+4;

      if ( data_left < 4+keylen ) {
        conn_append( conn, p, data_left );
        conn->needs = 4+keylen;
        finishOnData(conn);
        return;
      }

      DBG_READ printf("get key %d >%.*s<\n", keylen, keylen, key );
      unsigned long hv = CityHash64(key, keylen);      
      item *it = ht_find(mrq_ht, key, keylen, hv);

      if ( it ) { // Found

        if ( COMPRESSION_ENABLED ) {

          if ( conn->getq_head ) conn_queue_item( conn, it );
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
              conn_queue_item( conn, it );
            }

          }

        } else { // No compression

          // If we have items queued up add this one 
          if ( conn->getq_head ) conn_queue_item( conn, it );
          else                   conn_write_item( conn, it );
          DBG_READ printf("getkey: found >%.*s<\n", it->size, it->data);

        }
      } else {

        int n =  conn_write_buffer( conn, resp_get_not_found,   resp_get_not_found_len );
        if ( n ) conn_queue_buffer( conn, resp_get_not_found+n, resp_get_not_found_len-n );

        DBG_READ printf("getkey - not found\n");
      }

      p += 4 + keylen;
      data_left -= 4 + keylen;
      num_writes += 1;

    } else if ( cmd == SET ) {

      if ( data_left < 8 ) {
        conn_append( conn, p, data_left );
        conn->needs = 8;
        finishOnData(conn);
        return;
      }
      uint16_t keylen = *((uint16_t*)(p+2));
      uint32_t vlen = *((uint32_t*)(p+4));

      if ( data_left < 8+keylen+vlen ) {
        conn_append( conn, p, data_left );
        conn->needs = 8+keylen+vlen;
        finishOnData(conn);
        return;
      }

      char *key = p+8;
      DBG printf("set key %d >%.*s<\n", keylen, keylen, key );
      char *val = p+8+keylen;
      DBG printf("set val %d >%.*s<\n", vlen, vlen, val );

      uint64_t blockAddr;
      item *it;

      if ( COMPRESSION_ENABLED ) {
        blockAddr = blocks_alloc( sizeof(item) + vlen + keylen + 64 );
        it = blocks_translate( blockAddr );
        int rc = ZSTD_compress( it->data, vlen+64, p+8+keylen, vlen, 3 ); // instead of 64 use ZSTD_COMPRESSBOUND?

        //if ( ZSTD_isError(rc) ) {
          //const char *err = ZSTD_getErrorName(rc);
          //printf("DELME zstd error %s\n", err );
          //exit(1);
        //}

        // If successful store it otherwise ignore this cmd
        if ( rc > 0 ) {
          int compressed_size = it->size = rc;
          it->keysize = keylen;
          memcpy( it->data+compressed_size, p+8, keylen ); // Key goes after val
          data_left -= (8 + keylen + vlen);
          p += 8 + keylen + vlen;

          // Store the number of bits so if on disk we can allocate a big enough buffer to hold it
          if ( settings.disk_size ) blockAddr |= num_bits64(vlen+8+keylen) << 20; // TODO 8 is the item size?... just do sizeof?
          unsigned long hv = CityHash64(key, keylen);      
          ht_insert( mrq_ht, blockAddr, key, keylen, hv );

        } 

      } else {

        blockAddr = blocks_alloc( sizeof(item) + vlen + keylen );
        it = blocks_translate( blockAddr );
        DBG_SET printf(" set - it %p baddr %lx\n", it, blockAddr);
        it->size = vlen;
        memcpy( it->data, p+8+keylen, vlen ); // Val

        it->keysize = keylen;
        memcpy( it->data+vlen, p+8, keylen ); // Key goes after val

        data_left -= (8 + keylen + vlen);
        p += 8 + keylen + vlen;

        // Store the number of bits so if on disk we can allocate a big enough buffer to hold it
        if ( settings.disk_size ) blockAddr |= num_bits64(vlen+8+keylen) << 20; // TODO 8 is the item size?... just do sizeof?
        unsigned long hv = CityHash64(key, keylen);      
        ht_insert( mrq_ht, blockAddr, key, keylen, hv );
      }

  
      num_writes += 1;

    } else { // Close connection? TODO
      data_left = 0;
    }
  } 

  finishOnData(conn);

  return;
}

static void sig_handler(const int sig) {
  printf("Signal handled: %s.\n", strsignal(sig));
  exit(EXIT_SUCCESS);
}

static void usage(void) {
  printf( "Mrcache Version 0.1\n"
          "    -h, --help                    This help\n"
          "    -p, --port=<num>              TCP port to listen on (default: 7000)\n"
          "    -m, --max-memory=<megabytes>  Maximum amount of memory in mb (default: 256)\n"
          "    -z, --zstd                    Enable zstd compression in memory\n"
          "\n"
        );
}

int main (int argc, char **argv) {

  signal(SIGPIPE, SIG_IGN);

  char *shortopts =
    "h"
    "m:"
    "p:"
    "z"
    "d:"
    ;
  const struct option longopts[] = {
    {"help",             no_argument, 0, 'h'},
    {"max-memory", required_argument, 0, 'm'},
    {"port",       required_argument, 0, 'p'},
    {"zstd",             no_argument, 0, 'z'},
    {"disk",       required_argument, 0, 'd'},
    {0, 0, 0, 0}
  };

  settings.port = 7000;
  settings.max_memory = 512;
  settings.flags = 0;
  settings.disk_size = 0;

  int optindex, c;
  while (-1 != (c = getopt_long(argc, argv, shortopts, longopts, &optindex))) {
    switch (c) {
    case 'p':
      settings.port = atoi(optarg);
      break;
    case 'm':
      settings.max_memory = atoi(optarg);
      break;
    case 'z':
      ENABLE_COMPRESSION;
      break;
    case 'd':
      settings.disk_size = atoi(optarg);
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

  setup();

  if ( settings.disk_size ) {
    printf("Mrcache starting up on port %d with %dmb memory and %dmb of disk space allocated\n", settings.port, settings.max_memory, settings.disk_size );
  } else {
    printf("Mrcache starting up on port %d with %dmb allocated\n", settings.port, settings.max_memory );
  }

  settings.max_memory *= 1024*1024;

  loop = mr_create_loop(sig_handler);
  settings.loop = loop;
  mr_tcp_server( loop, settings.port, setup_conn, on_data );
  //mr_add_timer(loop, 10, on_timer);
  mr_run(loop);
  mr_free(loop);

  return 0;
}

// TODO writev was slower than copying to an output buffer and queue'ng items up
// If we can writev AND guarantee all writes flush and don't return EAGAIN 

            //conn->iovs[conn->iov_index].iov_base = conn->out_buf;
            //conn->iovs[conn->iov_index].iov_len = conn->out_cur_sz;
            //conn->iov_index += 1; 
            //mr_writev( loop, conn->fd, conn->iovs, conn->iov_index );
            //conn->iov_index = 0; 
            //conn->out_cur_sz = 0;
          //conn->iovs[conn->iov_index].iov_base = resp_get;
          //conn->iovs[conn->iov_index].iov_len = 2;
          //conn->iov_index += 1; 
          //conn->iovs[conn->iov_index].iov_base = ((char*)it)+2;
          //conn->iovs[conn->iov_index].iov_len = it->size+4;
          //conn->iov_index += 1; 
          //if ( conn->iov_index > 128 ) {
            //mr_writevf( loop, conn->fd, conn->iovs, conn->iov_index );
            //conn->iov_index = 0; 
          //}
