
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


#define BUFFER_SIZE 32*1024

// debug
unsigned long nw = 0;
unsigned long wsz = 0;
unsigned long nr = 0;
unsigned long rsz = 0;
unsigned long tot = 0;

static mr_loop_t *loop = NULL;
struct settings settings;

hashtable_t *mrq_ht;

#define NUM_IOVEC 256
typedef struct _conn
{
  char *buf, *recv_buf, *out_buf, *out_p;
  int out_max_sz, out_cur_sz, out_written;
  int max_sz;
  int cur_sz;
  int needs;
  int fd;

  void *getq_head;

  struct iovec iovs[NUM_IOVEC];
  int iov_index;

  int write_in_progress;
} my_conn_t;

#define RESIZE_OUT_BUFFER_IF_NECESSARY( c, sz ) \
  if ( (c->out_cur_sz + sz) > c->out_max_sz ) { \
    while ( (c->out_cur_sz + sz) > c->out_max_sz ) c->out_max_sz <<= 1; \
    c->out_buf = realloc( c->out_buf, c->out_max_sz ); \
  }

struct sockaddr_in addr;

//static int total_clients = 0;  // Total number of connected clients
//static int total_mem = 0;
static int num_writes = 0;
static double start_time = 0;

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
  c->out_buf = malloc( BUFFER_SIZE );
  c->out_max_sz = BUFFER_SIZE;
  *buf = c->recv_buf;
  *buflen = BUFFER_SIZE;
  return c;
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
static void conn_append_out( my_conn_t* c, char *data, int len ) {
  DBG printf(" append out %d \n", c->out_cur_sz);

  //if ( (c->out_cur_sz + len) > c->out_max_sz ) {
    //while ( (c->out_cur_sz + len) > c->out_max_sz ) c->out_max_sz <<= 1;
    //c->out_buf = realloc( c->out_buf, c->out_max_sz );
  //}

  memcpy( c->out_buf + c->out_cur_sz, data, len ); 
  c->out_cur_sz += len;

  DBG printf(" append cur now %d %p \n", c->out_cur_sz, c->out_buf);
}
static int conn_flush( my_conn_t* c ) {
  ssize_t nwritten = 0;
  char *p = c->out_buf;
  while ( c->out_cur_sz > 0 ) {
    nwritten = write(c->fd, p, c->out_cur_sz);
    if ( nwritten <= 0 ) return 0; // TODO -1 and not EAGAIN? Close connection?
    p += nwritten;
    c->out_cur_sz -= nwritten;
  }
  return 1;
}


void finishOnData( my_conn_t *conn ) {
  if ( conn->iov_index > 0 ) {
    mr_writevf( loop, conn->fd, conn->iovs, conn->iov_index );
    conn->iov_index = 0; 
  }
}

// TODO If the client got backed up 
void can_write( void *conn, int fd ) {
  //int l = BUFSIZE;
  //int nwritten = write(fd,buf,l);
  //printf("can_write nwritten %d\n",nwritten);
  //exit(-1);
}


void on_data(void *c, int fd, ssize_t nread, char *buf) {
  my_conn_t *conn = (my_conn_t*)c;
  char *p = NULL;
  int data_left = nread;

  //printf( " %d writes time taken %f \n ", num_writes, ((double)(clock()-start_time))/CLOCKS_PER_SEC );
  DBG printf("on_data nread %ld nw %d\n", nread, num_writes);
  //DBG if (conn->needs) { printf("needs %d\n", conn->needs);}
  //print_buffer( buf, (nread>32) ? 32 : nread  );

  // If we have partial data for this connection
  if ( conn->cur_sz ) {
    DBG printf("Received %d more bytes need %d\n",(int)nread, conn->needs);
    //conn_print( conn );
    conn_append( conn, buf, nread );
    DBG print_buffer( conn->buf, 32 );
    DBG printf("cur is now %d\n",conn->cur_sz);
    //conn_print( conn );
    if ( conn->cur_sz >= conn->needs ) {
      p = conn->buf;
      data_left = conn->cur_sz;
      DBG printf("Got enough %d num wirtes %d\n", data_left, num_writes);
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
      DBG printf("Received partial data %d more bytes need %d\n",data_left, conn->needs);
      finishOnData(conn);
      return;
    }

    int cmd   = (unsigned char)p[1];


    DBG printf(" num %d left %d\n", num_writes, data_left );
    DBG print_buffer( p, (data_left>32) ? 32 : data_left  );

    if ( cmd == GET ) {

      if ( data_left < 4 ) {
        conn_append( conn, p, data_left );
        conn->needs = 4;
        finishOnData(conn);
        DBG printf(" Not enough for a get left %d ", data_left );
        DBG print_buffer( p, data_left );
        return;
      }
      uint16_t keylen = *((uint16_t*)(p+2));
      char *key = p+4;

      DBG_READ printf("get key >%.*s<\n", keylen, key );
      unsigned long hv = CityHash64(key, keylen);      
      item *it = ht_find(mrq_ht, key, keylen, hv);

      if ( it ) { // Found

        if ( COMPRESSION_ENABLED ) {

          unsigned long long const decomp_sz = ZSTD_getFrameContentSize(it->data, it->size);
          RESIZE_OUT_BUFFER_IF_NECESSARY( conn, decomp_sz+6 ) // 2B cmd + 4B len + data
          int decomp_size = ZSTD_decompress( conn->out_buf+6+conn->out_cur_sz, decomp_sz, it->data, it->size );
          DBG printf("getkey: decompressed >%.*s<\n", decomp_size, conn->out_buf+6+conn->out_cur_sz);
          char *ob = conn->out_buf + conn->out_cur_sz;
          ob[0] = 0; ob[1] = 1;
          uint32_t *p32 = (uint32_t*)(ob+2);
          *p32 = decomp_size;
          conn->out_cur_sz += 6 + decomp_size;
          // if out_cur_sz > some num then write the output buffer?

          if ( conn->out_cur_sz > 16*1024 ) {

/*
            ssize_t nwritten, totlen = 0, count = conn->out_cur_sz;
            char *b = conn->out_buf;
            while(totlen != count) {
              nwritten = write(conn->fd,b,count-totlen);
              if (nwritten == 0) { printf("nwritten 0\n"); exit(-1); }
              if (nwritten == -1) { printf("nwritten -1 %d %s\n", errno, strerror(errno)); exit(-1); }
              totlen += nwritten;
              b += nwritten;
            }
*/
            conn->iovs[conn->iov_index].iov_base = conn->out_buf;
            conn->iovs[conn->iov_index].iov_len = conn->out_cur_sz;
            conn->iov_index += 1; 
            mr_writevf( loop, conn->fd, conn->iovs, conn->iov_index );
            conn->iov_index = 0; 
            conn->out_cur_sz = 0;
          }


        } else { // No compression

          // If read from disk then flush, read and Q up?
          // If larger than output buffer flush chunks

          // Copy to the output buffer if it fits
          if ( (conn->out_cur_sz + it->size+6)  <= BUFFER_SIZE ) {
            conn_append_out( conn, resp_get, 2 );
            conn_append_out( conn, ((char*)it)+2, it->size+4 );
          } else {

            // Otherwise flush output buffer

            if ( !conn_flush(conn) ) {

              printf("DELME not flushed\n"); exit(-1); // TODO Test the q and do large item chunks

              // If we were unable to write out the whole buffer do so later
              mr_add_write_callback( loop, can_write, NULL, fd );

              // push to Q
              getq_item_t *qi = calloc( 1, sizeof(getq_item_t) );
              qi->item = it;
              qi->next = conn->getq_head;
              conn->getq_head = qi;
              
            } else {
              conn->out_cur_sz = 0;
              conn_append_out( conn, resp_get, 2 );
              conn_append_out( conn, ((char*)it)+2, it->size+4 );
              // TODO If the item is larger than the buffer chunk it
              //if ( it->size+6 > BUFFER_SIZE ) {
              //} else {
              //}
            }
          }


          DBG_READ printf("getkey: found >%.*s<\n", it->size, it->data);
        }
      } else {

          conn_append_out( conn, resp_get_not_found, resp_get_not_found_len );

        /*
        conn->iovs[conn->iov_index].iov_base = resp_get_not_found;
        conn->iovs[conn->iov_index].iov_len = resp_get_not_found_len;
        conn->iov_index += 1; 
        if ( conn->iov_index > 128 ) {
          mr_writev( loop, conn->fd, conn->iovs, conn->iov_index );
          conn->iov_index = 0; 
        }
        */
        DBG_READ printf("getkey - not found\n");
      }

      data_left -= 4 + keylen;
      num_writes += 1;

    } else if ( cmd == SET ) {

      // TODO max val len = 1m - key and item sizes

      uint16_t keylen = *((uint16_t*)(p+2));
      uint32_t vlen = *((uint32_t*)(p+4));
      char *key = p+8;
      DBG printf("set key %d >%.*s<\n", keylen, keylen, key );
      char *val = p+8+keylen;
      DBG printf("set val %d >%.*s<\n", vlen, vlen, val );

      uint64_t blockAddr = blocks_alloc( sizeof(item) + vlen + keylen );
      DBG printf("block addr %ld\n", blockAddr);
      item *it = blocks_translate( blockAddr );
      DBG printf("item ptr %p\n", it);

      if ( COMPRESSION_ENABLED ) {
        int rc = ZSTD_compress( it->data, vlen, p+8+keylen, vlen, 3 );
        DBG printf(" compress rc %d\n", rc);
        if ( rc > 0 ) {
          it->size = vlen = rc;
        }
      } else {
        it->size = vlen;
        memcpy( it->data, p+8+keylen, vlen ); // Val
      }

      it->keysize = keylen;
      memcpy( it->data+vlen, p+8, keylen ); // Key goes after val

      // Store the number of bits so if on disk we can allocate a big enough buffer to hold it
      if ( settings.disk_size ) blockAddr |= num_bits64(vlen+8+keylen) << 20; // TODO 8 is the item size?... just do sizeof?

      unsigned long hv = CityHash64(key, keylen);      
      ht_insert( mrq_ht, blockAddr, key, keylen, hv );
  
      data_left -= 8 - keylen - vlen;
      p += 8 + keylen + vlen;
      num_writes += 1;

    } else { // Close connection? TODO
      data_left = 0;
    }
  } 


  //if ( conn->iov_index > 0 ) {
    //mr_writevf( loop, conn->fd, conn->iovs, conn->iov_index );
    //conn->iov_index = 0; 
  //}
  if ( conn->out_cur_sz > 0 ) conn_flush(conn);
/*
          if ( conn->out_cur_sz > 0 ) {
            printf("DELME writing out buffer at end of on data\n");
            ssize_t nwritten, totlen = 0, count = conn->out_cur_sz;
            char *b = conn->out_buf;
            while(totlen != count) {
              nwritten = write(conn->fd,b,count-totlen);
              if (nwritten == 0) { printf("nwritten 0\n"); exit(-1); }
              if (nwritten == -1) { printf("nwritten -1\n"); printf("%s\n",strerror(errno)); exit(-1); }
              totlen += nwritten;
              b += nwritten;
            }
          }
 */ 

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
          "    -m, --max-socket=<megabytes>  Maximum amount of memory in mb (default: 256)\n"
          "    -z, --zstd                    Enable zstd compression in memory\n"
          "\n"
        );
}

int main (int argc, char **argv) {

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
