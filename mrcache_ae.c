

#include <zstd.h>
#include <signal.h>
#include <getopt.h>
#include <stdarg.h>
#include <netinet/in.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>

#include "ae/ae.h"
#include "ae/anet.h"
#include "common.h"
#include "mrcache_ae.h"
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

aeEventLoop *loop;

struct settings settings;

hashtable_t *mrq_ht;

typedef struct _conn
{
  char *buf, *recv_buf, *out_buf;
  int out_max_sz, out_cur_sz;
  int max_sz;
  int cur_sz;
  int needs;
  int fd;

  int write_in_progress;
} my_conn_t;

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

static void setup() {
  mrq_ht = malloc( sizeof(hashtable_t) );
  ht_init(mrq_ht);
  blocks_init();
}

static void print_buffer( char* b, int len ) {
  for ( int z = 0; z < len; z++ ) {
    printf( "%02x ",(int)b[z]);
    if ( (int)b[z]  == 1 && (int)b[z-1] == 0 ) printf("\n");
    //printf( "%c",b[z]);
  }
  printf("\n");
}

void write_cb(struct aeEventLoop* loop, int fd, void* clientdata, int mask)
{
/*
  my_conn_t *c = (my_conn_t*)clientdata;
  DBG printf("DELME write_cb conn %p\n",c);

  int n = anetWrite(fd, s->read + s->q_wrote, s->q_sz);
  rsz += s->q_sz;
  DBG printf(" write_cb anetWrite %d\n",n);
  if (n == -1) {
    printf("errno %s\n", strerror(errno));
    if ( errno != EAGAIN ) {
      printf("Exiting as errno is not EAGAIN\n");
      print_stats();
      exit(1);
    }
  }
  if ( n == s->q_sz ) {
    c->write_in_progress = 0;
    aeDeleteFileEvent(loop, fd, AE_WRITABLE);
  } else {
    if ( n < 0 ) n = 0;
    s->q_wrote += n; 
    s->q_sz -= n;
  }
*/
  
}

static void conn_init( my_conn_t *c, int fd ) {
  c->buf = malloc( BUFFER_SIZE*2 );
  c->cur_sz = 0;
  c->max_sz = BUFFER_SIZE;
  c->needs = 0;
  c->fd = fd;
  c->write_in_progress = 0;
}

static void conn_destroy( my_conn_t *c) {
  
  aeDeleteFileEvent(loop, c->fd, AE_READABLE);
  free(c->buf);
  shutdown(c->fd,SHUT_RDWR);
  close(c->fd); 
  
}

static char obuf[32*1024];
static char *op = obuf;
static int osz = 0;

void conn_write( my_conn_t *c, char *p, int sz) 
{

  memcpy( op+osz, p, sz );
  osz += sz;

  if ( osz > 16*1024 ) {
    //print_buffer( obuf, osz ); printf( "DELME osz %d\n", osz );
    while ( osz ) {
      int n = anetWrite(c->fd, op, osz);
      //printf( "DELME osz %d nwritten %d\n", osz, n );
      if (n==-1){
        printf("errno %s\n", strerror(errno));
        exit(1);
      }
      op += n;
      osz -= n;
    }
    osz = 0;
    op = obuf;
  
    //int n = anetWrite(c->fd, p, sz);
  }
  //if ( c->write_in_progress ) return; // queue dont just return
  //printf(" DELME write sz %d > ", sz); print_buffer( p, (sz>32) ? 32 : sz  );
  //printf(" DELME write rc %d\n", n);
/*
  // If we didn't write it all setup a callback when the socket write buffer is ready again
  if ( n < sz ) {
    printf("DELME socket full\n");
    s->q_wrote = n;
    s->queued = 1;
    s->q_sz = sz-n;
    c->qslot = s;
    //c->slots[c->stail] = s;
    //c->stail = (c->stail+1)%512;
    c->write_in_progress = 1;
    aeCreateFileEvent(loop, c->fd, AE_WRITABLE, write_cb, c);
    return;
  }
*/
}


static void conn_append( my_conn_t* c, char *data, int len ) {
  DBG printf(" append cur %d \n", c->cur_sz);
  if ( (c->cur_sz + len) > c->max_sz ) {
    while ( (c->cur_sz + len) > c->max_sz ) c->max_sz <<= 1;
    c->buf = realloc( c->buf, c->max_sz );
  }
  memcpy( c->buf + c->cur_sz, data, len ); 
  c->cur_sz += len;
  DBG printf(" append cur now %d \n", c->cur_sz);
  //DBG print_buffer( c->buf, c->cur_sz );
}


void on_data(my_conn_t *conn, ssize_t nread, char *buf) 
{
  char *p = NULL;
  int data_left = nread;

  //printf( " %d writes\n", num_writes);

  //printf("on_data\n");
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
      printf("Received partial data %d more bytes need %d\n",data_left, conn->needs);
      //exit(-1);
      return;
    }

    int cmd   = (unsigned char)p[1];

    if ( cmd == GET ) {

      if ( data_left < 4 ) {
        conn_append( conn, p, data_left );
        conn->needs = 4;
        //finishOnData(conn);
        DBG printf(" Not enough for a get left %d ", data_left );
        DBG print_buffer( p, data_left );
        return;
      }
      uint16_t keylen = *((uint16_t*)(p+2));
      char *key = p+4;

      //DBG printf("get key >%.*s<\n", keylen, key );
      unsigned long hv = CityHash64(key, keylen);
      item *it = ht_find(mrq_ht, key, keylen, hv);

      if ( it ) { // Found

        if ( COMPRESSION_ENABLED ) {

    //memcpy( op+osz, p, sz );
    //osz += sz;
          unsigned long long const decomp_sz = ZSTD_getFrameContentSize(it->data, it->size);
          //RESIZE_OUT_BUFFER_IF_NECESSARY( conn, decomp_sz+6 ) // 2B cmd + 4B len + data
          int decomp_size = ZSTD_decompress( op+osz+6, decomp_sz, it->data, it->size );
          DBG printf("getkey: decompressed >%.*s<\n", decomp_size, conn->out_buf+6+conn->out_cur_sz);
          char *ob = op+osz;
          ob[0] = 0; ob[1] = 1;
          uint32_t *p32 = (uint32_t*)(ob+2);
          *p32 = decomp_size;
          osz += 6 + decomp_size;

          if ( osz > 16*1024 ) {
            //printf( "DELME osz %d\n", osz );
            int n = anetWrite(conn->fd, op, osz);
            osz = 0;
            
            //int n = anetWrite(c->fd, p, sz);
            if (n==-1){
              printf("errno %s\n", strerror(errno));
              exit(1);
            }
          }
          
        } else { // No compression

          conn_write( conn, resp_get, 2 );
          conn_write( conn, ((char*)it)+2, it->size+4 );
          //DBG printf("getkey: found >%.*s<\n", it->size, it->data);

        }

      } else {
        conn_write( conn, resp_get_not_found, resp_get_not_found_len );
        DBG printf("get - not found\n");
      }

      data_left -= 4 + keylen;
      num_writes += 1;

    } else if ( cmd == SET ) {

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

      unsigned long hv = CityHash64(key, keylen);
      ht_insert( mrq_ht, blockAddr, key, keylen, hv );

      data_left -= 8 - keylen - vlen;
      p += 8 + keylen + vlen;
      num_writes += 1;

    } else {
      data_left = 0;
    }
  }

  //printf("DELME flushing now. At num %d osz %d\n",num_writes, osz);
  // Flush the output buffer
  if ( osz > 0 ) {
    //print_buffer( obuf, osz ); 
    //printf( "DELME osz %d\n",osz);
    while ( osz ) {
      int n = anetWrite(conn->fd, op, osz);
      if (n==-1){
        printf("errno %s\n", strerror(errno));
        exit(1);
      }
      op += n;
      osz -= n;
    }
    osz = 0;
    op = obuf;
  
  }
  return;
}

char recv_buffer[BUFFER_SIZE];

void on_read(aeEventLoop *loop, int fd, void *clientdata, int mask)
{
  my_conn_t* conn = (my_conn_t*)clientdata;
  int nread;
  nread = read(fd, recv_buffer, BUFFER_SIZE);
  if (nread == -1) {
    if (errno == EAGAIN) return;
    else {
      printf("Unexpected errno: %s\n", strerror(errno));
      conn_destroy(conn);
      free(conn);
      return;
    }
  }
  if ( nread == 0 ) {
    //printf("Connection closed by client\n");
    conn_destroy(conn);
    free(conn);
    return;
  }
  on_data( conn, nread, recv_buffer );
}

void on_accept(aeEventLoop *loop, int fd, void *clientdata, int mask)
{
  int client_port, client_fd;
  char client_ip[128];
  // create client socket
  client_fd = anetTcpAccept(NULL, fd, client_ip, 128, &client_port);
  //printf("Accepted %s:%d\n", client_ip, client_port);

  anetEnableTcpNoDelay(NULL, client_fd);

  // set client socket non-block
  anetNonBlock(NULL, client_fd);

  my_conn_t *conn = (my_conn_t*) malloc(sizeof(my_conn_t));
  conn_init(conn, client_fd); 

  // Read callback
  int ret;
  ret = aeCreateFileEvent(loop, client_fd, AE_READABLE, on_read, (void*)conn);
  assert(ret != AE_ERR);
}


static void sig_handler(const int sig) {
  printf("Signal handled: %s.\n", strsignal(sig));
  //print_stats();
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

  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);

  char *shortopts =
    "h"
    "m:"
    "p:"
    "z"
    ;
  const struct option longopts[] = {
    {"help",             no_argument, 0, 'h'},
    {"max-memory", required_argument, 0, 'm'},
    {"port",       required_argument, 0, 'p'},
    {"zstd",             no_argument, 0, 'z'},
    {0, 0, 0, 0}
  };

  settings.port = 7000;
  settings.max_memory = 16;

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

  settings.max_memory *= 1024*1024;

  int ipfd;
  ipfd = anetTcpServer(NULL, settings.port, "0.0.0.0", 0);
  assert(ipfd != ANET_ERR);

  loop = aeCreateEventLoop(1024);

  int ret;
  ret = aeCreateFileEvent(loop, ipfd, AE_READABLE, on_accept, NULL);
  assert(ret != AE_ERR);

  aeMain(loop);

  aeDeleteEventLoop(loop);

  //int enable = 1;
  //setsockopt(sd, SOL_SOCKET, SO_OOBINLINE, &enable, sizeof(enable));
  
  return 0;
}
