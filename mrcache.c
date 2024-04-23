
#include <zstd.h>
#include <signal.h>
#include <getopt.h>
#include <stdarg.h>
#include <netinet/in.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <sys/mman.h>   // mmap

#include "liburing.h"

#include "common.h"
#include "mrcache.h"
#include "hashtable.h"
#include "blocks.h"
#include "wyhash.h"
#include "xxhash.h"

#define BUFFER_SIZE 32*1024
#define READ_BUF_SIZE 4096L // * 1024
#define NR_BUFS 16 * 1024

#define BR_MASK (NR_BUFS - 1)
#define BGID 1 

#define NEW_CLIENT 0xffffffffffffffff

//static mr_loop_t *loop = NULL;
static struct io_uring uring;
static struct io_uring_buf_ring* br;

hashtable_t *mrq_ht, *mrq_htnew;

#define NUM_IOVEC 1024
typedef struct _conn
{
  char *buf;
  int max_sz;
  int cur_sz;
  int needs;
  int fd;

  getq_item_t *getq_head, *getq_tail;
  bool stalled;

  struct iovec iovs[NUM_IOVEC];
  char free_me[NUM_IOVEC];
  int iov_start, iov_end, num_iov;
  int write_in_progress;
} my_conn_t;

struct sockaddr_in addr;

//static int total_clients = 0;  // Total number of connected clients
//static int total_mem = 0;
static int num_writes = 0;
//static int num_items = 0;

static char resp_get[2] = {0,1};
static char resp_get_not_found[4] = {0,0,0,0};
static char resp_get_not_found_len = 4;

static char* ring_buf;
static char *zstd_buffer;
static void setup() {
  mrq_ht = malloc( sizeof(hashtable_t) );
  ht_init(mrq_ht, config.index_size * 1024 * 1024);
  blocks_init(&config);

  srand((int)time(NULL));

  // TODO Only malloc if compressed cmds are used
  zstd_buffer = malloc( 16 * 1024 * 1024 + 128 );

}

static void tear_down() {

  // TODO ring buffers and ring
  io_uring_free_buf_ring(&uring, br, NR_BUFS, 1);
  io_uring_queue_exit(&uring);

  free(zstd_buffer);
  exit(-1); // TODO
}

static void print_buffer( char* b, int len ) {
  for ( int z = 0; z < len; z++ ) {
    printf( "%02x ",(int)b[z]);
  }
  printf("\n");
}

my_conn_t *conn_new(int fd ) {
  my_conn_t *c = calloc( 1, sizeof(my_conn_t));
  c->fd = fd;
  c->buf = malloc( BUFFER_SIZE*2 );
  c->max_sz = BUFFER_SIZE*2;
  memset(c->free_me, 0, NUM_IOVEC);
  return c;
}

void free_conn( my_conn_t *c ) {
  //mr_close( loop, c->fd );
  free(c->buf);
  // TODO free the output queue items?
  free(c);
}

// Append data to the connection buffer
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
  DBG printf("on_write_done - res %d conn %p\n", res, user_data);

  if ( res < 0 ) {
    printf(" err: %s\n",strerror(-res));
    exit(-1); // TODO
  }
  my_conn_t *conn = (my_conn_t*)user_data;
  conn->write_in_progress = 0;
  for ( int i = conn->iov_start; i < conn->iov_end; i++ ) {
    if ( conn->free_me[i] == 1 ) {
      free(conn->iovs[i].iov_base);
      conn->free_me[i] = 0;
    }
  }
  conn->iov_start = 0;
  conn->iov_end = 0;
}

void finishOnData( my_conn_t *conn ) {
  if ( conn->write_in_progress == 1 ) {
    printf("Got more cmds while writes are still in progress\n");
    exit(1);
  }
  if ( conn->iov_end > conn->iov_start ) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&uring);
    io_uring_prep_writev(sqe, conn->fd, &(conn->iovs[conn->iov_start]), conn->iov_end-conn->iov_start, 0);
    io_uring_sqe_set_data(sqe, conn);
    io_uring_submit(&uring);

    //mr_writevcb( loop, conn->fd, &(conn->iovs[conn->iov_start]), conn->iov_end-conn->iov_start , (void*)conn, on_write_done  );
    //mr_flush(loop);
    conn->write_in_progress = 1;
  }
}



int on_data(my_conn_t *conn, ssize_t nread, char *buf) {
  char *p = NULL;
  int data_left = nread;

  if ( nread == 0 ) { 
    free_conn(conn);
    return 1;
  }

  if ( nread < 0 ) { 
    free_conn(conn);
    return 1;
  }

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
      unsigned long hv = wyhash(key, keylen, 0, _wyp);
      //unsigned long hv = XXH3_64bits(key, keylen);

      item *it = NULL;
      int rc = ht_find(mrq_ht, key, keylen, hv, (void*)&it);
      DBG_READ printf("get ht_find rc %d\n", rc );

      config.tot_reads += 1;

      if ( rc == 1 && it ) { // Found

	      conn->iovs[conn->iov_end].iov_base = ((char*)it)+2;
	      conn->iovs[conn->iov_end].iov_len  = it->size+4;
	      conn->iov_end += 1;

        DBG_READ printf("getkey: found >%.*s<\n", it->size, it->data);
        DBG_READ print_buffer( it->data, it->size );

      } else {

	      conn->iovs[conn->iov_end].iov_base = resp_get_not_found;
	      conn->iovs[conn->iov_end].iov_len  = resp_get_not_found_len;
	      conn->iov_end += 1;

        config.misses += 1;
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
        return 0;
      }
      uint16_t keylen = *((uint16_t*)(p+2));
      int32_t vlen = *((int32_t*)(p+4));
      DBG_SET printf("set keylen %d vlen %d\n", keylen, vlen );

      if ( data_left < 8+keylen+vlen ) {
        conn_append( conn, p, data_left );
        conn->needs = 8+keylen+vlen;
        finishOnData(conn);
        return 0;
      }

      char *key = p+8;
      DBG_SET printf("set key %d >%.*s<\n", keylen, keylen, key );
      char *val = p+8+keylen;
      DBG_SET printf("set val %d >%.*s<\n", vlen, vlen, val );
      DBG_SET print_buffer( val, vlen );

      uint64_t blockAddr;
      item *it;

      config.tot_writes += 1;

      blockAddr = blocks_alloc( sizeof(item) + vlen + keylen );
      it = blocks_translate( blockAddr );

      DBG_SET printf(" set - it %p baddr %lx\n", it, blockAddr);
      it->size = vlen;
      memcpy( it->data, p+8+keylen, vlen ); // Val

      it->keysize = keylen;
      memcpy( it->data+vlen, p+8, keylen ); // Key goes after val

      data_left -= (8 + keylen + vlen);
      p += 8 + keylen + vlen;

      unsigned long hv = wyhash(key, keylen, 0, _wyp);
      //unsigned long hv = XXH3_64bits(key, keylen);
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
      unsigned long hv = wyhash(key, keylen, 0, _wyp);
      item *it = NULL;
      int rc = ht_find(mrq_ht, key, keylen, hv, (void*)&it);
      DBG_READ printf("get ht_find rc %d\n", rc );

      config.tot_reads += 1;

      if ( rc == 1 && it ) { // Found

        unsigned long long const decomp_sz = ZSTD_getFrameContentSize(it->data, it->size);
        if ( decomp_sz < 0 ) {  // This data wasn't compressed.  Return not found.
	        conn->iovs[conn->iov_end].iov_base = resp_get_not_found;
	        conn->iovs[conn->iov_end].iov_len  = resp_get_not_found_len;
	        conn->iov_end += 1;
        }

        char *dbuf = malloc( decomp_sz+4 );
        int decomp_size = ZSTD_decompress( dbuf+4, decomp_sz, it->data, it->size );
        uint32_t *p32 = (uint32_t*)(dbuf);
        *p32 = decomp_size;

	      conn->iovs[conn->iov_end].iov_base = dbuf;
	      conn->iovs[conn->iov_end].iov_len  = decomp_size+4;
	      conn->free_me[conn->iov_end] = 1;
	      conn->iov_end += 1;


      } else {

	      conn->iovs[conn->iov_end].iov_base = resp_get_not_found;
	      conn->iovs[conn->iov_end].iov_len  = resp_get_not_found_len;
	      conn->iov_end += 1;

        config.misses += 1;
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

      config.tot_writes += 1;

      int cmplen = ZSTD_compress( zstd_buffer, vlen+64, p+8+keylen, vlen, 2 ); // instead of 64 use ZSTD_COMPRESSBOUND?

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

        unsigned long hv = wyhash(key, keylen, 0, _wyp);
        ht_insert( mrq_ht, blockAddr, key, keylen, hv );

      } 

      num_writes += 1;

    } else if ( cmd == STAT ) {

      printf("STAT\n");
      printf("Total reads  %ld\n", config.tot_reads);
      printf("Total misses %ld\n", config.misses);
      printf("Total writes %ld\n", config.tot_writes);
      printf("Avg shift %.2f\n", (double)config.read_shifts/config.tot_reads);
      printf("Max shift %d\n", config.max_shift);
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
  if ( sig == SIGINT || sig == SIGTERM ) {
    tear_down();
    exit(EXIT_SUCCESS);
  }
}

static void usage(void) {
  printf( "Mrcache Version 0.1\n"
          "    -h, --help                    This help\n"
          "    -p, --port=<num>              TCP port to listen on (default: 7000)\n"
          "    -m, --max-memory=<mb>         Maximum amount of memory in mb (default: 256)\n"
          //"    -d, --max-disk=<gb>           Maximum amount of disk in gb (default: 1)\n"
          "    -i, --index-size=<mb>         Index size in mb (must be a power of 2 and sz/14 is the max number of items)\n"
          "\n"
        );
}

int main (int argc, char **argv) {

  signal(SIGPIPE, SIG_IGN);

  char *shortopts =
    "h"
    "m:"
    //"d:"
    "i:"
    "p:"
    ;
  const struct option longopts[] = {
    {"help",             no_argument, 0, 'h'},
    {"max-memory", required_argument, 0, 'm'},
    //{"max-disk",   required_argument, 0, 'd'},
    {"index-size", required_argument, 0, 'i'},
    {"port",       required_argument, 0, 'p'},
    {"zstd",             no_argument, 0, 'z'},
    {0, 0, 0, 0}
  };

  config.port = 7000;
  config.max_memory = 128;
  config.flags = 0;
  //config.disk_size = 0;
  config.index_size = 0;
  config.block_size = 16;
  config.max_connections = 128;

  config.read_shifts  = 0;
  config.tot_reads    = 0;
  config.tot_writes   = 0;
  config.write_shifts = 0;

  int optindex, c;
  while (-1 != (c = getopt_long(argc, argv, shortopts, longopts, &optindex))) {
    switch (c) {
    case 'p':
      config.port = atoi(optarg);
      break;
    case 'm':
      config.max_memory = atoi(optarg);
      break;
    //case 'd':
      //config.disk_size = atoi(optarg);
      //break;
    case 'i':
      config.index_size = atoi(optarg);
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

  if ( config.index_size == 0 ) {
    // By default the index size is 10% of memory rounded up to a power of 2
    config.index_size = (config.max_memory * 0.1);
    int power = 1;
    while(power < config.index_size) {
        power <<= 1;
    }
    config.index_size = power;
  }
  if ( !IS_POWER_OF_TWO(config.index_size) ) {
    printf("The index size must be a power of two\n\n");
    usage();
    return(2);
  }

  setup();

  double max_items = (double)(config.index_size>>3)*0.70; // Each entry is 8B max 70% full
  //if ( config.disk_size ) {
    //printf("Mrcache starting up on port %d with %dmb of memory and %dgb of disk allocated.  The max number of items is %0.1fm based on the index size of %dm\n", config.port, config.max_memory, config.disk_size, max_items, config.index_size );
  //} else {
    printf("Mrcache starting up on port %d with %dmb allocated. Maximum items is %0.1fm\n", config.port, config.max_memory+config.index_size, max_items );
  //}

  signal(SIGINT,  sig_handler);
  signal(SIGTERM, sig_handler);
  signal(SIGHUP,  sig_handler);

  // TODO Move this to a separate net.h
    int socket_options = 1;
    int socket_fd = -1;
    int ret;
    struct io_uring_params params;
    struct io_uring_sqe* sqe;
    struct io_uring_cqe* cqe;

    memset(&uring, 0, sizeof(uring));
    memset(&params, 0, sizeof(params));

    params.flags |= IORING_SETUP_SINGLE_ISSUER;
    params.features |= IORING_FEAT_FAST_POLL;
    params.features |= IORING_FEAT_SQPOLL_NONFIXED;
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = 1000; // TODO
    params.flags |= IORING_SETUP_CQSIZE | IORING_SETUP_CLAMP;
    params.cq_entries = 4096;

    ret = io_uring_queue_init_params(4096, &uring, &params);
    if (ret != 0) { printf("Error initializing uring: %s\n",strerror(ret)); goto cleanup; }

    // Setup the ring buffers 
    if (posix_memalign((void**)&ring_buf, 4096, READ_BUF_SIZE * NR_BUFS)) {
        perror("posix memalign");
        exit(1);
    }

    br = io_uring_setup_buf_ring(&uring, NR_BUFS, BGID, 0, &ret);
    if (!br) {
        fprintf(stderr, "Buffer ring register failed %d %s\n", ret, strerror(errno));
        return 1;
    }

    void* ptr = ring_buf;
    for (int i = 0; i < NR_BUFS; i++) {
        io_uring_buf_ring_add(br, ptr, READ_BUF_SIZE, i , BR_MASK, i);
        ptr += READ_BUF_SIZE;
    }
    io_uring_buf_ring_advance(br, NR_BUFS);

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    //address.sin_addr.s_addr = inet_addr(config.hostname);
    address.sin_port = htons(config.port); 

    if ((socket_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)) == -1) {
      printf("socket error : %d ...\n", errno);
      exit(EXIT_FAILURE);
    }

    if (bind(socket_fd, (struct sockaddr*)&address, sizeof(address)) < 0) { printf("Error binding to hostname\n"); goto cleanup; }
    if (listen(socket_fd, config.max_connections) < 0) goto cleanup;

    sqe = io_uring_get_sqe(&uring);
    io_uring_prep_multishot_accept(sqe, socket_fd, NULL, NULL, 0);//IORING_FILE_INDEX_ALLOC);
    io_uring_sqe_set_data64(sqe, NEW_CLIENT);
    io_uring_submit(&uring);

  // Event loop
  while(1) {
    struct io_uring_cqe* cqe;
    unsigned int head;
    unsigned int i = 0;

    io_uring_for_each_cqe(&uring, head, cqe) {
      if ( cqe->user_data == NEW_CLIENT ) {
        int client_fd = cqe->res;

        my_conn_t *conn = conn_new(client_fd);
    
        struct io_uring_sqe* sqe;
        sqe = io_uring_get_sqe(&uring);
        io_uring_prep_recv_multishot(sqe, client_fd, NULL, 0, 0);
        io_uring_sqe_set_data(sqe, (void*)conn);
        sqe->buf_group = BGID;
        sqe->flags |= IOSQE_BUFFER_SELECT;
        io_uring_submit(&uring);
    
      } else {
        my_conn_t *conn = (my_conn_t*)io_uring_cqe_get_data(cqe);

        // If this is a multishot recv event
        if (cqe->flags & IORING_CQE_F_MORE) {

          int bid = (cqe->flags >> IORING_CQE_BUFFER_SHIFT);
          void *p = ring_buf + bid * READ_BUF_SIZE;
          ret = on_data(conn, cqe->res, p);
          io_uring_buf_ring_add(br, p, READ_BUF_SIZE, bid, BR_MASK, 0);
          io_uring_buf_ring_advance(br, 1);

        } else { // write done

          if ( cqe->res <= 0 ) {
            //fprintf(stderr, "multishot cancelled for: %ull\n", conn->fd);
          } else {
            on_write_done(conn, cqe->res);
          }
        }
      }
      ++i;
    }

    //printf("Handled %d cqes\n",i);
    if (i) io_uring_cq_advance(&uring, i);

  }

  return 0;
cleanup:
  printf("Exiting due to setup failure");
  return 0;
}

/*
static void queue_cancel(struct io_uring *ring, struct conn *c)
{ 
  struct io_uring_sqe *sqe;
  int flags = 0;
    
  if (fixed_files)
    flags |= IORING_ASYNC_CANCEL_FD_FIXED;
  
  sqe = get_sqe(ring);
  io_uring_prep_cancel_fd(sqe, c->in_fd, flags);
  encode_userdata(sqe, c, __CANCEL, 0, c->in_fd);
  c->pending_cancels++;

  if (c->out_fd != -1) {
    sqe = get_sqe(ring);
    io_uring_prep_cancel_fd(sqe, c->out_fd, flags);
    encode_userdata(sqe, c, __CANCEL, 0, c->out_fd);
    c->pending_cancels++;
  }

  io_uring_submit(ring);
}
*/
