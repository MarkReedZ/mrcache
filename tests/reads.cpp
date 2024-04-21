
#include "mrloop.h"

#include <sys/time.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "rand_utils.h"

static struct timeval  tv1, tv2;

#define PIPE 128
static struct iovec iovs[PIPE];
static double start_time = 0;
static const int nkeys = 8196;
static char getbufs[nkeys][2048];
static char setbufs[nkeys][8196];
static int glens[nkeys];
static int slens[nkeys];
static int next = 0;
static std::unordered_map<int, std::string> indexValMap;
static std::deque<int> expectIndices;

static void print_buffer( char* b, int len ) {
  for ( int z = 0; z < len; z++ ) {
    printf( "%02x ",(int)b[z]);
    //printf( "%c",b[z]);
  }
  printf("\n");
}

void on_write_done(void *user_data) {
  printf("on_write_done - %ld\n", (unsigned long)user_data);
}

typedef struct _conn
{
  int fd;
  char buf[BUFSIZE];
} conn_t;

static mr_loop_t *loop = NULL;

void on_timer() { 
  printf("tick\n");
}

void *setup_conn(int fd, char **buf, int *buflen ) {
  //printf("New Connection\n");
  conn_t *conn = (conn_t*)calloc( 1, sizeof(conn_t) );
  conn->fd = fd;
  *buf = conn->buf;
  *buflen = BUFSIZE;
  return conn;
}

void fillIovs() {
  for( int i = 0; i < PIPE; i++ ) {

    if ( (rand_utils.rand64() & 1) && i != 2 ) { // != 2 so we always do 1 get
      iovs[next].iov_base = setbufs[next];
      iovs[next].iov_len  = slens[next];
    } else {
      iovs[next].iov_base = getbufs[next];
      iovs[next].iov_len  = glens[next];
      expectIndices.push_back(next); 
    }
    next += 1; if ( next >= nkeys ) next = 0;
  }
}

int on_data(void *conn, int fd, ssize_t nread, char *buf) {
  //printf("on_data >%.*s<\n", 8, buf+28);
  //print_buffer(buf, nread); 
  //exit(-1);
  bytes += nread;
  //printf("nread %d\n", nread);
  //printf("bytes: %d of %d\n", bytes, PIPE*14);
  //printf("bytes: %d of %d", bytes, PIPE*(22+vlen));
  //if ( bytes >= PIPE*(22+vlen) ) {
  if ( bytes >= PIPE*(4+vlen) ) {
    bytes = 0;
    reps += 1;
    //printf("rep %d\n", reps);
    if ( reps < NUM ) {
      //mr_writev( loop, fd, iovs, PIPE );
      //for( int i = 0; i < 100; i++ ) {
      mr_writev( loop, fd, iovs, PIPE );
      //mr_writevcb( loop, fd, iovs, PIPE, (void*)reps, on_write_done  );
      //printf("Writing again\n");
      mr_flush(loop);
      //}
    } else {
      gettimeofday(&tv2, NULL);
      double secs = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
      printf ("Total time = %f seconds cps %f\n", (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec), (PIPE*reps)/secs);
      exit(1);
    //double taken = ((double)(clock()-start_time))/CLOCKS_PER_SEC;
    //printf( " %d gets time taken %f cps %f \n ", NUM*100, taken, (NUM*128)/taken );
    }
  }

  //struct iovec *iov = malloc( sizeof(struct iovec) );
  //iov->iov_base = buf;
  //iov->iov_len  = nread;
  //mrWritev( loop, ((conn_t*)conn)->fd, iov, 1 );
  return 0;
}

void sig_handler(const int sig) {
  printf("Signal handled: %s.\n", strsignal(sig));
  printf(" Num bytes %d\n", bytes);
  exit(EXIT_SUCCESS);
}


int main() {

  //fork();
  //fork();

  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);
  
  loop = mr_create_loop(sig_handler);
  int fd = mr_connect(loop,"localhost", 7000, on_data);

  

  for ( int i = 0; i < nkeys; i++ ) {
    rand_utils::rand_bytes( buf, kl );
    char *p = getbufs[i];
    uint64_t kl = rand_utils::rand64() & 0x3FF;
    p[0] = 0; p[1] = 1; uint16_t *keylen = (uint16_t*)(p+2); *keylen = kl;
    rand_utils::rand_bytes( p+4, kl );
    glens[i] = kl + 4;

    uint64_t l = rand_utils::rand64() & 0xFFF;
    p = setbufs[i];
    p[0] = 0; p[1] = 2; 
    keylen = (uint16_t*)(p+2); *keylen = kl;
    uint32_t *vlen = (uint16_t*)(p+4); *vlen = l;
    strncpy( p+8, getbufs[i]+4, kl );
    rand_utils::rand_bytes( p+8+kl, l );
    slens[i] = l + kl + 8;
    indexValMap[ i ] = std_string( p+8+kl, l );
  }

  fillIovs();

  start_time = clock();
  gettimeofday(&tv1, NULL);
  mr_writev( loop, fd, iovs, PIPE );
  //mr_writevcb( loop, fd, iovs, PIPE, (void*)reps, on_write_done  );
  mr_flush(loop);


  mr_run(loop);
  mr_free(loop);

}
