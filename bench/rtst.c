
#include "mrloop.h"

#include <sys/time.h>

static struct timeval  tv1, tv2;

#define BUFSIZE 64*1024
#define NUM 1000
#define PIPE 64
static int bytes = 0;
static struct iovec iovs[PIPE];
static double start_time = 0;
static int reps = 0;

/*
static void print_buffer( char* b, int len ) {
  for ( int z = 0; z < len; z++ ) {
    printf( "%02x ",(int)b[z]);
    //printf( "%c",b[z]);
  }
  printf("\n");
}
*/

typedef struct _conn
{
  int fd;
  char buf[BUFSIZE];
} conn_t;

static mrLoop *loop = NULL;

void on_timer() { 
  printf("tick\n");
}

void *setup_conn(int fd, char **buf, int *buflen ) {
  //printf("New Connection\n");
  conn_t *conn = calloc( 1, sizeof(conn_t) );
  conn->fd = fd;
  *buf = conn->buf;
  *buflen = BUFSIZE;
  return conn;
}

void on_data(void *conn, int fd, ssize_t nread, char *buf) {
  //printf("on_data >%.*s<\n", nread > 128 ? 128 : nread, buf);
  //exit(-1);
  bytes += nread;
  //printf("bytes: %d\n", bytes);
  if ( bytes >= PIPE*11*100 ) {
    bytes = 0;
    reps += 1;
    if ( reps < NUM ) {
      //mrWritev( loop, fd, iovs, PIPE );
      for( int i = 0; i < 100; i++ ) {
        mrWritev( loop, fd, iovs, PIPE );
      }
    } else {
      gettimeofday(&tv2, NULL);
      double secs = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
      printf ("Total time = %f seconds cps %f\n", (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec), (PIPE*reps*100)/secs);
      exit(1);
    }
  }
  //struct iovec *iov = malloc( sizeof(struct iovec) );
  //iov->iov_base = buf;
  //iov->iov_len  = nread;
  //mrWritev( loop, ((conn_t*)conn)->fd, iov, 1 );

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
  
  loop = createLoop(sig_handler);
  int fd = mrConnect(loop,"localhost", 6379, on_data);
  //addTimer(loop, 10, on_timer);

  char *buf;
  struct iovec iov;
  buf = "SET test bench\r\n";
  iov.iov_base = buf;
  iov.iov_len = strlen(buf);
  mrWritev( loop, fd, &iov, 1 );

  //buf = "GET test\r\n";
  buf = "*2\r\n$3\r\nget\r\n$4\r\ntest\r\n";
  //iov.iov_base = buf;
  //iov.iov_len = strlen(buf);
  //mrWritev( loop, fd, &iov, 1 );

 
  for( int i = 0; i < PIPE; i++ ) {
    iovs[i].iov_base = buf;
    iovs[i].iov_len = strlen(buf);
  }
  start_time = clock();
  gettimeofday(&tv1, NULL);
  //mrWritevf( loop, fd, iovs, PIPE );
  for( int i = 0; i < 100; i++ ) {
    mrWritev( loop, fd, iovs, PIPE );
  }
  mrFlush(loop);

  //for( int i = 0; i < NUM; i++ ) {
  //}


  //struct iovec iov2;
  //iov2.iov_base = p;
  //iov2.iov_len = 10;
  //mrWritev( loop, fd, &iov2, 1 );

  runLoop(loop);
  freeLoop(loop);

}
