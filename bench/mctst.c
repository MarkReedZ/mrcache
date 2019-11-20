
#include "mrloop.h"

#include <sys/time.h>
static struct timeval  tv1, tv2;

#define BUFSIZE 64*1024
#define NUM 1000
#define PIPE 64
static int bytes = 0;
static struct iovec iovs[128];
static double start_time = 0;
static int reps = 0;
static int vlen = 100;

static void print_buffer( char* b, int len ) {
  for ( int z = 0; z < len; z++ ) {
    printf( "%02x ",(int)b[z]);
    //printf( "%c",b[z]);
  }
  printf("\n");
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
  conn_t *conn = calloc( 1, sizeof(conn_t) );
  conn->fd = fd;
  *buf = conn->buf;
  *buflen = BUFSIZE;
  return conn;
}

void on_data(void *conn, int fd, ssize_t nread, char *buf) {
  //printf("on_data >%.*s<\n", nread, buf);
  bytes += nread;
  //printf("bytes: %d\n", bytes);
  //printf("bytes: %d\n", PIPE*32);
  if ( bytes >= PIPE*(22+vlen) ) {
    bytes = 0;
    reps += 1;
    if ( reps < NUM ) {
      //mr_writev( loop, fd, iovs, PIPE );
      //for( int i = 0; i < 100; i++ ) {
        mr_writev( loop, fd, iovs, PIPE );
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
  int fd = mr_connect(loop,"localhost", 11211, on_data);

  //char buf[256];
  char *buf;
  struct iovec iov;

  buf = "get test\r\n";

  for( int i = 0; i < PIPE; i++ ) {
    iovs[i].iov_base = buf;
    iovs[i].iov_len = strlen(buf);
  }
  start_time = clock();
  gettimeofday(&tv1, NULL);
  mr_writev( loop, fd, iovs, PIPE );
  mr_flush(loop);


  mr_run(loop);
  mr_free(loop);

}
