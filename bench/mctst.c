
#include "mrloop.h"

#define BUFSIZE 64*1024
#define NUM 1000
#define PIPE 64
static int bytes = 0;
static struct iovec iovs[128];
static double start_time = 0;


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
  //printf("on_data >%.*s<\n", nread, buf);
  bytes += nread;
  //printf("bytes: %d\n", bytes);
  if ( bytes == NUM*PIPE*28 ) {
    double taken = ((double)(clock()-start_time))/CLOCKS_PER_SEC;
    printf( " %d gets time taken %f cps %f \n ", NUM*100, taken, (NUM*128)/taken );
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
  int fd = mrConnect(loop,"localhost", 11211, on_data);
  //addTimer(loop, 10, on_timer);

  //char buf[256];
  char *buf;
  struct iovec iov;
  //buf = "set test 0 0 5\r\nbench\r\n";
  //iov.iov_base = buf;
  //iov.iov_len = strlen(buf);
  //mrWritevf( loop, fd, &iov, 1 );

  buf = "get test\r\n";
  //iov.iov_base = buf;
  //iov.iov_len = strlen(buf);
  //mrWritev( loop, fd, &iov, 1 );

  for( int i = 0; i < PIPE; i++ ) {
    iovs[i].iov_base = buf;
    iovs[i].iov_len = strlen(buf);
  }
  start_time = clock();
  for( int i = 0; i < NUM; i++ ) {
    mrWritev( loop, fd, iovs, 100 );
  }

  //struct iovec iov2;
  //iov2.iov_base = p;
  //iov2.iov_len = 10;
  //mrWritev( loop, fd, &iov2, 1 );

  runLoop(loop);
  freeLoop(loop);

}
