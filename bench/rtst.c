
#include "mrloop.h"

#include <sys/time.h>
static struct timeval  tv1, tv2;

#define BUFSIZE 64*1024
#define NUM 10000
#define PIPE 256
static int bytes = 0;
static struct iovec iovs[1024];
static double start_time = 0;
static int reps = 0;
static int vlen = 16;
static int wcnt = 0;

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

int on_data(void *conn, int fd, ssize_t nread, char *buf) {
  //printf("on_data >%.*s<\n", 8, buf+28);
  //print_buffer(buf, nread); 
  //printf("nread=%d\n",nread);
  //exit(-1);
  bytes += nread;
  if ( bytes >= PIPE*(6+vlen) ) {
    bytes = 0;
    reps += 1;
    if ( reps < NUM ) {
      mr_writev( loop, fd, iovs, PIPE );
      mr_flush(loop);
    } else {
      gettimeofday(&tv2, NULL);
      double secs = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
      printf ("Total time = %f seconds cps %f\n", (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec), (PIPE*reps)/secs);
      exit(1);
    }
  }

  return 0;
}

void sig_handler(const int sig) {
  printf("Signal handled: %s.\n", strsignal(sig));
  printf(" Num bytes %d\n", bytes);
  exit(EXIT_SUCCESS);
}


int main() {

  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);
  
  loop = mr_create_loop(sig_handler);
  int fd = mr_connect(loop,"localhost", 6379, on_data);

  char buf[256], *p;
  struct iovec iov;

  memset(buf, 0, 256);
  strcpy(buf, "*2\r\n$3\r\nGET\r\n$4\r\ntest\r\n");

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
