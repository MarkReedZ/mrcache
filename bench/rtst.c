

#include "mrloop.h"

#include <sys/time.h>

static struct timeval  tv1, tv2;

#define BUFSIZE 64*1024
#define NUM 10000
#define PIPE 1
static int vlen = 10000;
static int bytes = 0;
static struct iovec iovs[PIPE];
static double start_time = 0;
static int reps = 0;
static char obuf[2*1024*1024];
static int olen = 0;


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

void *setup_conn(int fd, char **buf, int *buflen ) {
  //printf("New Connection\n");
  conn_t *conn = calloc( 1, sizeof(conn_t) );
  conn->fd = fd;
  *buf = conn->buf;
  *buflen = BUFSIZE;
  return conn;
}

void on_data(void *conn, int fd, ssize_t nread, char *buf) {

  bytes += nread;
  //printf("bytes: %d %d\n", bytes,PIPE*vlen);
  if ( bytes >= (PIPE)*(vlen+5) ) {
    bytes = 0;
    reps += 1;
    if ( reps < NUM ) {
      int n = write( fd, obuf, olen );
    } else {
      gettimeofday(&tv2, NULL);
      double secs = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
      printf ("Total time = %f seconds cps %f\n", (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec), (PIPE*reps)/secs);
      exit(1);
    }
  }

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
  int fd = mr_connect(loop,"localhost", 6379, on_data);

  int l = vlen;
  char *buf = calloc(1024*1024, 1);
  struct iovec iov;

  // Set our key
  sprintf(buf, "*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$%d\r\n", l);
  int cmdlen = strlen(buf);
  for (int x = cmdlen; x < cmdlen+l+2; x++ ) {
    buf[x] = 97;
  }
  buf[cmdlen+l+2] = '\r';
  buf[cmdlen+l+3] = '\n';
  int n = write( fd, buf, strlen(buf) );

  // Create a buffer doing PIPE number of gets
  char *buf2 = malloc(256);
  strcpy(buf2, "*2\r\n$3\r\nGET\r\n$4\r\ntest\r\n");

  char *p = obuf;
  l = strlen(buf2);
  olen = 0;
  for( int i = 0; i < PIPE; i++ ) {
    memcpy( p, buf2, l );
    p += l;
    olen += l;
  }
 
  start_time = clock();
  gettimeofday(&tv1, NULL);
  n = write( fd, obuf, olen );

  mr_run(loop);
  mr_free(loop);

}
