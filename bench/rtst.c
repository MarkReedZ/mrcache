
#include "mrloop.h"

#include <sys/time.h>

static struct timeval  tv1, tv2;

#define BUFSIZE 64*1024
#define NUM 1000
#define PIPE 64
static int vlen = 16;
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
  //printf("on_data >%.*s<\n", nread, buf);
  //for ( int x = 0; x < 50; x++ ) {
    //if ( buf[x] == 'b' ) { printf("YAY %d\n", x); exit(1); }
  //}
  //exit(-1);

  bytes += nread;
  //printf("bytes: %d %d\n", bytes,PIPE*vlen);
  if ( bytes >= (PIPE)*(vlen+5) ) {
    bytes = 0;
    reps += 1;
    if ( reps < NUM ) {
      //mr_writev( loop, fd, iovs, PIPE );
      //for( int i = 0; i < 100; i++ ) {
        //mr_writev( loop, fd, iovs, PIPE );
      //}
      int n = write( fd, obuf, olen );
    } else {
      gettimeofday(&tv2, NULL);
      double secs = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
      printf ("Total time = %f seconds cps %f\n", (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec), (PIPE*reps)/secs);
      exit(1);
    }
  }
  //struct iovec *iov = malloc( sizeof(struct iovec) );
  //iov->iov_base = buf;
  //iov->iov_len  = nread;
  //mr_writev( loop, ((conn_t*)conn)->fd, iov, 1 );

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
  //addTimer(loop, 10, on_timer);

  int l = vlen;
  char *buf = calloc(1024*1024, 1);
  struct iovec iov;

/*
  strcpy(buf, "SET test ");
  for (int x = 9; x < 9+l; x++ ) {
    buf[x] = 97;
  }
  buf[9+l] = 0;
*/
  sprintf(buf, "*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$%d\r\n", l);
  int cmdlen = strlen(buf);
  for (int x = cmdlen; x < cmdlen+l+2; x++ ) {
    buf[x] = 97;
  }
  buf[cmdlen+l+2] = '\r';
  buf[cmdlen+l+3] = '\n';
  //printf(" redis test len %d strlen %d >%s<\n ",l,strlen(buf), buf);
  //exit(1);
  //iov.iov_base = buf;
  //iov.iov_len = strlen(buf);
  //mr_writev( loop, fd, &iov, 1 );
  int n = write( fd, buf, strlen(buf) );

  //buf = "GET test\r\n";
  char *buf2 = malloc(256);
  strcpy(buf2, "*2\r\n$3\r\nGET\r\n$4\r\ntest\r\n");
  //strcpy(buf2, "GET test\r\n");
  //buf = "*2\r\n$3\r\nGET\r\n$4\r\ntest\r\n";
  //iov.iov_base = buf;
  //iov.iov_len = strlen(buf);
  //mr_writev( loop, fd, &iov, 1 );

  char *p = obuf;
  l = strlen(buf2);
  olen = 0;
  for( int i = 0; i < PIPE; i++ ) {
    memcpy( p, buf2, l );
    p += l;
    olen += l;
  }


 
  for( int i = 0; i < PIPE; i++ ) {
    iovs[i].iov_base = buf2;
    iovs[i].iov_len = strlen(buf2);
  }
  start_time = clock();
  gettimeofday(&tv1, NULL);

  n = write( fd, obuf, olen );

  //mr_writev( loop, fd, iovs, PIPE );
  //mr_writev( loop, fd, iovs, PIPE );
  //mr_flush(loop);

  //for( int i = 0; i < NUM; i++ ) {
  //}


  //struct iovec iov2;
  //iov2.iov_base = p;
  //iov2.iov_len = 10;
  //mr_writev( loop, fd, &iov2, 1 );

  mr_run(loop);
  mr_free(loop);

}
