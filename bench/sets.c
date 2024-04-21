
#include "mrloop.h"

#include <sys/time.h>
static struct timeval  tv1, tv2;


#define BUFSIZE 64*1024
#define NUM 1000
#define PIPE 64
static int bytes = 0;
static struct iovec iovs[PIPE];
static int reps = 0;


static void print_buffer( char* b, int len ) {
  for ( int z = 0; z < len; z++ ) {
    printf( "%02x ",(int)b[z]);
    //if ( (int)b[z]  == 1 && (int)b[z-1] == 0 ) printf("\n");
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

void on_write_done( void *buf ) {
  printf("DELME write done\n");
}

static int leftover = 0;
static int itemcnt = 0;
static int last_sz = 0;

int on_data(void *conn, int fd, ssize_t nread, char *buf) {
  printf("on_data\n"); // fd %d >%.*s<\n", ((conn_t*)conn)->fd, nread, buf);
  //print_buffer( buf, nread );
}

void sig_handler(const int sig) {
  printf("Signal handled: %s.\n", strsignal(sig));
  printf(" Num bytes %d\n", bytes);
  exit(EXIT_SUCCESS);
}


static int num;
static int fd;

int setTest( void *user_data) {

  char buf[4096];
  char key[2048];
  char *val = "some_value";
  char *p = buf;
  for ( int i = 0; i < 100; i++ ) {
    sprintf( key, "testing%d", num++ );  
    p[0] = 0;
    p[1] = 2;
    uint16_t *p16 = (uint16_t*)(p+2);
    *p16 = strlen(key);
    unsigned int *lenptr = (unsigned int*)(p+4);
    *lenptr = strlen(val);
    p += 8;
    strcpy( p, key );
    strcpy( p + strlen(key), val );
    p += strlen(key);
    p += strlen(val);
  }
  
  struct iovec iov;
  iov.iov_base = buf;
  iov.iov_len = p - buf;
  mr_writev( loop, fd, &iov, 1 );

  if ( num % 100000 == 0 ) {
    printf(" num %d fd %d\n", num, fd );
  }

  if ( num > 50 * 1000 * 1000 ) {
    exit(0);
  }

  mr_call_after( loop, setTest, 1, NULL );
  
}


int main() {

  //fork();
  //fork();

  loop = mr_create_loop(sig_handler);
  fd = mr_connect(loop,"localhost", 7000, on_data);
  printf("fd = %d\n", fd);
  //addTimer(loop, 10, on_timer);

  mr_call_after( loop, setTest, 1, NULL );

  mr_run(loop);
  mr_free(loop);

}
