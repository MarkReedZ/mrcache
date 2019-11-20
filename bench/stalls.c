
#include "mrloop.h"

#include <sys/time.h>
static struct timeval  tv1, tv2;


#define BUFSIZE 64*1024
#define NUM 100
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
void on_data(void *conn, int fd, ssize_t nread, char *buf) { }

void sig_handler(const int sig) {
  printf("Signal handled: %s.\n", strsignal(sig));
  printf(" Num bytes %d\n", bytes);
  exit(EXIT_SUCCESS);
}

static char foo[2048] = "Like any other social media site Facebook has length requirements when it comes to writing on the wall, providing status, messaging and commenting. Understanding how many characters you can use, enables you to more effectively use Facebook as a business or campaign tool. Private messaging is one of the main ways that people interact on Facebook. This type of direct messaging can be either an instant message (chat), or a regular email-type message. For both instant and regular messaging, there is a 20,000 character limit. A Facebook status may have character limits, but considering that it is at 63,206 characters, unless you are writing War and Peace, you should be fine. Facebook has raised this number 12 times to accommodate user’s status and feedback. Facebook wall posts have a 5000 character limit, but truncation begins at 477 characters. This enables someone to write a thoughtful response or create something similar to a blog.";

static int fd;

void write_something() {
  printf("wrote\n");
  char buf[4096];
  char *p = buf;
  p[0] = 0;
  p[1] = 1;
  uint16_t *p16 = (uint16_t*)(p+2);
  *p16 = 4;
  strcpy( p + 4, "test" );
  
  for( int i = 0; i < PIPE; i++ ) {
    iovs[i].iov_base = p;
    iovs[i].iov_len = 8;
  }
  
  for( int i = 0; i < 100; i++ ) {
    mr_writev( loop, fd, iovs, PIPE );
  }
  mr_flush(loop);

}

void longTest(int fd) {

  char buf[4096];
  char *p = buf;
  p[0] = 0;
  p[1] = 2;
  uint16_t *p16 = (uint16_t*)(p+2);
  *p16 = 4;
  unsigned int *lenptr = (unsigned int*)(p+4);
  *lenptr = strlen(foo);
  strcpy( p + 8, "test" );
  strcpy( p + 12, foo );

  struct iovec iov;
  iov.iov_base = p;
  iov.iov_len = 12 + strlen(foo);
  mr_writevf( loop, fd, &iov, 1 );

}

int main() {

  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);
  
  loop = mr_create_loop(sig_handler);
  fd = mr_connect(loop,"localhost", 7000, on_data);
  loop->readEvents[fd] = calloc( 1, sizeof(event_t) ); // Clear the read event so we stall

  mr_add_timer(loop, 0.5, write_something);

  longTest(fd);

  mr_run(loop);
  mr_free(loop);

}
