
#include "mrloop.h"

#include <sys/time.h>
static struct timeval  tv1, tv2;


#define BUFSIZE 64*1024
#define NUM 1000
#define PIPE 1
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

void on_write_done( void *buf ) {
  printf("DELME write done\n");
}

static int leftover = 0;
static int itemcnt = 0;
static int last_sz = 0;
void on_data(void *conn, int fd, ssize_t nread, char *buf) {
  //printf("on_data+6 >%.*s<\n", nread-6, buf+6);
  //printf("on_data %d >%.*s<\n", nread, nread > 128 ? 128 : nread, buf);
  //printf("on_data sz %d >%.*s<\n", nread, nread-6, buf+6);
  //exit(1);
  //print_buffer( buf, nread ); printf("\n");
  //printf("on_data sz %d\n", nread );

/*
  int left = nread;
  char *p = buf;
  if ( leftover ) {
    left -= (last_sz+6 - leftover); 
    p += (last_sz+6 - leftover); 
    itemcnt += 1;
    //printf(" DELME lefotver %d\n", leftover);
    leftover = 0;
    //exit(-1);
  }

  while ( left ) {
    //printf( " itemcnt %d buf ", itemcnt); print_buffer( p, 8 ); //printf("\n");
    if ( left < 6 ) { leftover = left; return; }
    if ( p[0] != 0 && p[1] != 1 ) {
      printf(" bad response\n");
      exit(-1);
    }
    p += 2;
    unsigned int  sz  = *((unsigned int*)(p));
    if ( sz != 945 ) {
      printf(" argh sz is %d\n", sz );
      exit(-1);
    }
    last_sz = sz;
    if ( left < (sz+6) ) { leftover = left; return; }
    p += sz+4;
    itemcnt += 1;
    left -= sz+6;
  }
*/ 
  bytes += nread;
  //printf("bytes %d vs %d\n",bytes, PIPE*6*100);
  //if ( bytes >= PIPE*6*100 ) {
  //if ( bytes >= PIPE*10*100 ) {
  if ( bytes >= PIPE*951*100 ) {
  //if ( itemcnt >= PIPE*100 ) {
    itemcnt = 0;
    //printf("bytes %d\n",bytes);
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

  //printf("on_data\n"); // fd %d >%.*s<\n", ((conn_t*)conn)->fd, nread, buf);
  //print_buffer( buf, nread );
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


void longTest(int fd) {

  char foo[2048] = "Like any other social media site Facebook has length requirements when it comes to writing on the wall, providing status, messaging and commenting. Understanding how many characters you can use, enables you to more effectively use Facebook as a business or campaign tool. Private messaging is one of the main ways that people interact on Facebook. This type of direct messaging can be either an instant message (chat), or a regular email-type message. For both instant and regular messaging, there is a 20,000 character limit. A Facebook status may have character limits, but considering that it is at 63,206 characters, unless you are writing War and Peace, you should be fine. Facebook has raised this number 12 times to accommodate user’s status and feedback. Facebook wall posts have a 5000 character limit, but truncation begins at 477 characters. This enables someone to write a thoughtful response or create something similar to a blog.";
  char buf[4096];
  char *p = buf;
  p[0] = 0;
  p[1] = 8;
  uint16_t *p16 = (uint16_t*)(p+2);
  *p16 = 4;
  unsigned int *lenptr = (unsigned int*)(p+4);
  *lenptr = strlen(foo);
  strcpy( p + 8, "test" );
  strcpy( p + 12, foo );

  struct iovec iov;
  iov.iov_base = p;
  iov.iov_len = 12 + strlen(foo);
  mrWritevf( loop, fd, &iov, 1 );

  char buf2[4096];
  p = buf2;
  p[0] = 0;
  p[1] = 7;
  p16 = (uint16_t*)(p+2);
  *p16 = 4;
  strcpy( p + 4, "test" );

  struct iovec iov2;
  iov2.iov_base = p;
  iov2.iov_len = 8;
  //mrWritevf( loop, fd, &iov2, 1 );

  if ( 1 ) {
    p += 32;
    p[0] = 0;
    p[1] = 7;
    p16 = (uint16_t*)(p+2);
    *p16 = 4;
    strcpy( p + 4, "test" );
  
    for( int i = 0; i < PIPE; i++ ) {
      iovs[i].iov_base = p;
      iovs[i].iov_len = 8;
    }
    gettimeofday(&tv1, NULL);
  
    //mrWritevf( loop, fd, iovs, PIPE );
    //mrFlush(loop);
    for( int i = 0; i < 100; i++ ) {
      mrWritev( loop, fd, iovs, PIPE );
    }
    mrFlush(loop);
  }

  runLoop(loop);
  freeLoop(loop);
  exit(-1);

}

void get64test(int fd) {

  char buf[4096];
  char buf2[4096];
  char *p = buf;
  p[0] = 0;
  p[1] = 2;
  unsigned long *keyp = (unsigned long*)(p+2);
  *keyp = 2;
  unsigned int *lenptr = (unsigned int*)(p+10);

  struct iovec iov;
  iov.iov_base = p;
  iov.iov_len = 18;
  mrWritevf( loop, fd, &iov, 1 );

/*
  p = buf2;
  p[0] = 0;
  p[1] = 1;
  p16 = (uint16_t*)(p+2);
  *p16 = 2;

  struct iovec iov2;
  iov2.iov_base = p;
  iov2.iov_len = 4;
  mrWritevf( loop, fd, &iov2, 1 );
*/
  p += 18;
  p[0] = 0;
  p[1] = 1;
  keyp = (unsigned long*)(p+2);
  *keyp = 2;

  for( int i = 0; i < PIPE; i++ ) {
    iovs[i].iov_base = p;
    iovs[i].iov_len = 10;
  }
  gettimeofday(&tv1, NULL);

  //mrWritevf( loop, fd, iovs, PIPE );
  //mrFlush(loop);
  for( int i = 0; i < 100; i++ ) {
    mrWritev( loop, fd, iovs, PIPE );
  }
  mrFlush(loop);


  runLoop(loop);
  freeLoop(loop);
  exit(-1);
}

int main() {

  //fork();
  //fork();

  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);
  
  loop = createLoop(sig_handler);
  int fd = mrConnect(loop,"localhost", 7000, on_data);
  printf("fd = %d\n");
  //addTimer(loop, 10, on_timer);

  longTest(fd);
  //get64test(fd);

}
