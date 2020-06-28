

#include "mrloop.h"

#include <sys/time.h>
static struct timeval  tv1, tv2;

static int buf_index = 0;

static int num_values = 10;
static int vlen = 16;
static char *values[10];
static char *obufs[10];
static int obuf_lens[10];

#define BUFSIZE 2*1024*1024
#define NUM 10000
#define PIPE 64

static int bytes = 0;
static int bw = 0;
static struct iovec iovs[PIPE];
static int reps = 0;
int resp_len = 10;
static char buf2[2*1024*1024];
static char obuf[2*1024*1024];
static int olen = 0;

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

void *setup_conn(int fd, char **buf, int *buflen ) {
  conn_t *conn = calloc( 1, sizeof(conn_t) );
  conn->fd = fd;
  *buf = conn->buf;
  *buflen = BUFSIZE;
  return conn;
}

static int leftover = 0;
static int itemcnt = 0;
static int last_sz = 0;
static char cbuf[BUFSIZE];
static char *cbufp = cbuf;
static int needs = 0;
void on_data(void *conn, int fd, ssize_t nread, char *buf) {

  bytes += nread;
  if ( nread <= 0 ) {
    printf(" errno %d %s\n",errno, strerror(errno));
    return;
  }
  //print_buffer( buf, nread ); printf("\n");
  //printf("on_data sz %d\n", nread );


  int left = nread;
  char *p = buf;
  if ( needs ) {
    memcpy( cbufp, buf, nread );
    cbufp += nread;
    if ( cbufp - cbuf < needs ) return;

    p = cbuf;
    left = cbufp - cbuf;
    cbufp = cbuf;
    
    leftover = 0;
    needs = 0;
  }

  while ( left ) {
    //printf( " itemcnt %d buf ", itemcnt); print_buffer( p, 16 ); //printf("\n");
    if ( left < 6 ) { leftover = left; needs = 6; memcpy( cbufp, p, left ); cbufp += left; return; }
    if ( p[0] != 0 && p[1] != 1 ) {
      printf(" bad response itemcnt %d\n",itemcnt);
      printf( " itemcnt %d buf ", itemcnt); print_buffer( p, 16 ); //printf("\n");
      mr_close(loop, fd);
      mr_stop(loop);
      return;
    }
    p += 2;
    unsigned int  sz  = *((unsigned int*)(p));
    if ( sz != resp_len-6 ) {
      printf(" argh sz is %d\n", sz );
      printf(" Num bytes read %d written %d\n", bytes,bw);
      char statbuf[32]; statbuf[0] = 0; statbuf[1] = 3;
      int n = write( fd, statbuf, 2 );
      mr_close(loop, fd);
      exit(-1);
    }
    //printf("left %d sz %d\n", left, sz );
    if ( left < sz+6 ) { leftover = left; needs = sz+6; memcpy( cbufp, p-2, left ); cbufp += left; return; }
    p += 4;

    p += sz;
    //if ( itemcnt == 21 ) print_buffer( p, left ); //printf("\n");
    //printf( " after itemcnt %d buf ", itemcnt+1); print_buffer( p, 16 ); //printf("\n");
    last_sz = sz;
    if ( left < (sz+6) ) { leftover = left; return; }
    itemcnt += 1;
    left -= sz+6;

    if ( (itemcnt % PIPE) == 0 ) {
      reps += 1;
      if ( reps >= NUM ) {

        gettimeofday(&tv2, NULL);
        double secs = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
        printf ("Total time = %f seconds, gets per second  %f\n", (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec), (PIPE*reps)/secs);

        char statbuf[32]; statbuf[0] = 0; statbuf[1] = 3;
        int n = write( fd, statbuf, 2 );
        mr_close(loop, fd);
        exit(1);

      }
      buf_index = (buf_index+1)%10;
      int n = write( fd, obufs[buf_index], obuf_lens[buf_index] );
      if ( n != obuf_lens[buf_index] ) {
        printf("ERROR n is %d len is %d\n",n, obuf_lens[buf_index]);
        exit(1);
      }
    }
  }

}

void sig_handler(const int sig) {
  printf("Signal handled: %s.\n", strsignal(sig));
  printf(" Num bytes read %d written %d\n", bytes,bw);
  exit(EXIT_SUCCESS);
}


void set(int fd, const char *key, char *val) {

  char buf[2*1024*1024];
  char *p = buf;
  p[0] = 0;
  p[1] = 2;
  uint16_t *keylen = (uint16_t*)(p+2);
  *keylen = strlen(key);
  strcpy( p + 8, key );

  unsigned int *vl = (unsigned int*)(p+4);
  *vl = strlen(val);
  strcpy( p + 8 + strlen(key), val );
  resp_len = strlen(val) + 6;

  int tot = 8 + strlen(val) + strlen(key);
  int n = write(fd, p, tot);
  if ( n < tot ) {
    
    ssize_t written = n;
    while(written != tot) {
      n = write(fd,p,tot-written);
      if (n == -1) { printf("ERROR n -1\n"); exit(1); }
      written += n;
      p += n;
    }

  }
  

}

void setup_buf(int fd, int i) {
  static int n = 0;
  char key[32];
  char *p = obufs[i];

  for( int i = 0; i < PIPE; i++ ) {
    sprintf(key, "test%d", n++);
    if ( n > 100 ) n = 0;
    int kl = strlen(key);
    p[0] = 0; p[1] = 1;
    uint16_t *keylen = (uint16_t*)(p+2);
    *keylen = kl;
    strcpy( p + 4, key );
    //memcpy( p, tmp, l );
    p += 4 + kl;
  }
  obuf_lens[i] = p-obufs[i];
}  

void setup(int fd) {
  for ( int i = 0; i < num_values; i++ ) {
    values[i] = calloc( 1, vlen + 1 );  
    for ( int x = 0; x < vlen; x++ ) {
      values[i][x] = 48 + (i%10);
    }
  }

  // Set 200 keys
  char key[32];
  for ( int x = 0; x < 200; x++ ) {
    sprintf(key, "test%d", x);
    set(fd, key, values[x%num_values]);
  }

  for ( int i = 0; i < 10; i++ ) {
    obufs[i] = calloc( 1, 32*1024 );
    setup_buf(fd, i);
  }
  
}

int main() {

  //fork();
  //fork();

  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);
  
  loop = mr_create_loop(sig_handler);
  int fd = mr_connect(loop,"localhost", 7000, on_data);

  setup(fd);
  printf("Setup done, starting test\n");

  gettimeofday(&tv1, NULL);
  int n = write( fd, obufs[buf_index], obuf_lens[buf_index] );
  if ( n == -1 ) {
    printf("Couldn't connect to server\n");
    printf("ERROR n is %d len is %d\n",n, obuf_lens[buf_index]);
    exit(1);
  }
  if ( n != obuf_lens[buf_index] ) {
    printf("ERROR n is %d len is %d\n",n, obuf_lens[buf_index]);
    exit(1);
  }
  mr_run(loop);
  mr_free(loop);

}
