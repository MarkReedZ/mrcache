
#include "mrloop.h"
#include "rand_utils.h"
#include <getopt.h>

#include <sys/time.h>
static struct timeval  tv1, tv2;

#define BUFSIZE 64*1024
static int bytes = 0;
static struct iovec iovs[2048];
static double start_time = 0;
static int reps = 0;
static int num = 100000;
static int batch_size = 128;
static int vlen = 16;
static int klen = 4;
static int wcnt = 0;
static int batch_bytes = 0;

static void print_buffer( char* b, int len ) {
  for ( int z = 0; z < len; z++ ) {
    printf( "%02x ",(int)b[z]);
  }
  printf("\n");
}

void on_write_done(void *user_data) {
  printf("on_write_done - %ld\n", (unsigned long)user_data);
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
  conn_t *conn = calloc( 1, sizeof(conn_t) );
  conn->fd = fd;
  *buf = conn->buf;
  *buflen = BUFSIZE;
  return conn;
}

void next_test() {
  exit(1);
}

int on_data(void *conn, int fd, ssize_t nread, char *buf) {
  //printf("on_data >%.*s<\n", 16, buf);
  //print_buffer(buf, nread); 
  //exit(-1);
  bytes += nread;
  if ( bytes >= batch_bytes ) {
    bytes = 0;
    reps += 1;
    if ( reps < num ) {
      mr_writev( loop, fd, iovs, batch_size );
      mr_flush(loop);
    } else {
      gettimeofday(&tv2, NULL);
      double secs = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
      printf ("    Total time = %f seconds cps %f\n", (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec), (batch_size*num)/secs);
      next_test();
    }
  }

  return 0;
}

void sig_handler(const int sig) {
  printf("Signal handled: %s.\n", strsignal(sig));
  printf(" Num bytes %d\n", bytes);
  exit(EXIT_SUCCESS);
}


static void usage(void) {
  printf( "Mrcache Benchmark\n"
          "    -h, --help                    This help\n"
          "    -h, --host=localhost          Hostname (default: localhost)\n"
          "    -p, --port=<num>              TCP port to listen on (default: 7000)\n"
          "    -k, --key-len=<num>           Length of the keys (default: 4)\n"
          "    -v, --value-len=<num>         Length of the values (default: 64)\n"
          "    -n,                           Number of requests (default: 10000)\n"
          "    -b,                           Batch size (default: 128)\n"
          "\n"
        );
} 

int main(int argc, char* argv[]) {
  int port = 7000;
  int compression = 0;
  char *host = "localhost";

  char *shortopts = "p:" "k:" "v:" "n:" "b:" "h:" "z" ; 
  const struct option longopts[] = {
    {"port",       required_argument, 0, 'p'},
    {"key-len",    required_argument, 0, 'k'},
    {"value-len",  required_argument, 0, 'v'},
    {"num",        required_argument, 0, 'n'},
    {"batch-len",  required_argument, 0, 'b'},
    {"host",       required_argument, 0, 'h'},
    {"zstd",       no_argument, 0, 'z'},
    {0, 0, 0, 0}
  };

  int optindex, c;
  while (-1 != (c = getopt_long(argc, argv, shortopts, longopts, &optindex))) {
    switch (c) {
    case 'p': port = atoi(optarg); break;
    case 'v': vlen = atoi(optarg); break;
    case 'k': klen = atoi(optarg); break;
    case 'n': num  = atoi(optarg); break;
    case 'b': batch_size  = atoi(optarg); break;
    case 'z': compression = 1; break;
    case 'h':
      if ( optarg != NULL ) { host = optarg; }
      else { usage(); return(2); }
      break;
    default:
      usage();
      return(2);
    }
  }

  if ( port == 0 ) { printf("ERROR Invalid port specified with -p\n"); exit(-1); }
  if ( vlen == 0 ) { printf("ERROR Invalid value len specified with -v\n"); exit(-1); }
  if ( klen == 0 ) { printf("ERROR Invalid key len specified with -k\n"); exit(-1); }
  if ( num  == 0 ) { printf("ERROR Invalid number of requests specified with -n\n"); exit(-1); }
  if ( batch_size  < 1 || batch_size > 1024 ) { printf("ERROR Invalid batch size, max is 1024\n"); exit(-1); }

  // TODO Can we fork against a list of ports / servers
  //fork();

  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);
  
  printf("Running benchmarks against host %s port %d\n",host, port);
  loop = mr_create_loop(sig_handler);
  int fd = mr_connect(loop,host, port, on_data);

  char buf[4*1024*1024], *p;
  char key[128*1024];
  struct iovec iov;

  ru_rand_bytes( key, klen ); // Random key

  p = buf;
  p[0] = 0; p[1] = 2 + (2*compression);  // 2 is SET and 4 is SETZ
  uint16_t *kp = (uint16_t*)(p+2); *kp = klen;
  uint32_t *vp = (uint32_t*)(p+4); *vp = vlen;
  strncpy( p+8, key, klen );
  ru_rand_bytes( p+8+klen, vlen );
  iov.iov_base = buf;
  iov.iov_len = vlen+klen+8;
  mr_writevf( loop, fd, &iov, 1 );

  p = buf;
  p[0] = 0; p[1] = 1 + (2*compression); // 1 is GET and 3 is GETZ
  kp = (uint16_t*)(p+2);
  *kp = klen;
  strcpy( p + 4, key );

  for( int i = 0; i < batch_size; i++ ) {
    iovs[i].iov_base = buf;
    iovs[i].iov_len = 4+klen;
  }
  printf("  Read Benchmark key len %d value len %d batch size %d total requests %d\n",klen, vlen, batch_size, num);
  batch_bytes = batch_size*(4+vlen);
  start_time = clock();
  gettimeofday(&tv1, NULL);
  mr_writev( loop, fd, iovs, batch_size );
  //mr_writevcb( loop, fd, iovs, batch_size, (void*)reps, on_write_done  );
  mr_flush(loop);


  mr_run(loop);
  mr_free(loop);

}
