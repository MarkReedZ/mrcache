

#include <zstd.h>
#include <signal.h>
#include <getopt.h>
#include <netinet/in.h>
#include <errno.h>

#include "liburing.h"

#include "common.h"
#include "mrcache.h"
#include "hashtable.h"
#include "blocks.h"
#include "wyhash.h"
#include "xxhash.h"
#include "net.h"

hashtable_t *mrq_ht;

static char resp_get[2] = {0,1};
static char resp_get_not_found[4] = {0,0,0,0};
static char resp_get_not_found_len = 4;

static char* ring_buf;
static char *zstd_buffer;
static void setup() {
  mrq_ht = malloc( sizeof(hashtable_t) );
  ht_init(mrq_ht, config.index_size * 1024 * 1024);
  blocks_init(&config);

  srand((int)time(NULL));

  // TODO Only malloc if compressed cmds are used
  zstd_buffer = malloc( 16 * 1024 * 1024 + 128 );

}

static void tear_down() {

  net_shutdown();
  free(zstd_buffer);
  exit(-1); // TODO
}

static void print_buffer( char* b, int len ) {
  for ( int z = 0; z < len; z++ ) {
    printf( "%02x ",(int)b[z]);
  }
  printf("\n");
}

int on_data(my_conn_t *conn, char *buf, int data_left) {
  char *p = buf;

  while ( data_left > 0 ) {
    if ( data_left < 2 ) { conn_append( conn, p, data_left ); conn->needs = 2; net_submit_writes(conn); return 0; }

    int cmd   = (unsigned char)p[1];

    if ( cmd == GET ) {

      if ( data_left < 4 ) { conn_append( conn, p, data_left ); conn->needs = 4; net_submit_writes(conn); return 0; }

      uint16_t keylen = *((uint16_t*)(p+2));
      char *key       = p+4;

      if ( data_left < 4+keylen ) { conn_append( conn, p, data_left ); conn->needs = 4+keylen; net_submit_writes(conn); return 0; }

      item *it = NULL;
      unsigned long hv = wyhash(key, keylen, 0, _wyp);
      int rc = ht_find(mrq_ht, key, keylen, hv, (void*)&it);

      config.tot_reads += 1;

      if ( rc == 1 && it ) { 
        net_gather_write( conn, ((char*)it)+2, it->size+4, 0 ); 
      } else {
        net_gather_write( conn, resp_get_not_found, resp_get_not_found_len, 0 );
        config.misses += 1;
      }

      p += 4 + keylen;
      data_left -= 4 + keylen;

    } else if ( cmd == SET ) {

      if ( data_left < 8 ) { conn_append( conn, p, data_left ); conn->needs = 8; net_submit_writes(conn); return 0; }

      uint16_t keylen = *((uint16_t*)(p+2));
      int32_t vlen    = *((int32_t*) (p+4));

      if ( data_left < 8+keylen+vlen ) { conn_append( conn, p, data_left ); conn->needs = 8+keylen+vlen; net_submit_writes(conn); return 0; }
      config.tot_writes += 1;

      char *key = p+8;
      char *val = p+8+keylen;

      uint64_t blockAddr = blocks_alloc( sizeof(item) + vlen + keylen );
      item *it = blocks_translate( blockAddr );

      it->size = vlen;
      memcpy( it->data, p+8+keylen, vlen ); // Val
      it->keysize = keylen;
      memcpy( it->data+vlen, p+8, keylen ); // Key goes after val

      data_left -= (8 + keylen + vlen);
      p += 8 + keylen + vlen;

      unsigned long hv = wyhash(key, keylen, 0, _wyp);
      ht_insert( mrq_ht, blockAddr, key, keylen, hv );


    } else if ( cmd == GETZ ) {

      if ( data_left < 4 ) { conn_append( conn, p, data_left ); conn->needs = 4; net_submit_writes(conn); return 0; }
      uint16_t keylen = *((uint16_t*)(p+2));
      char *key = p+4;

      if ( data_left < 4+keylen ) { conn_append( conn, p, data_left ); conn->needs = 4+keylen; net_submit_writes(conn); return 0; }

      unsigned long hv = wyhash(key, keylen, 0, _wyp);
      item *it = NULL;
      int rc = ht_find(mrq_ht, key, keylen, hv, (void*)&it);

      if ( rc == 1 && it ) { // Found
        config.tot_reads += 1;

        unsigned long long const decomp_sz = ZSTD_getFrameContentSize(it->data, it->size);
        if ( decomp_sz < 0 ) {  // This data wasn't compressed.  Return not found.
          net_gather_write( conn, resp_get_not_found, resp_get_not_found_len, 0 );
        }
        char *dbuf = malloc( decomp_sz+4 );
        int decomp_size = ZSTD_decompress( dbuf+4, decomp_sz, it->data, it->size );
        uint32_t *p32 = (uint32_t*)(dbuf);
        *p32 = decomp_size;

        net_gather_write( conn, dbuf, decomp_size+4, 1 );

      } else {
        net_gather_write( conn, resp_get_not_found, resp_get_not_found_len, 0 );
        config.misses += 1;
      }

      p += 4 + keylen;
      data_left -= 4 + keylen;

    } else if ( cmd == SETZ ) {

      if ( data_left < 8 ) { conn_append( conn, p, data_left ); conn->needs = 8; net_submit_writes(conn); return 0; }

      uint16_t keylen = *((uint16_t*)(p+2));
      uint32_t vlen   = *((uint32_t*)(p+4));

      if ( data_left < 8+keylen+vlen ) { conn_append( conn, p, data_left ); conn->needs = 8+keylen+vlen; net_submit_writes(conn); return 0; }
      config.tot_writes += 1;

      char *key = p+8;
      char *val = p+8+keylen;

      uint64_t blockAddr;
      item *it;

      int cmplen = ZSTD_compress( zstd_buffer, vlen+64, p+8+keylen, vlen, 2 ); // instead of 64 use ZSTD_COMPRESSBOUND?

      // If successful store it otherwise ignore this cmd
      if ( cmplen > 0 ) {

        blockAddr = blocks_alloc( sizeof(item) + cmplen + keylen );
        it = blocks_translate( blockAddr );
        it->keysize = keylen;
        it->size = cmplen;
        memcpy( it->data, zstd_buffer, cmplen ); // Copy in the compressed value
        memcpy( it->data+cmplen, p+8, keylen ); // Key goes after val

        data_left -= (8 + keylen + vlen);
        p += 8 + keylen + vlen;

        unsigned long hv = wyhash(key, keylen, 0, _wyp);
        ht_insert( mrq_ht, blockAddr, key, keylen, hv );

      } 

    } else if ( cmd == STAT ) {

      printf("STAT\n");
      printf("Total reads  %ld\n", config.tot_reads);
      printf("Total misses %ld\n", config.misses);
      printf("Total writes %ld\n", config.tot_writes);
      printf("Avg shift %.2f\n", (double)config.read_shifts/config.tot_reads);
      printf("Max shift %d\n", config.max_shift);
      ht_stat(mrq_ht);

      data_left -= 2;
      p += 2;

    } else { 
      // Invalid cmd
      data_left = 0;
      free_conn(conn);
      return 1; 
    }
  } 

  net_submit_writes(conn);
  return 0;
}


static void sig_handler(const int sig) {
  printf("Signal handled: %s.\n", strsignal(sig));
  if ( sig == SIGINT || sig == SIGTERM ) {
    tear_down();
    exit(EXIT_SUCCESS);
  }
}

static void usage(void) {
  printf( "Mrcache Version 0.1\n"
          "    -h, --help                    This help\n"
          "    -p, --port=<num>              TCP port to listen on (default: 7000)\n"
          "    -m, --max-memory=<mb>         Maximum amount of memory in mb (default: 256)\n"
          //"    -d, --max-disk=<gb>           Maximum amount of disk in gb (default: 1)\n"
          "    -i, --index-size=<mb>         Index size in mb (must be a power of 2 and sz/14 is the max number of items)\n"
          "\n"
        );
}

int main (int argc, char **argv) {

  signal(SIGPIPE, SIG_IGN);

  char *shortopts =
    "h"
    "m:"
    //"d:"
    "i:"
    "p:"
    ;
  const struct option longopts[] = {
    {"help",             no_argument, 0, 'h'},
    {"max-memory", required_argument, 0, 'm'},
    //{"max-disk",   required_argument, 0, 'd'},
    {"index-size", required_argument, 0, 'i'},
    {"port",       required_argument, 0, 'p'},
    {"zstd",             no_argument, 0, 'z'},
    {0, 0, 0, 0}
  };

  config.port = 7000;
  config.max_memory = 128;
  config.flags = 0;
  //config.disk_size = 0;
  config.index_size = 0;
  config.block_size = 16;
  config.max_connections = 128;

  config.read_shifts  = 0;
  config.tot_reads    = 0;
  config.tot_writes   = 0;
  config.write_shifts = 0;

  int optindex, c;
  while (-1 != (c = getopt_long(argc, argv, shortopts, longopts, &optindex))) {
    switch (c) {
    case 'p':
      config.port = atoi(optarg);
      break;
    case 'm':
      config.max_memory = atoi(optarg);
      break;
    //case 'd':
      //config.disk_size = atoi(optarg);
      //break;
    case 'i':
      config.index_size = atoi(optarg);
      break;
    case 'h':
      usage();
      return(2);
      break;
    default:
      usage();
      return(2);
    }
  }

  if ( config.index_size == 0 ) {
    // By default the index size is 10% of memory rounded up to a power of 2
    config.index_size = (config.max_memory * 0.1);
    int power = 1;
    while(power < config.index_size) {
        power <<= 1;
    }
    config.index_size = power;
  }
  if ( !IS_POWER_OF_TWO(config.index_size) ) {
    printf("The index size must be a power of two\n\n");
    usage();
    return(2);
  }

  setup();

  double max_items = (double)(config.index_size>>3)*0.70; // Each entry is 8B max 70% full
  //if ( config.disk_size ) {
    //printf("Mrcache starting up on port %d with %dmb of memory and %dgb of disk allocated.  The max number of items is %0.1fm based on the index size of %dm\n", config.port, config.max_memory, config.disk_size, max_items, config.index_size );
  //} else {
    printf("Mrcache starting up on port %d with %dmb allocated. Maximum items is %0.1fm\n", config.port, config.max_memory+config.index_size, max_items );
  //}

  signal(SIGINT,  sig_handler);
  signal(SIGTERM, sig_handler);
  signal(SIGHUP,  sig_handler);

  return net_init_and_run(config);
}

