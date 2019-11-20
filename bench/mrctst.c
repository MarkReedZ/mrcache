
#include "mrloop.h"

#include <sys/time.h>
static struct timeval  tv1, tv2;


#define BUFSIZE 2*1024*1024
#define NUM 100
#define PIPE 2
static int bytes = 0;
static int bw = 0;
static struct iovec iovs[PIPE];
static int reps = 0;
int resp_len = 10;
static char buf2[2*1024*1024];

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
  //printf("on_data+6 >%.*s<\n", nread-6, buf+6);
  //printf("on_data %d >%.*s<\n", nread, nread > 128 ? 128 : nread, buf);
  //printf("on_data sz %d >%.*s<\n", nread, nread-6, buf+6);
  //exit(1);
  //print_buffer( buf, nread ); printf("\n");



  //printf("on_data sz %d\n", nread );

/*
  int left = nread;
  char *p = buf;
  if ( needs ) {
    memcpy( cbufp, buf, nread );
    cbufp += nread;
    if ( cbufp - cbuf < needs ) return;

    p = cbuf;
    left = cbufp - cbuf;
    cbufp = cbuf;
    
    //left -= (last_sz+6 - leftover); 
    //p += (last_sz+6 - leftover); 
    //itemcnt += 1;
    leftover = 0;
    needs = 0;
    //exit(-1);
  }

  while ( 0 ) {
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
      mr_close(loop, fd);
      exit(-1);
    }
    //printf("left %d sz %d\n", left, sz );
    if ( left < sz+6 ) { leftover = left; needs = sz+6; memcpy( cbufp, p-2, left ); cbufp += left; return; }
    p += 4;

    if (resp_len==10) {
      if ( p[0] != 102 && p[1] != 97 &&
          p[2] != 114 && p[3] != 116 ) {
        printf(" argh not fart left %d ",left); print_buffer(p-6, left+6); 
        printf(" Num bytes read %d written %d\n", bytes,bw);
        mr_close(loop, fd);
        exit(-1);
      }
    }
    for (int i = 0; i < sz; i++ ) {
      if ( p[i] != 0x61 ) {
        printf(" argh not all As i %d left %d ",i, left); print_buffer(p+i-6, 16); 
        printf(" Num bytes read %d written %d\n", bytes,bw);
        mr_close(loop, fd);
        mr_stop(loop);
        return;
      }
    }

    p += sz;
    //if ( itemcnt == 21 ) print_buffer( p, left ); //printf("\n");
    //printf( " after itemcnt %d buf ", itemcnt+1); print_buffer( p, 16 ); //printf("\n");
    last_sz = sz;
    if ( left < (sz+6) ) { leftover = left; return; }
    itemcnt += 1;
    left -= sz+6;
  }
*/ 
  //printf("bytes %d vs %d\n",bytes, PIPE*resp_len*100);
  if ( bytes >= PIPE*resp_len*99 ) {
    itemcnt = 0;
    //printf("bytes %d\n",bytes);
    bytes = 0;
    reps += 1;
    if ( reps < NUM ) {
      //mr_writev( loop, fd, iovs, PIPE );
      for( int i = 0; i < 100; i++ ) {
        mr_writev( loop, fd, iovs, PIPE );
      }
    } else {
      gettimeofday(&tv2, NULL);
      double secs = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec);
      printf ("Total time = %f seconds cps %f\n", (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 + (double) (tv2.tv_sec - tv1.tv_sec), (PIPE*reps*100)/secs);
      mr_close(loop, fd);
      exit(1);

    }
  }

  //printf("on_data\n"); // fd %d >%.*s<\n", ((conn_t*)conn)->fd, nread, buf);
  //print_buffer( buf, nread );
  //struct iovec *iov = malloc( sizeof(struct iovec) );
  //iov->iov_base = buf;
  //iov->iov_len  = nread;
  //mr_writev( loop, ((conn_t*)conn)->fd, iov, 1 );

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

  unsigned int *vlen = (unsigned int*)(p+4);
  *vlen = strlen(val);
  strcpy( p + 8 + strlen(key), val );
  resp_len = strlen(val) + 6;

  printf(" val len %d\n", strlen(val));

  struct iovec iov;
  iov.iov_base = p;
  iov.iov_len = 8 + strlen(val) + strlen(key);
  mr_writevf( loop, fd, &iov, 1 );

}
void setup_iovs(int fd, const char *key) {
  char *p = buf2;
    p[0] = 0;
    p[1] = 1;
    uint16_t *keylen = (uint16_t*)(p+2);
    *keylen = strlen(key);
    strcpy( p + 4, key );
  
    for( int i = 0; i < PIPE; i++ ) {
      iovs[i].iov_base = p;
      iovs[i].iov_len = 4+strlen(key);
    }
  
    //mr_writevf( loop, fd, iovs, PIPE );
    //mr_flush(loop);
    for( int i = 0; i < 100; i++ ) {
      mr_writev( loop, fd, iovs, PIPE );
      bw += resp_len*PIPE;
    }
    mr_flush(loop);

}

int main() {

  //fork();
  //fork();

  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);
  
  loop = mr_create_loop(sig_handler);
  int fd = mr_connect(loop,"localhost", 7000, on_data);
  //addTimer(loop, 10, on_timer);

  //char foo[2048] = "Like any other social media site Facebook has length requirements when it comes to writing on the wall, providing status, messaging and commenting. Understanding how many characters you can use, enables you to more effectively use Facebook as a business or campaign tool. Private messaging is one of the main ways that people interact on Facebook. This type of direct messaging can be either an instant message (chat), or a regular email-type message. For both instant and regular messaging, there is a 20,000 character limit. A Facebook status may have character limits, but considering that it is at 63,206 characters, unless you are writing War and Peace, you should be fine. Facebook has raised this number 12 times to accommodate user’s status and feedback. Facebook wall posts have a 5000 character limit, but truncation begins at 477 characters. This enables someone to write a thoughtful response or create something similar to a blog.";
  char foo[1024*1024] = "On then sake home is am leaf. Of suspicion do departure at extremely he believing. Do know said mind do rent they oh hope of. General enquire picture letters garrets on offices of no on. Say one hearing between excited evening all inhabit thought you. Style begin mr heard by in music tried do. To unreserved projection no introduced invitation.  Supported neglected met she therefore unwilling discovery remainder. Way sentiments two indulgence uncommonly own. Diminution to frequently sentiments he connection continuing indulgence. An my exquisite conveying up defective. Shameless see the tolerably how continued. She enable men twenty elinor points appear. Whose merry ten yet was men seven ought balls.  It allowance prevailed enjoyment in it. Calling observe for who pressed raising his. Can connection instrument astonished unaffected his motionless preference. Announcing say boy precaution unaffected difficulty alteration him. Above be would at so going heard. Engaged at village at am equally proceed. Settle nay length almost ham direct extent. Agreement for listening remainder get attention law acuteness day. Now whatever surprise resolved elegance indulged own way outlived.  Whole every miles as tiled at seven or. Wished he entire esteem mr oh by. Possible bed you pleasure civility boy elegance ham. He prevent request by if in pleased. Picture too and concern has was comfort. Ten difficult resembled eagerness nor. Same park bore on be. Warmth his law design say are person. Pronounce suspected in belonging conveying ye repulsive.  Difficulty on insensible reasonable in. From as went he they. Preference themselves me as thoroughly partiality considered on in estimating. Middletons acceptance discovered projecting so is so or. In or attachment inquietude remarkably comparison at an. Is surrounded prosperous stimulated am me discretion expression. But truth being state can she china widow. Occasional preference fat remarkably now projecting uncommonly dissimilar. Sentiments projection particular companions interested do at my delightful. Listening newspaper in advantage frankness to concluded unwilling.  If wandered relation no surprise of screened doubtful. Overcame no insisted ye of trifling husbands. Might am order hours on found. Or dissimilar companions friendship impossible at diminution. Did yourself carriage learning she man its replying. Sister piqued living her you enable mrs off spirit really. Parish oppose repair is me misery. Quick may saw style after money mrs.  Blind would equal while oh mr do style. Lain led and fact none. One preferred sportsmen resolving the happiness continued. High at of in loud rich true. Oh conveying do immediate acuteness in he. Equally welcome her set nothing has gravity whether parties. Fertile suppose shyness mr up pointed in staying on respect.  That know ask case sex ham dear her spot. Weddings followed the all marianne nor whatever settling. Perhaps six prudent several her had offence. Did had way law dinner square tastes. Recommend concealed yet her procuring see consulted depending. Adieus hunted end plenty are his she afraid. Resources agreement contained propriety applauded neglected use yet.  Lose away off why half led have near bed. At engage simple father of period others except. My giving do summer of though narrow marked at. Spring formal no county ye waited. My whether cheered at regular it of promise blushes perhaps. Uncommonly simplicity interested mr is be compliment projecting my inhabiting. Gentleman he september in oh excellent.  Any delicate you how kindness horrible outlived servants. You high bed wish help call draw side. Girl quit if case mr sing as no have. At none neat am do over will. Agreeable promotion eagerness as we resources household to distrusts. Polite do object at passed it is. Small for ask shade water manor think men begin. ";
  printf("DELME %d\n",strlen(foo));
  //char foo[1024*1024];
  //int l = 50000;
  //for ( int x = 0; x < l; x++ ) {
    //foo[x] = 97;
  //}
  //foo[l] = 0;
  
  set(fd,"test11", foo);
  gettimeofday(&tv1, NULL);
  setup_iovs(fd, "test11");
  //get64test(fd);
  mr_run(loop);
  mr_free(loop);

}
