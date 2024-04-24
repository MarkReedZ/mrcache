
#define NUM_IOVEC 1024
#define NUM_IOVEC_GROUPS 256
typedef struct _iovecs
{
  int num, fd, idx;
  int in_prog;
  uint64_t bytes;
  struct iovec iovs[NUM_IOVEC];
  void *free_me[NUM_IOVEC];
} iovecs_t;

typedef struct _conn
{
  char *buf;
  int max_sz;
  int cur_sz;
  int needs;
  int fd;

  //getq_item_t *getq_head, *getq_tail;
  bool stalled;

  iovecs_t iovecs[NUM_IOVEC_GROUPS];
  int iov_idx;
  //int iov_start, iov_end, num_iov;
  //int write_in_progress;
} my_conn_t;


void net_free();
my_conn_t *conn_new(int fd );
void free_conn( my_conn_t *c );
void conn_append( my_conn_t* c, char *data, int len );
void net_on_write_done(iovecs_t *iovecs, int res);
int net_init_and_run(config_t config);
void net_shutdown();
void net_submit_writes( my_conn_t *conn );
void net_gather_write(my_conn_t *conn, char *p, int len, bool free);
