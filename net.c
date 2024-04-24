

#include <signal.h>
#include <getopt.h>
#include <netinet/in.h>
#include <errno.h>

#include "liburing.h"
#include "common.h"
#include "mrcache.h"
#include "net.h"

#define BUFFER_SIZE 32*1024
#define READ_BUF_SIZE 1024L * 32 // * 1024
#define NR_BUFS 16 * 1024

#define BR_MASK (NR_BUFS - 1)
#define BGID 1 

#define NEW_CLIENT 0xffffffffffffffff

//static mr_loop_t *loop = NULL;
static struct io_uring uring;
static struct io_uring_buf_ring* br;

struct sockaddr_in addr;
static char* ring_buf;

void net_free() {

  io_uring_free_buf_ring(&uring, br, NR_BUFS, 1);
  io_uring_queue_exit(&uring);

}

void net_shutdown() {
  net_free();
}

my_conn_t *conn_new(int fd ) {
  my_conn_t *c = calloc( 1, sizeof(my_conn_t));
  c->fd = fd;
  c->buf = malloc( BUFFER_SIZE*2 );
  c->max_sz = BUFFER_SIZE*2;
  for (int i = 0; i<NUM_IOVEC_GROUPS; i++ ) {
    c->iovecs[i].fd = fd;
    c->iovecs[i].idx = i;
  }
  return c;
}

void free_conn( my_conn_t *c ) {
  free(c->buf);
  free(c);
}

void conn_append( my_conn_t* c, char *data, int len ) {
  DBG printf(" append cur %d \n", c->cur_sz);

  if ( (c->cur_sz + len) > c->max_sz ) {
    while ( (c->cur_sz + len) > c->max_sz ) c->max_sz <<= 1;
    c->buf = realloc( c->buf, c->max_sz );
  }

  memcpy( c->buf + c->cur_sz, data, len ); 
  c->cur_sz += len;

  DBG printf(" append cur now %d %p \n", c->cur_sz, c->buf);

}

void net_on_write_done(iovecs_t *iovecs, int res) {

  //printf("DELME on_write_done res %d idx %d\n",res, iovecs->idx);
  // Short write
  if ( res < iovecs->bytes ) {
    //printf("DELME short write res %d bytes %d num %d\n",res, iovecs->bytes, iovecs->num);
    int left = iovecs->bytes - res;
    iovecs->bytes = left;
    int i;
    for (i = iovecs->num-1; i > 0; i-- ) {
      left -= iovecs->iovs[i].iov_len;
      
      if ( left == 0 ) break;
      if ( left < 0 ) {
        iovecs->iovs[i].iov_base -= left; 
        iovecs->iovs[i].iov_len += left; 
        break;
      }
    } 

    struct io_uring_sqe *sqe = io_uring_get_sqe(&uring);
    io_uring_prep_writev(sqe, iovecs->fd, &(iovecs->iovs[i]), iovecs->num-i, 0);
    io_uring_sqe_set_data(sqe, iovecs);
    io_uring_submit(&uring);
    return;
  }
    

  if ( res < 0 ) {
    printf(" err: %s\n",strerror(-res));
    exit(-1); // TODO
  }

  for ( int i = 0; i < iovecs->num; i++ ) {
    if ( iovecs->free_me[i] ) {
      free(iovecs->free_me[i]);
      iovecs->free_me[i] = NULL;
    }
  }

  iovecs->in_prog = 0;
  iovecs->num = 0;
  iovecs->bytes = 0;
}
void net_submit_writes( my_conn_t *conn ) {
  iovecs_t *iovecs = conn->iovecs + conn->iov_idx;

  //if ( iovecs->in_prog == 1 ) {
    //printf("Got more cmds while writes are still in progress\n");
    //exit(1);
  //}
  if ( iovecs->num > 0 ) {
    //int bytes = 0;
    //for ( int i = 0; i < iovecs->num; i++ ) {
      //bytes += iovecs->iovs[i].iov_len;
    //}
    //total_bytes += bytes;
    //printf("DELME idx %d num %d\n", conn->iov_idx, iovecs->num);
    
    iovecs->in_prog = 1; 
    struct io_uring_sqe *sqe = io_uring_get_sqe(&uring);
    io_uring_prep_writev(sqe, iovecs->fd, &(iovecs->iovs[0]), iovecs->num, 0);
    io_uring_sqe_set_data(sqe, iovecs);
    io_uring_submit(&uring);
    conn->iov_idx += 1; if ( conn->iov_idx >= NUM_IOVEC_GROUPS ) conn->iov_idx = 0; //TODO
  }
}

void net_gather_write(my_conn_t *conn, char *p, int len, bool free) {
  iovecs_t *iovecs = conn->iovecs + conn->iov_idx;
  iovecs->iovs[iovecs->num].iov_base = p;
  iovecs->iovs[iovecs->num].iov_len  = len;
  iovecs->num += 1;
  iovecs->bytes += len;
  if ( free ) iovecs->free_me[iovecs->num] = p;
}

int net_init_and_run(config_t config) {
    int socket_options = 1;
    int socket_fd = -1;
    int ret;
    struct io_uring_params params;
    struct io_uring_sqe* sqe;
    struct io_uring_cqe* cqe;

    memset(&uring, 0, sizeof(uring));
    memset(&params, 0, sizeof(params));

    params.flags |= IORING_SETUP_SINGLE_ISSUER;
    params.features |= IORING_FEAT_FAST_POLL;
    params.features |= IORING_FEAT_SQPOLL_NONFIXED;
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = 1000; // TODO
    params.flags |= IORING_SETUP_CQSIZE | IORING_SETUP_CLAMP;
    params.cq_entries = 4096;

    ret = io_uring_queue_init_params(4096, &uring, &params);
    if (ret != 0) { printf("Error initializing uring: %s\n",strerror(ret)); goto cleanup; }

    // Setup the ring buffers 
    if (posix_memalign((void**)&ring_buf, 4096, READ_BUF_SIZE * NR_BUFS)) {
        perror("posix memalign");
        exit(1);
    }

    br = io_uring_setup_buf_ring(&uring, NR_BUFS, BGID, 0, &ret);
    if (!br) {
        fprintf(stderr, "Buffer ring register failed %d %s\n", ret, strerror(errno));
        return 1;
    }

    void* ptr = ring_buf;
    for (int i = 0; i < NR_BUFS; i++) {
        io_uring_buf_ring_add(br, ptr, READ_BUF_SIZE, i , BR_MASK, i);
        ptr += READ_BUF_SIZE;
    }
    io_uring_buf_ring_advance(br, NR_BUFS);

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    //address.sin_addr.s_addr = inet_addr(config.hostname);
    address.sin_port = htons(config.port); 

    if ((socket_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)) == -1) {
      printf("socket error : %d ...\n", errno);
      exit(EXIT_FAILURE);
    }

    if (bind(socket_fd, (struct sockaddr*)&address, sizeof(address)) < 0) { printf("Error binding to hostname\n"); goto cleanup; }
    if (listen(socket_fd, config.max_connections) < 0) goto cleanup;

    sqe = io_uring_get_sqe(&uring);
    io_uring_prep_multishot_accept(sqe, socket_fd, NULL, NULL, 0);//IORING_FILE_INDEX_ALLOC);
    io_uring_sqe_set_data64(sqe, NEW_CLIENT);
    io_uring_submit(&uring);

  // Event loop
  while(1) {
    struct io_uring_cqe* cqe;
    unsigned int head;
    unsigned int i = 0;

    //unsigned tail = io_uring_smp_load_acquire((&uring)->cq.ktail);
    //for (head = *(&uring)->cq.khead; \
         (cqe = (head != tail ?  &(&uring)->cq.cqes[io_uring_cqe_index(&uring, head, (&uring)->cq.ring_mask)] : NULL)); \
         head++)  {
    io_uring_for_each_cqe(&uring, head, cqe) {
      if ( cqe->user_data == NEW_CLIENT ) {
        int client_fd = cqe->res;

        my_conn_t *conn = conn_new(client_fd);
    
        struct io_uring_sqe* sqe;
        sqe = io_uring_get_sqe(&uring);
        io_uring_prep_recv_multishot(sqe, client_fd, NULL, 0, 0);
        io_uring_sqe_set_data(sqe, (void*)conn);
        sqe->buf_group = BGID;
        sqe->flags |= IOSQE_BUFFER_SELECT;
        io_uring_submit(&uring);
    
      } else {

        // If this is a multishot recv event
        if (cqe->flags & IORING_CQE_F_MORE) {
          my_conn_t *conn = (my_conn_t*)io_uring_cqe_get_data(cqe);

          int bid = (cqe->flags >> IORING_CQE_BUFFER_SHIFT);
          void *p = ring_buf + bid * READ_BUF_SIZE;
          if ( cqe->res == 0 ) {
            free_conn(conn);
          } else {

            // If we have partial data for this connection
            if ( conn->cur_sz ) {
              conn_append( conn, p, cqe->res );
              if ( conn->cur_sz >= conn->needs ) {
                int tmp = conn->cur_sz;
                conn->cur_sz = 0;
                ret = on_data(conn, conn->buf, tmp);
              } 
            } else {
              ret = on_data(conn, p, cqe->res);
            }
            //if (ret) close( conn->fd ); // TODO How to close

          }
          io_uring_buf_ring_add(br, p, READ_BUF_SIZE, bid, BR_MASK, 0);
          io_uring_buf_ring_advance(br, 1);

        } else { // write done
          iovecs_t *iovecs = (iovecs_t*)io_uring_cqe_get_data(cqe);
          if ( cqe->res <= 0 ) {
            //fprintf(stderr, "multishot cancelled for: %ull\n", conn->fd);
          } else {
            net_on_write_done(iovecs, cqe->res);
          }
        }
      }
      ++i;
      if ( i > 128 ) break;
    }

    if (i) io_uring_cq_advance(&uring, i);
    //if (i) printf("YAY handled %d events\n",i);

  }

  return 0;
cleanup:
  printf("Exiting due to setup failure");
  return 0;
}

/*
static void queue_cancel(struct io_uring *ring, struct conn *c)
{ 
  struct io_uring_sqe *sqe;
  int flags = 0;
    
  if (fixed_files)
    flags |= IORING_ASYNC_CANCEL_FD_FIXED;
  
  sqe = get_sqe(ring);
  io_uring_prep_cancel_fd(sqe, c->in_fd, flags);
  encode_userdata(sqe, c, __CANCEL, 0, c->in_fd);
  c->pending_cancels++;

  if (c->out_fd != -1) {
    sqe = get_sqe(ring);
    io_uring_prep_cancel_fd(sqe, c->out_fd, flags);
    encode_userdata(sqe, c, __CANCEL, 0, c->out_fd);
    c->pending_cancels++;
  }

  io_uring_submit(ring);
}
*/
