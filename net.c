#define _GNU_SOURCE

#include "net.h"
#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "liburing.h"

#define BUF_SIZE   (16 * 1024)
#define NR_BUFS    1024
#define BR_MASK    (NR_BUFS - 1)
#define BGID       1
#define MAX_IOV    64
#define TAG_ACCEPT 1ull
#define TAG_RECV   2ull
#define TAG_SEND   3ull
#define TAG_CANCEL 4ull
#define NO_PIN     0xffffffffu

struct batch {
    struct iovec iov[MAX_IOV];
    uint32_t pin_slot[MAX_IOV];
    void *to_free[MAX_IOV];
    int n;
};

struct conn {
    int fd;
    int recv_armed;
    int closing;
    int stalled;
    int send_live;
    int cur;
    int inflight;
    uint8_t *in;
    int in_len;
    int in_cap;
    struct batch batches[2];
    struct conn *next;
    struct conn *all_next;
};

static struct io_uring ring;
static struct io_uring_buf_ring *br;
static uint8_t *ring_buf;
static store_t *db;
static struct conn *all_conns;
static volatile sig_atomic_t stop_flag;
static int listen_fd = -1;

static const uint8_t STATUS_OK = MRC_OK;
static const uint8_t STATUS_NOTFOUND = MRC_NOTFOUND;
static const uint8_t STATUS_TOO_BIG = MRC_TOO_BIG;
static const uint8_t MISS_RESP[5] = { MRC_NOTFOUND, 0, 0, 0, 0 };

static void on_signal(int sig) { (void)sig; stop_flag = 1; }

static uint64_t pack(struct conn *c, uint64_t tag) {
    return ((uint64_t)(uintptr_t)c) | tag;
}

static struct conn *unpack(uint64_t ud, uint64_t *tag) {
    *tag = ud & 7ull;
    if (*tag == TAG_ACCEPT) return NULL;
    return (struct conn *)(uintptr_t)(ud & ~7ull);
}

static struct io_uring_sqe *get_sqe(void) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    if (sqe) return sqe;
    io_uring_submit(&ring);
    return io_uring_get_sqe(&ring);
}

static void recycle(int bid) {
    io_uring_buf_ring_add(br, ring_buf + (size_t)bid * BUF_SIZE, BUF_SIZE,
                          (unsigned short)bid, BR_MASK, 0);
    io_uring_buf_ring_advance(br, 1);
}

static void release_batch(struct batch *b) {
    int i;
    for (i = 0; i < b->n; i++) {
        if (b->pin_slot[i] != NO_PIN) store_unpin(db, b->pin_slot[i]);
        free(b->to_free[i]);
        b->to_free[i] = NULL;
        b->pin_slot[i] = NO_PIN;
    }
    b->n = 0;
}

static void destroy_conn(struct conn *c) {
    struct conn **pp;
    if (c->fd >= 0) {
        close(c->fd);
        c->fd = -1;
    }
    release_batch(&c->batches[0]);
    release_batch(&c->batches[1]);
    free(c->in);
    for (pp = &all_conns; *pp; pp = &(*pp)->all_next) {
        if (*pp == c) { *pp = c->all_next; break; }
    }
    free(c);
}

static void maybe_free(struct conn *c);

static int flush_conn(struct conn *c) {
    struct batch *b = &c->batches[c->cur];
    struct io_uring_sqe *sqe;
    if (b->n == 0) return 1;
    if (c->send_live || c->closing) return 0;
    sqe = get_sqe();
    if (!sqe) return 0;
    io_uring_prep_writev(sqe, c->fd, b->iov, (unsigned)b->n, 0);
    io_uring_sqe_set_data64(sqe, pack(c, TAG_SEND));
    c->inflight = c->cur;
    c->cur ^= 1;
    c->send_live = 1;
    return 1;
}

static int queue_iov(struct conn *c, const void *p, int len, uint32_t pin, void *owned) {
    struct batch *b;
    if (len < 0) return 0;
    b = &c->batches[c->cur];
    if (b->n >= MAX_IOV) {
        if (!flush_conn(c)) return 0;
        b = &c->batches[c->cur];
        if (b->n >= MAX_IOV) return 0;
    }
    b->iov[b->n].iov_base = (void *)p;
    b->iov[b->n].iov_len = (size_t)len;
    b->pin_slot[b->n] = pin;
    b->to_free[b->n] = owned;
    if (pin != NO_PIN) store_pin(db, pin);
    b->n++;
    return 1;
}

static void begin_cancel(struct conn *c) {
    struct io_uring_sqe *sqe;
    if (!c->recv_armed) return;
    sqe = get_sqe();
    if (!sqe) return;
    io_uring_prep_cancel_fd(sqe, c->fd, 0);
    io_uring_sqe_set_data64(sqe, pack(c, TAG_CANCEL));
}

static void maybe_free(struct conn *c) {
    if (c->closing && !c->recv_armed && !c->send_live)
        destroy_conn(c);
}

static int append_in(struct conn *c, const uint8_t *p, int n) {
    if (n <= 0) return 0;
    if (c->in_len + n > c->in_cap) {
        int cap = c->in_cap ? c->in_cap : 4096;
        uint8_t *grown;
        while (cap < c->in_len + n) {
            if (cap > 32 * 1024 * 1024) return -1;
            cap *= 2;
        }
        grown = realloc(c->in, (size_t)cap);
        if (!grown) return -1;
        c->in = grown;
        c->in_cap = cap;
    }
    memcpy(c->in + c->in_len, p, (size_t)n);
    c->in_len += n;
    return 0;
}

/* One command. >0 consumed, 0 need more bytes, -2 blocked, -1 fatal. */
static int try_one(struct conn *c, uint8_t *p, int n) {
    uint16_t klen;
    uint32_t vlen;
    uint8_t op;
    int body;
    if (n < MRC_HDR) return 0;
    if (p[0] != MRC_VERSION) return -1;
    op = p[1];
    memcpy(&klen, p + 2, 2);
    memcpy(&vlen, p + 4, 4);
    if (op == MRC_SET) {
        uint32_t maxv;
        if (klen == 0) return -1;
        maxv = store_max_value(db, klen);
        if (vlen > maxv) {
            /* Refuse to accumulate a value that cannot be stored. */
            if (vlen > 1024u * 1024u) return -1;
            body = (int)klen + (int)vlen;
            if (n < MRC_HDR + body) return 0;
            if (!queue_iov(c, &STATUS_TOO_BIG, 1, NO_PIN, NULL)) return -2;
            return MRC_HDR + body;
        }
        body = (int)klen + (int)vlen;
    } else if (op == MRC_GET || op == MRC_DEL) {
        if (klen == 0 || vlen != 0) return -1;
        body = klen;
    } else if (op == MRC_PING || op == MRC_STAT) {
        if (klen != 0 || vlen != 0) return -1;
        body = 0;
    } else {
        return -1;
    }
    if (n < MRC_HDR + body) return 0;

    if (op == MRC_GET) {
        uint32_t rlen = 0, slot = 0;
        uint8_t *it = store_get(db, p + MRC_HDR, klen, &rlen, &slot);
        if (!it) {
            if (!queue_iov(c, MISS_RESP, 5, NO_PIN, NULL)) return -2;
        } else if (!queue_iov(c, it, (int)rlen, slot, NULL)) {
            return -2;
        }
    } else if (op == MRC_SET) {
        int rc = store_set(db, p + MRC_HDR, klen, p + MRC_HDR + klen, vlen);
        if (rc == STORE_BLOCKED) {
            flush_conn(c);
            rc = store_set(db, p + MRC_HDR, klen, p + MRC_HDR + klen, vlen);
        }
        if (rc == STORE_BLOCKED) return -2;
        if (rc == STORE_TOO_BIG) {
            if (!queue_iov(c, &STATUS_TOO_BIG, 1, NO_PIN, NULL)) return -2;
        } else if (!queue_iov(c, &STATUS_OK, 1, NO_PIN, NULL)) {
            return -2;
        }
    } else if (op == MRC_DEL) {
        int rc = store_del(db, p + MRC_HDR, klen);
        const uint8_t *st = rc ? &STATUS_NOTFOUND : &STATUS_OK;
        if (!queue_iov(c, st, 1, NO_PIN, NULL)) return -2;
    } else if (op == MRC_PING) {
        if (!queue_iov(c, &STATUS_OK, 1, NO_PIN, NULL)) return -2;
    } else {
        struct store_stats st;
        uint8_t *msg = malloc(1 + 7 * 8);
        uint64_t fields[7];
        int i;
        if (!msg) return -1;
        store_stat(db, &st);
        msg[0] = MRC_OK;
        fields[0] = st.gets; fields[1] = st.hits; fields[2] = st.misses;
        fields[3] = st.sets; fields[4] = st.dels; fields[5] = st.evicts;
        fields[6] = st.live;
        for (i = 0; i < 7; i++) memcpy(msg + 1 + i * 8, &fields[i], 8);
        if (!queue_iov(c, msg, 1 + 7 * 8, NO_PIN, msg)) {
            free(msg);
            return -2;
        }
    }
    return MRC_HDR + body;
}

static int parse_buf(struct conn *c, uint8_t *p, int n) {
    int off = 0;
    while (off < n) {
        int got = try_one(c, p + off, n - off);
        if (got == -1) return -1;
        if (got == -2) { c->stalled = 1; break; }
        if (got == 0) { c->stalled = 0; break; }
        off += got;
        c->stalled = 0;
    }
    return off;
}

static int handle_bytes(struct conn *c, uint8_t *buf, int n) {
    int consumed;
    if (c->in_len) {
        if (append_in(c, buf, n) < 0) return -1;
        consumed = parse_buf(c, c->in, c->in_len);
        if (consumed < 0) return -1;
        if (consumed > 0) {
            memmove(c->in, c->in + consumed, (size_t)(c->in_len - consumed));
            c->in_len -= consumed;
        }
    } else {
        consumed = parse_buf(c, buf, n);
        if (consumed < 0) return -1;
        if (consumed < n && append_in(c, buf + consumed, n - consumed) < 0)
            return -1;
    }
    flush_conn(c);
    return 0;
}

static void resume_stalled(void) {
    struct conn *c;
    for (c = all_conns; c; ) {
        struct conn *next = c->all_next;
        if (c->stalled && !c->closing && c->in_len) {
            int consumed = parse_buf(c, c->in, c->in_len);
            if (consumed < 0) {
                c->closing = 1;
                begin_cancel(c);
                maybe_free(c);
            } else {
                if (consumed > 0) {
                    memmove(c->in, c->in + consumed, (size_t)(c->in_len - consumed));
                    c->in_len -= consumed;
                }
                flush_conn(c);
            }
        }
        c = next;
    }
}

static int batch_consume(struct batch *b, int nbytes) {
    while (b->n > 0 && nbytes >= (int)b->iov[0].iov_len) {
        nbytes -= (int)b->iov[0].iov_len;
        if (b->pin_slot[0] != NO_PIN) store_unpin(db, b->pin_slot[0]);
        free(b->to_free[0]);
        if (b->n > 1) {
            memmove(&b->iov[0], &b->iov[1], (size_t)(b->n - 1) * sizeof b->iov[0]);
            memmove(&b->pin_slot[0], &b->pin_slot[1], (size_t)(b->n - 1) * sizeof b->pin_slot[0]);
            memmove(&b->to_free[0], &b->to_free[1], (size_t)(b->n - 1) * sizeof b->to_free[0]);
        }
        b->n--;
    }
    if (b->n > 0 && nbytes > 0) {
        b->iov[0].iov_base = (uint8_t *)b->iov[0].iov_base + nbytes;
        b->iov[0].iov_len -= (size_t)nbytes;
    }
    return b->n == 0;
}

static void arm_recv(struct conn *c) {
    struct io_uring_sqe *sqe = get_sqe();
    if (!sqe) return;
    io_uring_prep_recv_multishot(sqe, c->fd, NULL, 0, 0);
    sqe->buf_group = BGID;
    sqe->flags |= IOSQE_BUFFER_SELECT;
    io_uring_sqe_set_data64(sqe, pack(c, TAG_RECV));
    c->recv_armed = 1;
}

static void arm_accept(void) {
    struct io_uring_sqe *sqe = get_sqe();
    if (!sqe) return;
    io_uring_prep_multishot_accept(sqe, listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    io_uring_sqe_set_data64(sqe, TAG_ACCEPT);
}

static struct conn *conn_new(int fd) {
    struct conn *c;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    c = calloc(1, sizeof *c);
    if (!c) { close(fd); return NULL; }
    c->fd = fd;
    c->all_next = all_conns;
    all_conns = c;
    arm_recv(c);
    return c;
}

static void on_send(struct conn *c, int res) {
    struct batch *b = &c->batches[c->inflight];
    if (res <= 0) {
        release_batch(b);
        c->send_live = 0;
        c->closing = 1;
        begin_cancel(c);
        maybe_free(c);
        resume_stalled();
        return;
    }
    if (batch_consume(b, res)) {
        c->send_live = 0;
        if (!c->closing) flush_conn(c);
    } else if (!c->closing) {
        struct io_uring_sqe *sqe = get_sqe();
        if (sqe) {
            io_uring_prep_writev(sqe, c->fd, b->iov, (unsigned)b->n, 0);
            io_uring_sqe_set_data64(sqe, pack(c, TAG_SEND));
        }
    }
    if (c->closing) maybe_free(c);
    else resume_stalled();
}

static void on_recv(struct conn *c, struct io_uring_cqe *cqe) {
    int more = (cqe->flags & IORING_CQE_F_MORE) != 0;
    int has_buf = (cqe->flags & IORING_CQE_F_BUFFER) != 0;
    int bid = has_buf ? (int)(cqe->flags >> IORING_CQE_BUFFER_SHIFT) : -1;

    if (cqe->res <= 0 || c->closing) {
        if (has_buf) recycle(bid);
        c->recv_armed = more;
        if (!c->closing) c->closing = 1;
        if (more) begin_cancel(c);
        else c->recv_armed = 0;
        maybe_free(c);
        return;
    }

    if (!has_buf) {
        c->recv_armed = more;
        c->closing = 1;
        if (more) begin_cancel(c);
        maybe_free(c);
        return;
    }

    if (handle_bytes(c, ring_buf + (size_t)bid * BUF_SIZE, cqe->res) < 0) {
        c->closing = 1;
        recycle(bid);
        c->recv_armed = more;
        if (more) begin_cancel(c);
        else c->recv_armed = 0;
        maybe_free(c);
        return;
    }
    recycle(bid);
    c->recv_armed = more;
    if (!more && !c->closing) arm_recv(c);
    if (c->closing) maybe_free(c);
}

static void on_accept(struct io_uring_cqe *cqe) {
    int more = (cqe->flags & IORING_CQE_F_MORE) != 0;
    if (cqe->res >= 0)
        conn_new(cqe->res);
    if (!more)
        arm_accept();
}

static int setup_ring(void) {
    struct io_uring_params p;
    /* SQPOLL and DEFER_TASKRUN cannot be combined. SQPOLL is the fast path
     * when the kernel allows it; otherwise completions are deferred until
     * the wait below, which is what DEFER_TASKRUN is for. */
    unsigned attempts[] = {
        IORING_SETUP_SQPOLL | IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_CQSIZE,
        IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_CQSIZE,
        IORING_SETUP_CQSIZE,
        0
    };
    int i, ret = -1;
    for (i = 0; i < 4; i++) {
        memset(&p, 0, sizeof p);
        p.flags = attempts[i];
        if (p.flags & IORING_SETUP_CQSIZE) {
            p.flags |= IORING_SETUP_CLAMP;
            p.cq_entries = 4096;
        }
        if (p.flags & IORING_SETUP_SQPOLL)
            p.sq_thread_idle = 2000;
        ret = io_uring_queue_init_params(2048, &ring, &p);
        if (ret == 0) {
            fprintf(stderr, "io_uring sqpoll=%d defer_taskrun=%d\n",
                    (p.flags & IORING_SETUP_SQPOLL) != 0,
                    (p.flags & IORING_SETUP_DEFER_TASKRUN) != 0);
            return 0;
        }
    }
    fprintf(stderr, "io_uring_queue_init: %s\n", strerror(-ret));
    return -1;
}

static int setup_buffers(void) {
    int ret, i;
    if (posix_memalign((void **)&ring_buf, 4096, (size_t)BUF_SIZE * NR_BUFS) != 0)
        return -1;
    br = io_uring_setup_buf_ring(&ring, NR_BUFS, BGID, 0, &ret);
    if (!br) {
        fprintf(stderr, "buf ring: %s\n", strerror(-ret));
        return -1;
    }
    for (i = 0; i < NR_BUFS; i++) {
        io_uring_buf_ring_add(br, ring_buf + (size_t)i * BUF_SIZE, BUF_SIZE,
                              (unsigned short)i, BR_MASK, i);
    }
    io_uring_buf_ring_advance(br, NR_BUFS);
    return 0;
}

static int listen_on(int port) {
    struct sockaddr_in addr;
    int fd, one = 1;
    fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return -1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, 1024) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }
    return fd;
}

int net_run(struct net_config cfg) {
    struct __kernel_timespec ts = { .tv_sec = 1, .tv_nsec = 0 };
    struct sigaction sa;
    db = cfg.store;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    if (setup_ring() < 0) return 1;
    if (setup_buffers() < 0) return 1;
    listen_fd = listen_on(cfg.port);
    if (listen_fd < 0) return 1;
    arm_accept();
    printf("mrcache listening on %d\n", cfg.port);
    fflush(stdout);

    while (!stop_flag) {
        struct io_uring_cqe *cqe = NULL;
        unsigned head;
        int rc, n = 0;
        rc = io_uring_submit_and_wait_timeout(&ring, &cqe, 1, &ts, NULL);
        if (rc == -EINTR || rc == -ETIME) continue;
        if (rc < 0) {
            fprintf(stderr, "wait: %s\n", strerror(-rc));
            break;
        }
        io_uring_for_each_cqe(&ring, head, cqe) {
            uint64_t tag = 0;
            uint64_t ud = io_uring_cqe_get_data64(cqe);
            struct conn *c;
            n++;
            if ((ud & 7ull) == TAG_ACCEPT) {
                on_accept(cqe);
                continue;
            }
            c = unpack(ud, &tag);
            if (!c) continue;
            if (tag == TAG_RECV) on_recv(c, cqe);
            else if (tag == TAG_SEND) on_send(c, cqe->res);
            else if (tag == TAG_CANCEL) maybe_free(c);
        }
        if (n) {
            io_uring_cq_advance(&ring, (unsigned)n);
            /* Publish the writes and recvs armed while handling completions
             * before waiting again, so a reply is not delayed one loop. */
            io_uring_submit(&ring);
        }
    }

    io_uring_queue_exit(&ring);
    free(ring_buf);
    return 0;
}
