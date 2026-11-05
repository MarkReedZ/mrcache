# io_uring notes for mrcache

This is the working reference for changing mrcache's I/O. It states the
rules an implementation has to follow. The upstream papers live in
`docs/references/` and are older than the network path in `net.c`. Where
they disagree with this note, trust this note plus the liburing man pages
and `linux/io_uring.h`.

mrcache is one thread, one ring, and a cache. Disk support has to be
asynchronous on that same thread. A blocking `pread` or `pwrite` in the
event loop freezes every connection.

## Mental model

`io_uring_setup` creates two rings in memory shared with the kernel:

- The submission queue (SQ) holds submission queue entries (SQEs). The
  application is the producer. The kernel is the consumer.
- The completion queue (CQ) holds completion queue entries (CQEs). The
  kernel is the producer. The application is the consumer.

There is one CQE per completed SQE, except multishot operations, which post
many CQEs for one SQE. `cqe->res` is a byte count on success or a negative
`-errno` on failure. It is never an error pointer and it is not a boolean.

liburing is the interface to use. The raw `mmap` layout is the kernel
contract underneath it; new code should call `io_uring_get_sqe`,
`io_uring_prep_*`, `io_uring_sqe_set_data`, `io_uring_submit`, and
`io_uring_for_each_cqe`.

A typical submission:

```c
struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
if (!sqe) { /* ring is full: reap CQEs, then try again */ }
io_uring_prep_read(sqe, fd, buf, len, file_offset);
io_uring_sqe_set_data(sqe, op);   /* op stays live until the CQE */
io_uring_submit(ring);            /* or submit once per loop turn */
```

`io_uring_get_sqe` only reserves a slot in userspace. The kernel does not
see the SQE until submit (or until the SQPOLL thread notices the tail).
Preparing many SQEs and submitting once is the normal pattern. Submitting
inside every completion handler, which `net.c` does today, works but
spends extra enters and makes backpressure harder to see.

## SQE and CQE fields that matter

`struct io_uring_sqe` (64 bytes unless `IORING_SETUP_SQE128`):

| Field | Role |
| --- | --- |
| `opcode` | Which operation. Set by `io_uring_prep_*`. |
| `flags` | `IOSQE_*` flags. Separate from the opcode's own flag word. |
| `fd` | File or socket. With `IOSQE_FIXED_FILE`, this is an index into the registered file table, not a descriptor number. |
| `off` | File offset for read and write. Ignored by sockets. |
| `addr` | Buffer pointer, or pointer to an `iovec` array for `readv` / `writev`. |
| `len` | Byte count, or iovec count for vectored ops. |
| `rw_flags` | `RWF_*` flags such as `RWF_DSYNC`. Zero for normal cache I/O. |
| `user_data` | Copied unchanged into the CQE. The only reliable way to tell operations apart. |
| `buf_index` | Registered-buffer index for `*_FIXED`, or the provided-buffer group when `IOSQE_BUFFER_SELECT` is set. |
| `ioprio` | Also carries recv/send flags such as `IORING_RECVSEND_POLL_FIRST`. |

`struct io_uring_cqe`:

| Field | Role |
| --- | --- |
| `user_data` | The value set on the SQE. |
| `res` | Bytes transferred, or `-errno`. |
| `flags` | `IORING_CQE_F_*`. Buffer id is `flags >> IORING_CQE_BUFFER_SHIFT` (16) when `IORING_CQE_F_BUFFER` is set. |

`IORING_CQE_F_MORE` means a multishot SQE will post another CQE. The final
CQE for that SQE does not have `F_MORE`. That final CQE is still a
completion of the original operation.

## Ordering

The kernel starts SQEs in submission order and is free to finish them in
any other order. A disk read submitted before a socket write can complete
after it. Code that needs an order has three tools:

- `IOSQE_IO_LINK`. The next SQE starts only after this one succeeds. If
  this one fails, the linked tail is cancelled with `-ECANCELED` and is
  not run. Use this for a short chain whose arguments are known up front
  (write this block, then an `fallocate` punch of an old region).
- `IOSQE_IO_HARDLINK`. Same chain, but a failure does not cancel the rest.
- `IOSQE_IO_DRAIN`. The SQE waits until every earlier submission on the
  ring has completed. This stalls unrelated connections. Do not use it in
  the server.

A link cannot express "read an item, then write `size+4` bytes to the
socket", because the byte count is inside the bytes being read. That is
two submissions, connected by the application: the read CQE parses the
header and only then prepares the socket write. Keep the read buffer live
across both.

Completions of independent connections should stay unlinked so one slow
disk read does not hold the ring.

## Buffer lifetime

These are the rules that make use-after-free bugs in this style of server:

- The kernel may read or write a buffer from submission until the matching
  CQE is reaped. Do not free, reuse, or overwrite that buffer before then.
- `user_data` must point at memory that stays valid until the CQE. No
  stack addresses.
- After the CQE, the buffer belongs to the application again, unless the
  operation was multishot and `F_MORE` is set, in which case only that
  one provided buffer was returned and the SQE is still armed.
- A short transfer (`res > 0 && res < len`) is success for the prefix.
  Retry the remainder at `file_offset + res` with the buffer advanced by
  `res`. Do not treat a short write of a sealed block as done, and do not
  reuse the block.
- `res < 0` means nothing was transferred. `-EAGAIN` and `-EINTR` are
  retries. `-ENOBUFS` on a provided-buffer recv means the buffer group was
  empty; replenish and rearm. `-ECANCELED` is the link or cancel path.
  `-EIO`, `-ENOSPC`, and `-EROFS` are failures of that operation.

Provided buffers (the recv path) have a second lifetime. The application
adds buffers to a buffer ring, which hands them to the kernel. A recv CQE
with `IORING_CQE_F_BUFFER` hands one buffer back. The application owns it
until `io_uring_buf_ring_add` plus `io_uring_buf_ring_advance`. Recycling
the recv buffer while a `writev` still points into it is a use after free.
mrcache avoids that today by copying partial commands into the connection
buffer and by pointing GET replies at the item arena or at a static
not-found buffer, not at the recv buffer.

## Opcodes for this project

| Opcode | Prep helper | Use here |
| --- | --- | --- |
| `IORING_OP_ACCEPT` | `io_uring_prep_multishot_accept` | Already used. One armed accept posts a CQE per connection. Rearm when a CQE arrives without `F_MORE`. |
| `IORING_OP_RECV` | `io_uring_prep_recv_multishot` | Already used, with `IOSQE_BUFFER_SELECT` and a buffer-ring group. `addr` is NULL. |
| `IORING_OP_WRITEV` | `io_uring_prep_writev` | Already used for gathered socket replies. `off` must be 0. Not for disk. |
| `IORING_OP_READ` | `io_uring_prep_read` | Positioned item reads. `off` is the file offset. |
| `IORING_OP_WRITE` | `io_uring_prep_write` | Positioned sealed-block writes when the pages are not registered. |
| `IORING_OP_READ_FIXED` / `IORING_OP_WRITE_FIXED` | `io_uring_prep_read_fixed`, `io_uring_prep_write_fixed` | Same, but `buf` must lie inside the registered buffer selected by `buf_index`. Avoids per-op page pinning. |
| `IORING_OP_FSYNC` | `io_uring_prep_fsync` | Not used by the capacity tier. A durability checkpoint would link it after a data write. `IORING_FSYNC_DATASYNC` is the data-only variant. |
| `IORING_OP_FALLOCATE` | `io_uring_prep_fallocate` | Preallocate the data file at startup (`FALLOC_FL_KEEP_SIZE` is wrong for this; use 0 mode to allocate). Optional `FALLOC_FL_PUNCH_HOLE` when dropping a region. |
| `IORING_OP_CLOSE` / `IORING_OP_ASYNC_CANCEL` | `io_uring_prep_close`, `io_uring_prep_cancel` | Connection teardown. Cancel the multishot recv and any disk read for that connection before closing. |
| `IORING_OP_READ_MULTISHOT` | — | Pollable files only (pipes, sockets). Regular files and block devices are not eligible. Do not use it for the cache file. |

`IORING_OP_READV` / `WRITEV` also work on files. A sealed block is one
contiguous 16 MiB range, so a single `WRITE` is enough. Vectored disk I/O
is not needed.

Open cache files with `O_RDWR` and without `O_APPEND`. `O_APPEND` ignores
the SQE offset, and the sketched `open` in `blocks.c` uses it. Positioned
reads and writes both depend on `off`.

## Registered files and registered buffers

`io_uring_register_files` installs a file table on the ring. Later SQEs
set `IOSQE_FIXED_FILE` and pass the index as `fd`. The kernel skips the
file-table lookup. Register the cache data file once. Update the table
with `io_uring_register_files_update` if a file is replaced. Do not close
a descriptor that is still registered.

`io_uring_register_buffers` pins a list of `iovec`s for the life of the
registration. `READ_FIXED` / `WRITE_FIXED` pass an interior pointer: `buf`
may be any address inside that registered range, and `len` bytes from
there are transferred. Register the item arena as one buffer and pass the
address of the sealed 16 MiB block. Register the disk-read pool as a
second buffer (or as further entries).

Registration pins pages and charges `RLIMIT_MEMLOCK`. An unprivileged
default memlock is far smaller than the arena. `WRITE_FIXED` of a
multi-gigabyte arena requires raising memlock (or `CAP_IPC_LOCK`). Plain
`IORING_OP_WRITE` pins pages only for the duration of that write and does
not need a multi-gigabyte memlock. Start with plain `READ` and `WRITE`.
Move the sealed-block write to `WRITE_FIXED` only after memlock is set
deliberately.

Registered buffers must remain mapped at the same addresses until
`io_uring_unregister_buffers`. The arena already lives for the process
lifetime, which matches this.

## O_DIRECT and IOPOLL

`O_DIRECT` bypasses the page cache. It requires alignment of the file
offset, the buffer address, and the length to the logical block size
(usually 512 or 4096). A 16 MiB sealed block can satisfy that if the arena
is page-aligned (`posix_memalign`, not plain `malloc`) and the file offset
is a multiple of the block size. A packed item is not aligned and is not a
multiple of 4096, so an item read cannot be a raw `O_DIRECT` read of the
item bytes. The direct-I/O read has to pull an aligned window into a
bounce buffer and then slice the item out.

`IORING_SETUP_IOPOLL` polls the device for completions instead of waiting
for interrupts. It applies to a ring used for direct storage I/O. It
cannot host the socket operations the server already runs. Using it means
a second ring and a way to wake the network ring (`IORING_OP_MSG_RING`,
since 5.18). That split is optional and not part of the first disk
milestone.

Buffered writes leave the just-evicted block in the page cache, so a
follow-up GET often completes as a memory copy inside the kernel. That is
a real latency win and a real accounting lie: the kernel holds a second
copy of the cache, and reclaim can drop it at any time. `O_DIRECT` makes
`max-memory` honest and makes every cold GET pay device latency. The
capacity tier should start buffered for that reason.

## Setup flags

mrcache currently passes `io_uring_queue_init_params` this set:

| Flag | On the ring today | Meaning |
| --- | --- | --- |
| `IORING_SETUP_SQPOLL` | yes | A kernel thread submits SQEs. Userspace does not have to enter on every submit. `sq_thread_idle` is 1000 ms. Creating the thread may require privileges (`CAP_SYS_NICE` on kernels that still gate SQPOLL). |
| `IORING_SETUP_SINGLE_ISSUER` | yes | One task submits. Required for `DEFER_TASKRUN`. Keep the ring on the event-loop thread. |
| `IORING_SETUP_CQSIZE` and `IORING_SETUP_CLAMP` | yes | CQ is requested at 4096 entries and clamped to what the kernel allows. SQ depth is also 4096. |
| `IORING_FEAT_FAST_POLL`, `IORING_FEAT_SQPOLL_NONFIXED` written into `params.features` | ignored | `features` is an output of setup, not an input. The kernel overwrites it. Those two symbols are feature bits, not setup flags. |

Add `IORING_SETUP_DEFER_TASKRUN` (Linux 6.1, requires `SINGLE_ISSUER`) when
disk completions land. With that flag, completion task_work runs when the
application waits for or fetches events, instead of on every kernel entry.
That is what keeps a batch of disk and socket CQEs together.

Do not set `IORING_SETUP_IOPOLL` on this ring.

SQPOLL does not by itself deliver CQEs to a thread that never enters the
kernel. Task_work for the issuer has to run in the issuer. Today's loop
never calls `io_uring_wait_cqe`. It gets away with that during the GET
benchmark because every reply calls `io_uring_submit`, and that submit
often enters. A disk read with no socket write pending has nothing to
submit, and a pure userspace spin will not observe the CQE.

The loop has to be able to sleep on the CQ:

```c
for (;;) {
    int rc = io_uring_submit_and_wait(&ring, busy ? 0 : 1);
    if (rc < 0 && rc != -EINTR) { /* fatal */ }

    unsigned head;
    struct io_uring_cqe *cqe;
    int n = 0;
    io_uring_for_each_cqe(&ring, head, cqe) {
        dispatch(cqe);   /* may prepare SQEs; must not free the cqe */
        n++;
    }
    if (n)
        io_uring_cq_advance(&ring, n);
}
```

`busy` can be a short spin budget so an idle server sleeps and a saturated
server does not. `submit_and_wait` with a wait count of 0 still processes
deferred task_work when `DEFER_TASKRUN` is set, as long as the liburing
helper issues `IORING_ENTER_GETEVENTS`. If a helper returns without
entering, call `io_uring_get_events` when `IORING_SQ_TASKRUN` is set in the
SQ flags. Check the liburing version's `io_uring_submit_and_wait` against
this before relying on it.

Advance the CQ only after the handlers have copied anything they need out
of the CQE. `io_uring_cqe_get_data` is just `user_data`.

## How to tell completions apart

`net.c` uses three `user_data` conventions on one ring: an accept sentinel,
a `my_conn_t *` for recv, and an `iovecs_t *` for writev. The handler
treats `F_MORE` as "this is a recv" and anything else as "this is a
write". That is already wrong for the last multishot recv CQE, which has
no `F_MORE` and is not a write. Disk CQEs would be misread the same way.

Every submission should carry a small heap object:

```c
enum op_kind {
    OP_ACCEPT,
    OP_RECV,        /* user_data is the conn; multishot, lives until cancel */
    OP_SEND,        /* gathered writev */
    OP_DISK_WRITE,  /* sealed 16 MiB block */
    OP_DISK_READ    /* one item read for one GET */
};

struct uring_op {
    enum op_kind kind;
    void *ptr;          /* conn, iovecs, block write, or get request */
};
```

Multishot recv can keep storing the `conn *` itself if accept, send, disk
write, and disk read all store a pointer to `struct uring_op` and the
handler branches on a tag in the first field. Do not branch on `F_MORE`.
Use `F_MORE` only to decide whether a multishot SQE is still armed.

The accept path should rearm when the accept CQE lacks `F_MORE` (error or
a kernel that ended the multishot). Check `cqe->res` for a negative error
before calling `conn_new`.

## Disk patterns

### Sealed-block write

A block is immutable once it is sealed. Submit one `WRITE` of `1 << 24`
bytes at the file offset for that block sequence. The buffer is the arena
slot itself, or a copy if the slot must be reused immediately.

Prefer not copying. Hold the arena slot until the write CQE reports the
full 16 MiB. `blocks_alloc` stalls SET when the next slot is still in
flight. One extra 16 MiB slot of slack hides a single in-flight write. If
the device falls behind ingest, SET backpressure is the correct behavior.
GETs of blocks that are still in memory keep running.

On a short write, resubmit the tail. On `-EIO` or `-ENOSPC`, mark the
block as dropped rather than on disk, free the slot, and count the error.
Do not retry forever on the event-loop thread, and do not `exit`.

The write is not durable. Buffered data sits dirty until the kernel
flushes it. That matches a cache. Do not `FSYNC` on the eviction path.

### Item read

Submit `READ` at `file_offset(block) + item_offset` into a buffer from a
fixed pool (a few hundred buffers, two sizes is enough: 32 KiB and a
fallback up to the largest item that fits in a block). On completion:

1. If `res < 6`, the header is incomplete: fail the GET or retry.
2. Parse `keysize` and `valuesize`. Total is `6 + valuesize + keysize`.
3. If `res` covers the total, compare the key, then `writev` the reply
   from `buf + 2` for `valuesize + 4` bytes. Release the read buffer on
   the send CQE, not on the read CQE.
4. If `res` is short of the total, submit a second read for the tail.
   This is the path for values larger than the first window. The 10 KiB
   benchmark item fits in one 32 KiB read.

Do not `malloc` a buffer per read on the completion path. The pool bound
is also the bound on in-flight disk GETs. When the pool is empty, leave
the GET queued and stop pulling more work only if the queue itself is
bounded. A per-connection cap (one in-flight disk read) plus a global cap
(the pool size) is enough.

### What not to do

- Do not call `pread`, `pwrite`, `read`, `write`, or `fsync` on the event
  loop.
- Do not use `O_APPEND` on the cache file.
- Do not link a disk read to the socket write.
- Do not use `IOSQE_IO_DRAIN`.
- Do not reuse a sealed block, a read buffer, or a send iovec base before
  its CQE.
- Do not treat `F_MORE` as the operation type.
- Do not use `READ_MULTISHOT` on the cache file.
- Do not put `IOPOLL` on the network ring.
- Do not assume SQPOLL delivers completions with no enter.
- Do not ignore `io_uring_get_sqe` returning NULL. Reap and retry, or
  apply backpressure. A NULL SQE today is a crash.
- Do not register the arena until memlock is sized for it.

## Kernel versions

The README's "5.5+" and "5.8+" lines describe an older tree. The binary
as it stands needs at least:

| Feature in the current server | Kernel |
| --- | --- |
| Rings, `READ`/`WRITE`, fixed files and buffers, `SQPOLL` | 5.1 |
| `IORING_OP_READ` / `IORING_OP_WRITE` (non-vectored, non-fixed) | 5.6 |
| Legacy provided buffers | 5.7 |
| Buffer rings, multishot accept, `COOP_TASKRUN` | 5.19 |
| Multishot recv, `SINGLE_ISSUER` | 6.0 |
| `DEFER_TASKRUN` | 6.1 |

So the running server requires Linux 6.0 or newer. Disk work that follows
this note should target 6.1 or newer so `DEFER_TASKRUN` is available.
Probe with `io_uring_queue_init_params` and refuse to start if the kernel
rejects the flags, which the init path already does for the current set.

## Where this shows up in the tree

| Path | What it does |
| --- | --- |
| `net.c` | The live ring: SQPOLL, multishot accept, multishot recv, provided buffers, `writev` replies. |
| `blocks.c` | Commented `mr_writevcb` / `mr_readvcb` sketches from the old mrloop wrapper. Not wired to the ring in `net.c`. |
| `tests/disk/tst.c` | A standalone mrloop file write, not the server. |
| `docs/architecture.md` | How the cache uses these rules, and the disk design. |
