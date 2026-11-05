# mrcache architecture

mrcache is a single-threaded key-value cache. The index and the values
live in memory. Values are packed into large blocks and thrown away in
FIFO order when the arena is full. The network path is an `io_uring`
completion loop.

Disk is not implemented. `config.disk_size` is never set, the file setup
in `blocks_init` is commented out, and the read and write helpers in
`blocks.c` are not called. The types and comments are a sketch toward a
capacity tier, which this note designs. The I/O rules for that design are
in `docs/io_uring.md`.

The wire protocol stays the same. Disk changes capacity and latency, not
the client API.

## Current architecture

```
clients
  |  TCP, protocol below
  v
net.c  io_uring ring (one thread)
  |  parse GET / SET / GETZ / SETZ / STAT
  v
hashtable.c                blocks.c
open-addressed index       16 MiB slots in one arena
8 bytes / slot             item bytes live here
  |                          ^
  +-------- block+offset ----+
```

One process, one thread, no locks. `main` parses flags, allocates the
index and the arena, then `net_init_and_run` does not return until
shutdown.

### Protocol

Defined in `protocol.txt` and parsed in `on_data`:

| Command | Bytes | Reply |
| --- | --- | --- |
| `GET` (1) | version, cmd, `u16` key length, key | `u32` value length, value. A miss is 4 zero bytes. |
| `SET` (2) | version, cmd, `u16` key length, `u32` value length, key, value | none |
| `GETZ` (3) | same shape as `GET` | value after zstd decompress |
| `SETZ` (4) | same shape as `SET` | none; body is stored compressed |
| `STAT` (5) | version, cmd | none; prints counters on stdout |

The version byte is not checked. `SET` has no reply, so a client learns
about failure only by the connection dropping. A stored empty value and a
miss are the same four zero bytes.

Commands are pipelined. `on_data` walks a recv buffer until a command is
incomplete, copies that tail onto the connection, and remembers how many
bytes the next read must reach (`conn->needs`).

Key length is a `u16` (at most 65535). Value length is a `u32`. The
README limits keys to 32 KiB and values to 16 MiB. The parser does not
enforce either limit. A value that does not fit in one 16 MiB block is
still written and spills into the next slot, so the real limit is one
item per block: header + value + key must be under 16 MiB.

### Item layout

```c
struct item {          /* packed, 6 byte header */
    uint16_t keysize;
    int32_t  size;     /* value length, compressed length for SETZ */
    char     data[];   /* value bytes, then key bytes */
};
```

The key sits after the value so a hit can reply without copying. The
reply is `size + 4` bytes starting at `(char *)item + 2`, which is the
length field and the value. The key is not sent.

That layout is also why the per-item overhead is small. Each live item
costs:

- 6 bytes of header in the arena
- one 8-byte index slot, at a 70% fill limit, so about 11.4 bytes of
  index per live item

Together that is about 18 bytes, which is the figure in the README.
The README's other figure, "index overhead is ~14 bytes" and "sz/14",
does not match this code. `ht_init` divides the index byte budget by 8,
and `max_size` is 70% of that slot count.

### Index

`hashtable_t.tbl` is a flat array of `uint64_t`, allocated once at
startup and never resized. The size is a power of two, defaulting to 10%
of `max_memory` rounded up to a power of two. The entry count is
`index_bytes / 8`.

The hash is `wyhash(key, keylen, seed 0)`. The home slot is
`hash & mask`. Collisions walk forward one slot at a time and wrap.
There are no linked buckets.

Each occupied slot stores a location, not a pointer:

```
63            36 35           24 23                     0
+---------------+---------------+------------------------+
| block id, 28b | fragment, 12b | offset in the block    |
+---------------+---------------+------------------------+
```

`BLOCK_SHIFT` is 36 and the offset mask is 24 bits, matching the 16 MiB
block. On insert, `SET_KEY` overwrites bits 35..24 with the low 12 bits
of the home slot (`wyhash & mask`, truncated). The README describes this
fragment as "the last byte of the key". The code stores 12 bits of the
hash instead. Nothing on the GET path reads the fragment today. It was
left there so a disk miss could reject a slot without reading the key.

A zero slot is empty and ends a probe. Insert of a new key stops at the
first slot that is empty or whose block is no longer in the arena, writes
the location there, and increments `ht->size`. Insert of a key whose
current item is still in memory overwrites that slot and decrements the
old block's item count.

`ht->size` is compared to 70% of the slot count. Crossing that calls
`blocks_lru`, which drops the oldest arena block. `max_shift` records the
longest insert probe. `ht_find` refuses to walk further than that, which
is safe only while every live key was inserted under the same cap.

Lookups that pass an evicted slot keep walking. Evicted slots are not
empty; they are stale locations whose block id is below `min_block`.
`blocks_translate` returns NULL for those, and `ht_find` skips them.
Insert treats them as free and overwrites them. That is the whole
eviction story for the index: stale entries linger until a later insert
reuses the slot, and a lookup that lands on one simply moves on.

### Arena and FIFO

`blocks_init` allocates one contiguous region of `max_memory` megabytes
and splits it into `max_memory / block_size` slots. `block_size` is 16
and is not a flag. The block stride is also hardcoded as `1 << 24`, so
the two only agree while the block size stays 16 MiB.

`blocks_alloc` appends into the current block. If the item does not fit,
it seals the block, bumps `cur_block`, and wraps to `base + (block %
num_blocks) * 16 MiB`. When the wrap would reuse a slot,
`blocks_lru` runs first:

- the slot's item count is subtracted from `ht->size`
- the count is cleared
- `min_block` increments

Items in that block are no longer reachable through `blocks_translate`.
Their index slots remain until overwritten. The cache is FIFO in units of
16 MiB, not LRU in units of keys. A key written once sits in whatever
block was current and dies with that block, even if it is hot.
`blocks_is_near_lru` (true for the four oldest live blocks) is the hook
the README describes for promoting a hit by rewriting it at the head.
Nothing calls it.

Block ids start at 1. They are compared with raw `<` against
`min_block`. That comparison is wrong once ids wrap. The comment in
`blocks.h` says wrap is at 128 TiB of writes; a 28-bit id at 16 MiB per
block actually wraps around 4 PiB. Either way the current compare does
not handle it. `todo` lists wrap as open work.

`items_in_block` is sized to the arena slot count and indexed by
`block % num_blocks`. It is valid for the memory ring only.

### Network loop

`net_init_and_run` builds one ring with SQ depth 4096, CQ depth 4096,
`IORING_SETUP_SQPOLL`, `IORING_SETUP_SINGLE_ISSUER`,
`IORING_SETUP_CQSIZE`, and `IORING_SETUP_CLAMP`. The SQPOLL idle time is
1000 ms. Writes into `params.features` before init do not enable
anything; that field is a result, not a request. See `docs/io_uring.md`.

The recv side is a provided buffer ring: 16384 buffers of 32 KiB, which
is 512 MiB, allocated with `posix_memalign`. That pool is larger than the
default data arena (128 MiB in `main`, 256 MiB in the README). Multishot
accept posts each new connection. Each connection gets one multishot recv
with `IOSQE_BUFFER_SELECT`.

Replies are gathered into `iovec` groups (`NUM_IOVEC` 1024, `NUM_IOVEC_GROUPS`
256) and submitted as `writev`. A short socket write resubmits the tail.
Buffers marked `free` are released in the write completion. The free-me
slot is stored at index `num` after `num` has already been incremented,
so it does not match the iovec it belongs to. Disk replies must not copy
that indexing.

The completion handler is a busy loop. It never waits on the CQ. It
classifies events by `user_data == NEW_CLIENT`, then by
`IORING_CQE_F_MORE`. Multishot recvs that are still armed have `F_MORE`.
Their final CQE does not, and it falls into the write-done path. There is
no handler for a negative `res` on accept. `io_uring_get_sqe` is not
checked for NULL.

This loop is why a small GET is fast: the item is already in the arena,
the reply iovec points at it, and the thread does not sleep. It is also
why disk I/O cannot be added as a synchronous read inside `ht_find`.

`GETZ` decompresses into a malloc'd buffer and gathers a write of that
buffer. `SETZ` compresses into a single global `zstd_buffer` at level 2
before copying into the arena. One global buffer is fine only while one
thread runs the loop. The `-z` flag is in the long-option table and not
in `shortopts`, and no flag is stored. Clients select compression by
sending `SETZ` / `GETZ`.

### What the disk sketch actually contains

`blocks.h` declares `blocks_is_disk`, `blocks_fs_read`, and
`blocks_fs_write`. `mrcache.h` has `disk_item_t` and a `getq_item_t` with
room for three disk reads. `my_conn_t.stalled` exists. The connection's
get queue pointers are commented out.

`blocks_fs_write` mallocs a 16 MiB copy of the arena slot and builds an
iovec. The `mr_writevcb` call is commented out. `blocks_fs_read` mallocs
32 KiB, computes a file offset, and the `mr_readvcb` call is commented
out. File creation is commented out and would open with `O_APPEND`, which
cannot serve those positioned reads. The server links `-lmrloop` and does
not call it. `tests/disk/tst.c` is a separate mrloop write test.

`ht_insert` treats every non-memory slot as free space:

```c
if (!blocks_is_mem(ht->tbl[hash])) break;
```

With disk enabled, that overwrites index entries for keys that are still
live on disk. `todo` calls this out ("Insert has to be disk aware").
`ht_find` never attempts a disk read; a location outside the arena is a
hole in the probe.

So the current server is a memory FIFO cache with a fast `io_uring`
front end. Disk is a set of types, bit-field comments, and dead helpers.

## Disk tier

### What it is for

The arena is the hot tier. Disk is a second FIFO tier for items that have
fallen out of the arena and are still inside the configured disk budget.
The index stays in RAM and covers both tiers. A GET of a memory-resident
item does not change. A GET of a disk-resident item waits for one
positioned read, then uses the same reply path as a memory hit.

This is a capacity feature. It is not a durability feature. A crash, a
kill, or a power loss drops the index, and the files are useless without
it. The first milestone does not `fsync`. Callers that need durability
need a database.

The operator-facing additions are:

- `--max-disk` in gigabytes, default 0 (today's behavior)
- a directory for the data file, default `fsblocks/`
- counters: disk hits, disk misses after a fingerprint match, read
  latency, bytes written, write errors, stalled SETs, in-flight reads

The protocol, the item layout, and the single-threaded loop stay.

### Capacity, in numbers

The index, not the disk, bounds how many keys exist. At 16-byte slots and
a 70% fill (the slot width is justified below):

| Live keys | Index RAM | Value size at which disk pays off |
| --- | --- | --- |
| 1 million | ~23 MB | any; the arena already holds them if values are small |
| 10 million | ~229 MB | around a few hundred bytes and up |
| 100 million | ~2.3 GB | the README's example: 10 KiB values, 1 TB of disk, ~1 TB of payload |

A 16-byte key and a 16-byte value do not belong on this tier. The index
entry is as big as the payload, and a disk read is a bad trade for 16
bytes. The useful shape is the one the README already picks: large values,
many more of them than the arena can hold. Compression (`SETZ`) stores the
compressed bytes, so it multiplies disk capacity the same way it
multiplies the arena, at the cost of a decompress on the hit path after
the read completes.

A 1 TB disk of 16 MiB blocks is 65536 blocks. A 16 GB arena at the same
block size is 1024 blocks. The live window is about 66k blocks. That is
the working set the index and the sequence arithmetic have to cover.

Write traffic is sequential. At a 200 MB/s ingest that misses the arena,
the device sees 200 MB/s of 16 MiB writes. A consumer 1 TB SSD rated at
0.3 drive writes per day is about 3.5 MB/s sustained. This workload will
wear that drive out. The tier wants an endurance-rated NVMe, or a disk
budget the operator has matched to the drive's DWPD. Random item reads are
the other side of the same device: fine for NVMe, poor for a hard disk.
The design assumes SSD.

### Data plane

```
GET key
  hash -> probe index
    empty slot: miss
    fingerprint miss: keep probing
    memory hit: existing zero-copy reply
    disk hit: enqueue on the connection, submit READ, do not block

SET key
  append item to the open arena block
  probe index in memory
    same fingerprint and key still live: replace the slot
    else: install at the first tombstone, or at the first empty slot
  if the arena block must be reused:
    seal it, submit one 16 MiB WRITE, stall further SETs until it finishes
  if the disk window is full:
    the write overwrites the oldest disk block and those keys become misses

disk READ completion
  parse the item header in the read buffer
  if short: submit the tail read
  compare the key
  on match: writev the reply from the read buffer; free the buffer on send completion
  on mismatch: keep probing (rare; 64-bit fingerprint collision)
```

Memory hits never enter this queue. A connection with a disk GET in
flight can keep parsing. Later GETs on that connection are ordered behind
it, because the protocol has no request id and replies are matched by
order. SETs do not produce replies and can commit while a GET is waiting.
A SET of the same key updates the index; the GET already in flight returns
the location it captured when it probed.

### Index slot

Disk forces a wider slot. An 8-byte slot has room for a block id, a 24-bit
offset, and 12 bits of hash. Identifying a key from 12 bits is a 1-in-4096
filter, so every plausible disk GET would still read, and a SET of a key
that currently lives on disk cannot find its slot without reading the old
value. That turns SET into an asynchronous state machine and is the bug
`blocks_is_mem` would cause if it were used as a "free slot" test.

Use 16 bytes:

```c
struct slot {
    uint64_t loc;   /* high 32: block sequence. low 32: byte offset. */
    uint64_t fp;    /* wyhash of the key. loc == 0 means empty. */
};
```

Block sequences start at 1, so `loc == 0` is the empty marker even when
`fp` would have been 0. Offset needs 24 bits for a 16 MiB block. Bit 24 of
the low half is free and is the clock bit described under promotion.

Probe:

- `loc == 0` ends the probe (true empty).
- A sequence outside the live window is a tombstone. Skip it on find.
  Remember the first one on insert.
- `fp` equal to `wyhash(key)` is a candidate. If the block is in the
  arena, memcmp the key and return. If it is on disk, this is the only
  slot that should generate a read.
- Insert walks until a true empty, replaces a candidate in place, and
  otherwise fills the first tombstone. Replacing does not increment
  `ht->size`. Installing does.

The full-hash compare is what keeps SET off the disk. The key memcmp after
a memory hit, and after a disk read, still decides identity. A 64-bit
wyhash collision across 100 million keys is unlikely and is rejected by
the memcmp.

Cost: index RAM roughly doubles versus today's 8-byte slots, to about 23
bytes per live key at 70% fill. For the 100 million key example that is
about 2.3 GB instead of about 1.1 GB. Still small next to a terabyte of
values, and still well under a pointer-heavy hash table. Keeping the
8-byte slot was considered and rejected because it pushes disk I/O into
the insert probe.

Tombstones do not end a probe, so a cluster can get long after a wave of
evictions. `ht->size` stays honest (below), and insert reuses the first
tombstone, so the steady-state empty fraction remains about 30%. If the
longest probe crosses a fixed cap (1024 is plenty at 70% fill), drop one
more arena block and retry. Do not scan the whole table on the GET path.

`ht->size` has to count disk items too. `items_in_block[]` is only as long
as the arena. Add a second array, `items_on_disk[]`, with one counter per
disk block (~256 KB for a 1 TB budget). Overwriting a disk-resident key
decrements that counter. Sealing a block copies its arena count into the
disk counter for that sequence. Dropping a disk block subtracts its
counter from `ht->size` in constant time. A background scrub that walks a
few thousand index slots per loop turn can repair drift; it is not on the
miss path.

Sequence comparisons use wrapping subtraction, not `<`:

```c
/* true if a is in [tail, head], for a window much smaller than 2^31 */
static inline bool seq_in(uint32_t a, uint32_t tail, uint32_t head) {
    return (uint32_t)(a - tail) <= (uint32_t)(head - tail);
}
```

A 32-bit sequence at 16 MiB per block wraps after 64 PiB of writes. The
live window for any realistic disk budget is far below 2^31 blocks, which
is what the unsigned trick requires. Raw `<` against `min_block` goes
away.

### Files

One preallocated file, not a rotating set of append-only files. The sketch
uses 64 blocks per file, `O_APPEND`, and an offset that does not wrap with
the file. A single file avoids that:

- `fallocate` the whole budget at startup so the first pass does not
  interleave allocation with eviction
- one registered descriptor
- file offset of block sequence `s` is `(s % disk_blocks) * (1 << 24)`
- the newest `num_blocks` sequences are in the arena and are not in the
  file yet
- sealing block `s` writes it over the file slot that held `s - disk_blocks`,
  which has just left the live window

Open `O_RDWR`, never `O_APPEND`. Create the directory on startup. If
`--max-disk` is 0, do not create files and do not widen the on-disk
behavior; the 16-byte slot can still be used in memory-only mode so the
index code has one layout.

Item bytes on disk are the same packed `item` as in the arena, including
the key after the value. A hit reply is still `buf + 2` for `size + 4`
bytes. The key compare uses `buf + 6 + size`.

The first read is 32 KiB or the remainder of the block, whichever is
smaller. If the header says the item is larger, a second read fetches the
tail. Most of the 10 KiB workload finishes in one device round trip.
Values near the 16 MiB block cap take two. The size is not stored in the
index on purpose: a 32-bit fingerprint collides too often to replace
wyhash, and spending another 8 bytes per slot to hold a length is a poor
trade against a rare second read.

### Block lifecycle

1. `blocks_alloc` appends into the open arena block, same as today.
2. When the next item does not fit, the block is sealed. It is immutable
   from this point. No item is split across blocks; an item bigger than
   the block is rejected and the connection is closed.
3. Submit one `WRITE` of 16 MiB at the file offset for that sequence. The
   buffer is the arena slot. Do not malloc a second copy.
4. The slot stays pinned until the CQE says all 16 MiB landed. Short writes
   resubmit the tail. `blocks_alloc` that needs this slot stalls SETs.
   Memory GETs continue. One spare arena slot (allocate `num_blocks + 1`
   worth of sequences of slack, or simply refuse to advance `min_block`
   until the write of the oldest slot completes) absorbs a single
   in-flight write so a fast device does not stall every seal.
5. On a hard write error, the block is treated as evicted, not as on disk.
   Keys in it become misses. The process stays up. The error counter
   increments.
6. Advancing the disk tail drops `items_on_disk` for the overwritten
   sequence and clears nothing inline; those index slots become tombstones
   because their sequence fails `seq_in`.

The 16 MiB memcpy in `blocks_fs_write` would run on the event thread at
every seal. At a few milliseconds per copy, that is a periodic pause of
every connection. Skipping the copy is the reason to pin the slot instead.

If the device cannot absorb the ingest rate, SET latency grows until the
spare slot is exhausted and SETs wait for a CQE. That is the backpressure
signal. There is no silent drop of acknowledged data, because SET has no
ack anyway; the bytes are in the arena until the write fails, at which
point they are dropped as they would have been with `--max-disk 0`.

### Connection state for disk GETs

`getq_item_t` is the right idea and the wrong shape (three fixed reads,
no ordering, no buffer pool). Per connection:

- `inflight` — the GET waiting on a read, or NULL
- a small FIFO of parsed commands that have replies and must wait behind
  `inflight`

Dispatch:

- Memory GET or miss with an empty queue: gather the reply now.
- Disk candidate, nothing in flight: take a read buffer, submit `READ`,
  store the request as `inflight`. If the pool is empty, queue the
  request and leave it unsubmitted.
- Anything with a reply while `inflight` is set: append to the FIFO. Do
  not gather yet.
- Read CQE: finish the probe. On hit, gather a `writev` that points into
  the read buffer and set `free` so the buffer returns to the pool on the
  send CQE. On miss, gather the four zero bytes and return the buffer
  immediately. Then start the next FIFO entry, which may itself be a disk
  GET.

One in-flight disk read per connection is enough. More would reorder
replies or require buffering them anyway. The global cap is the read
buffer pool.

Cancel path, which the server does not have today: on recv `res == 0` or
a negative recv, cancel the disk read and drop the FIFO before freeing the
connection. A CQE that arrives after free is a use after free. The
`uring_op` for the read holds a generation or a pointer the cancel path
nulls out.

### io_uring integration

One ring, the one in `net.c`. Tag every completion with an explicit
operation kind. Stop using `IORING_CQE_F_MORE` as the recv-versus-write
test. Enable `IORING_SETUP_DEFER_TASKRUN` (the ring is already
`SINGLE_ISSUER`) and wait on the CQ when there is nothing to reap, so a
disk completion is observed even when no socket write is being submitted.
Details and the forbidden patterns are in `docs/io_uring.md`.

Reads and writes use `IORING_OP_READ` and `IORING_OP_WRITE` with a file
offset. Registered files are a straightforward follow-up. Registered
buffers and `WRITE_FIXED` wait until `RLIMIT_MEMLOCK` is raised to cover
the arena; they are an optimization, not a requirement. `O_DIRECT` and a
second `IOPOLL` ring are not part of this design. Buffered I/O keeps
recently sealed blocks in the page cache, which is the latency behavior
we want for items that just fell out of the arena. The cost is that the
kernel holds another copy until reclaim, so the machine's RAM use is
larger than `--max-memory` plus the index plus the 512 MiB recv pool.
Operators should size for that, or accept that a memory cgroup will turn
"buffered" into "device latency" under pressure.

No `fsync` on the write path. No `IOSQE_IO_DRAIN`. No link between the
item read and the socket write.

### Promotion

FIFO is the wrong hit-rate policy for a skewed keyspace, and it is the
right write policy for an SSD. Keep FIFO allocation. Add promotion as a
second milestone, behind a flag that defaults on:

- In the arena, a GET whose block is within the oldest quarter of live
  blocks (the generalization of `blocks_is_near_lru`) copies the item to
  the head and updates the slot. The old copy dies with the old block.
- On a disk hit, a clock bit (bit 24 of the offset word) records the first
  hit. The second hit copies the item to the arena head and clears the
  bit. A one-hit wonder does not enter the arena.

Promotion turns a scanning workload into extra arena writes, which evict
other keys faster. The clock bit is the mitigation. Workloads that really
are queues can disable the flag and keep a pure FIFO.

Doing this on the second milestone keeps the first milestone's GET path
easy to test: a disk hit returns the bytes and does not allocate.

### Failure and crash

| Event | Behavior |
| --- | --- |
| Process crash, `SIGKILL`, power loss | Index is gone. Next start is an empty cache. Leftover files are truncated or replaced. |
| Short write | Resubmit the remainder. The arena slot stays pinned. |
| `EIO`, `ENOSPC` on a sealed write | Drop that block as if it had been evicted. Count it. Keep serving. |
| Short or failed item read | Reply as a miss if the retry fails, count it, release the buffer. |
| Disk slower than ingest | SETs stall when the spare arena slot is exhausted. GETs of memory keys still run. |
| Client disconnects mid-read | Cancel the read, drop queued replies, do not write to the fd. |

There is no repair of a torn 16 MiB write because nothing reads the file
after a restart.

### Milestones

**A. Correct capacity tier.** 16-byte slots, sequence arithmetic, one
preallocated file, sealed 16 MiB writes with the slot pinned until the
CQE, 32 KiB item reads with a tail read when needed, per-connection reply
ordering, disk-aware insert and replace, `items_on_disk` counts, explicit
uring operation tags, `DEFER_TASKRUN`, and the counters above. No fsync,
no promotion, no `O_DIRECT`.

**B. Hit rate.** Arena promotion for near-LRU blocks, and two-hit
promotion from disk, with a flag to disable both.

**C. Fewer copies and fewer lookups.** Register the data file. Optionally
`WRITE_FIXED` on the arena once memlock is set. Replace the busy
classification in `net.c` with the wait loop from `docs/io_uring.md` if
milestone A has not already done so.

**D. Warm restart, only if a real caller needs it.** This is a different
product. It needs an index checkpoint, `fsync` of the data file before the
checkpoint is published, a generation number so a torn checkpoint is
ignored, and a rule for dirty arena blocks that were not sealed. Until
that exists, documentation and the startup log should say the disk tier
does not survive restart.

### What stays memory-only

`--max-disk 0` keeps today's eviction: sealing a block drops it. The
index can still move to 16-byte slots in that mode so there is one probe
implementation. Default index sizing stays 10% of `max_memory`. When disk
is enabled, the startup log should print the maximum live items
(`index_bytes / 16 * 0.7`) and warn if `disk_bytes / assumed_value_size`
would exceed it. The server cannot know the value size; the warning can
use 1 KiB as a conservative default and the README can keep the 10 KiB
example.

### Features, benefits, tradeoffs

**Features**

- Values live past arena eviction, up to `--max-disk`, while the process
  stays up.
- The hot path for a memory hit is unchanged: one probe, one `writev`
  into the arena.
- Writes to disk are sequential 16 MiB chunks. Reads are small and
  positioned, issued from the same ring as the sockets.
- The client protocol does not change. Compression keeps working and
  applies to the bytes on disk.
- SET of an existing key updates the index without reading the old value.
- Eviction, write failure, and a full disk degrade to misses, which is
  what the cache already does when the arena wraps.
- Operators can leave disk off and get the current server.

**Benefits**

- Capacity for large values without sizing RAM to the whole keyspace. A
  16 GB arena plus a 1 TB NVMe holds the 10 KiB, 100 million key example
  that does not fit in RAM, with the index as the RAM cost that remains.
- Sequential eviction matches SSD write patterns better than a random
  write-back cache would.
- Buffered writes mean a key that just missed the arena is often still in
  the page cache, so the common "just evicted" GET is a copy, not a device
  round trip.
- Backpressure is explicit. A slow disk stalls SETs instead of unbounded
  buffering.
- Crash behavior stays simple because durability is out of scope.

**Tradeoffs**

- Cold GET latency moves from a memory probe (the 18.6 Mops regime) to a
  device round trip. On NVMe that is tens of microseconds and up; it is
  100x to 1000x slower than a memory hit, and it occupies a connection
  slot until the CQE. One slow key head-of-line blocks later replies on
  that connection.
- Index RAM tracks key count, not value bytes. Disk does not make tiny
  values cheap. The 16-byte slot makes the index about twice as big as
  today.
- Every byte that survives to eviction is written again. Ingest at
  200 MB/s is 200 MB/s of SSD writes, plus promotion traffic if milestone B
  is on. Endurance has to be planned. Overwritten and never-read items
  still occupy their block until the block is sealed and later
  overwritten, so the disk holds garbage.
- FIFO without promotion has a weak hit rate on skewed workloads: a hot
  key dies with its block. Promotion fixes the hit rate and adds write
  amplification and the risk of cache pollution. The clock bit and the
  off switch are there because neither default is right for every caller.
- Buffered I/O double-books RAM in the page cache. `O_DIRECT` would make
  the budget honest and would make the "just evicted" case slower and more
  complicated (aligned bounce buffers, no `IOPOLL` on the network ring).
  The design picks the page cache and asks the operator to size for it.
- The event loop must grow real operation tags, a CQ wait, and cancel on
  disconnect. Those are bugfixes the disk tier depends on; they are also
  risk in the memory-only path and should land with tests before the file
  writes do.
- Nothing on disk survives restart. Anyone who reads "stored to disk" as
  "safe on disk" will be wrong. The startup banner has to say so.
- A single thread still does all compression, memcpy, and CQ processing.
  Disk I/O is off-thread inside the kernel, but a seal, a promotion copy,
  or a `GETZ` decompress still pauses the loop. Large `GETZ` values become
  more visible once a disk read has already been paid for.

### Testing notes

The open list in `todo` maps onto milestone A:

- block sequence wrap, using a small sequence space in a unit test rather
  than 64 PiB of writes
- a sealed-block write that completes, a short write that is retried, and
  a failed write that becomes a miss
- a read of an item just sealed, a read that needs a tail, and a read of
  a block that has since been overwritten
- insert that replaces a disk-resident key and does not leave a second
  live slot
- a block that is exactly full, and an item that does not fit
- fuzz of pipelined GET and SET with a disk budget of a few blocks, in
  the style of `tests/traffic.py`

Milestone A is done when those hold with `--max-disk` set and when the
same tests pass with `--max-disk 0`.
