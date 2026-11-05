# mrcache

mrcache is an in-memory key-value cache. It keeps the per-key overhead small, serves GET replies straight out of the value arena, and drives the sockets with `io_uring`. It is a single thread, a FIFO of fixed-size blocks, and a binary protocol. It does not store to disk.

Linux 6.1 or newer and [liburing](https://github.com/axboe/liburing) are required. When the kernel allows it, a submission-queue polling thread submits I/O. That thread is a second core while the server is busy, and it goes idle two seconds after the last submission.

## Build

```
make
./mrcache -p 7000 -m 256
```

| Flag | Meaning |
| --- | --- |
| `-p` | TCP port (default 7000) |
| `-m` | Arena size in MiB (default 256) |
| `-M` | Arena size in bytes, instead of `-m` |
| `-i` | Index size in MiB (default about 10% of the arena, at least 1 MiB when the arena is that large) |
| `-B` | Block size in bytes, a power of two (default 16 MiB, or the arena if that is smaller) |

`make test` builds the server, runs the in-process store tests, and runs the Go protocol tests.

## Protocol

Integers are little-endian. Every command has one reply, in order.

```
request:  u8 version=1, u8 op, u16 key_len, u32 val_len, key, value
```

`val_len` and the value bytes are present only for `SET`. Other commands send `val_len = 0` and no value.

| Op | Code | Reply |
| --- | --- | --- |
| GET | 1 | `u8 status`, `u32 val_len`, value. Status 0 is a hit, including an empty value. Status 1 is a miss and `val_len` is 0. |
| SET | 2 | `u8 status`. 0 stored. 2 does not fit in a block. |
| DEL | 3 | `u8 status`. 0 deleted. 1 not found. |
| PING | 4 | `u8 0`. `key_len` and `val_len` are 0. |
| STAT | 5 | `u8 0`, then seven `u64` counters: gets, hits, misses, sets, dels, evicts, live keys. |

A key is at most 65535 bytes. A value has to fit in one block together with its header and key. Items are never split. An empty value is stored and returned as a hit.

## Layout

The index is an open-addressed table of 8-byte slots (block sequence and offset), filled to at most 70%. The arena is one allocation split into fixed blocks, default 16 MiB. Items are appended into the current block. When the arena or the index is full, the oldest block is dropped and the keys that lived there become misses. Replacing a key writes a new copy and leaves the old bytes until that block is dropped.

A GET reply is the first bytes of the item (`status`, `val_len`, value), so the socket write points into the arena. That block stays pinned until the write completes, which stops eviction from reusing it underneath an in-flight reply.

There is no TTL, no LRU, no persistence, and no replication. FIFO is the eviction policy because it keeps writes sequential in the arena and the index entry at 8 bytes.

## Benchmark

`bench/bench.go` speaks this protocol and Redis RESP. It checks the payload of every GET.

```
# mrcache, from the repo root, with the server already listening
cd bench && go run . -p 7000 -d 5 -c 16 -b 64 -k 16 -v 128 -keys 1000

# the same client against Redis
go run . -redis -p 6379 -d 5 -c 16 -b 64 -k 16 -v 128 -keys 1000 -op get
```

`-op set` measures writes. `-keys 1` is a single hot key. A larger `-keys` spreads the lookup.

## Where this fits next to Redis

Released Redis, through 8.4 (November 2025) and the 8.12 release candidates (November 2026), does not include an `io_uring` backend. `io_uring` support exists as open pull requests. What Redis did ship, starting in 8.0, is I/O threads on top of its epoll loop: extra threads read and write sockets while commands still run on one thread.

That difference is smaller than the data-structure difference. Redis parses RESP, looks up a `dict` entry, and follows an object header and an SDS string. A small string key costs on the order of a few dozen bytes of allocator and metadata overhead before the value. mrcache's steady-state overhead is the 7-byte item prefix plus one 8-byte index slot at a 70% fill, about 18 bytes on top of the key and the value, and the GET does not copy the value.

mrcache is the right tool when all of these are true: the working set is strings, losing the oldest block is acceptable, the process can be Linux-only, and one core should hold as many keys as possible and answer them with as little work as possible. It is the wrong tool when you need expiry, LRU, persistence, replication, multiple cores of command execution, or the Redis client ecosystem. Those features are why Redis is larger and slower per key, and they are also why it is the default.
