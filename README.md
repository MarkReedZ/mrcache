# mrcache
Mrcache is a key value store ala memcached with support for compression and disk.  It requires linux kernel 5.3+ as it uses io_uring which is 2-4x faster than epoll.

# Benchmarks

```
GET - 16B

pipelining
  mrcache (io_uring)  5.7m
  redis               1.3m
  memcached           700k

no pipelining
  mrcache (io_uring)  215k
  redis               112k
  mrcache (epoll)     100k

GET - 100kb

  mrcache (io_uring)   36k
  memcached            38k
  redis                10k

```

# Dependencies

* Linux kernel version 5.5+
* https://github.com/MarkReedZ/mrloop - io_uring based C event loop

# Usage

Use `make` to build and `mrcache` to run

```
Mrcache Version 0.1
    -h, --help                    This help
    -p, --port=<num>              TCP port to listen on (default: 7000)
    -m, --max-memory=<mb>         Maximum amount of memory in mb (default: 256)
    -d, --max-disk=<gb>           Maximum amount of disk in gb (default: 1)
    -i, --index-size=<mb>         Index size in mb (must be a power of 2 and sz/14 is the max number of items)
    -z, --zstd                    Enable zstd compression in memory
```

The following command line will start up with 16mb of memory, 1gb of disk, 1mb of index, and zstd compression

```
./mrcache -m 16 -d 1 -i 1 -z
```

# Clients

[Python asyncio](https://github.com/MarkReedZ/asyncmrcache)
```
# pip install asyncmrcache

    rc = await asyncmrcache.create_client("localhost", loop)
    await rc.set(b"key", b"value")
    print( await rc.get(b"key") )

```


