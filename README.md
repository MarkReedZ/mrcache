# Mrcache

Mrcache is a key value store with support for compression and disk.  Mrcache focuses on speed and limiting overhead to <20B per item which results in several limitations: the max size for a key is 32kb and value is 2mb.  It also requires linux kernel 5.4+ as it uses io_uring which is 2-4x faster than epoll. 

# Benchmarks

```
GET - 16B

pipelining
  mrcache         2.7m
  redis           1.0m
  memcached       700k

no pipelining
  mrcache (io_uring)  215k
  redis               112k
  mrcache (epoll)     100k

GET - 10kb

  mrcache        250k
  mrcache (zstd)  70k
  memcached       38k
  redis           65k

```

# Dependencies

* Linux kernel version 5.5+
* https://github.com/MarkReedZ/mrloop - io_uring based C event loop
* [Python asyncio client](https://github.com/MarkReedZ/asyncmrcache)


# Usage

Use `make` to build and `mrcache` to run

```
Mrcache Version 0.1
    -h, --help                    This help
    -p, --port=<num>              TCP port to listen on (default: 7000)
    -m, --max-memory=<mb>         Maximum amount of memory in mb (default: 256)
    -d, --max-disk=<gb>           Maximum amount of disk in gb (default: 1)
    -i, --index-size=<mb>         Index size in mb (must be a power of 2 and sz/14 is the max number of items)
    -z, --zstd                    Enable zstd compression 
```

The following command line will start up with 16mb of memory, 1gb of disk, and 1mb of index while using zstd compression for the stored values.

```
./mrcache -m 16 -d 1 -i 1 -z
```

If you don't specify the disk size then Mrcache will operate as an in memory cache.

```
./mrcache -m 1024 
```

# Clients

[Python asyncio](https://github.com/MarkReedZ/asyncmrcache)
```
# pip install asyncmrcache

    rc = await asyncmrcache.create_client("localhost", loop)
    await rc.set(b"key", b"value")
    print( await rc.get(b"key") )

```

# TODO

- [Zstandard](https://facebook.github.io/zstd/) offers various compression levels and allows you to train a dictionary. We need to support both and could provide an example JSON trained dictionary.
- We only support get and set.  
- We don't support TTL
- Need to track more stats and return them on a STATS cmd

# Limitations

Key size is limited to <32kb and values are limited to <2mb due to the focus on maximizing speed and limiting per item memory overhead. 

You must also specify the size of your index as the index is allocated at startup and does not grow or shrink.  Index overhead is ~14 bytes meaning if you allocate 16mb to the index you can store a little over a million items before the cache becomes full.  If you have 4gb of memory and 1TB of disk space with 10kb items to store then you can fit 100 million items on disk and would need to allocate 1.4gb to the index.

# Internals

Mrcache has an item overhead of 18 bytes vs 50 to 60 for redis and memcached.  This is achieved by using an open addressing hash table and writing items into 2mb blocks in memory.  Instead of using pointers we store the block number and offset in order to retrieve the item which saves space allowing us to pack more information into the 8 bytes stored in the index hash table.  

The open addressing hash table is an in memory index for all of our items.  When hash collisions occur instead of having a linked list of items for that 'bucket' we increment the hash value and store our item in the first open spot.  To retrieve an item we compare our key against the item's key and increment the hash until we find our item or an empty spot in the index.  On average 2 items will be tested for each get if the cache is full.  For items stored on disk we keep the last byte of the key in the index to avoid unnecessary reads to disk for the key comparisons. 

Mrcache stores items into 2mb blocks in memory and as memory gets full these blocks are written to disk and when the disk is full the oldest blocks are deleted.  To maintain itself as an LRU cache when you do a get items will be copied into the youngest block in memory if their block is close to being moved to disk or deleted. 

When a block is LRU'd out of the cache we simply free the block or truncate the block on disk for reuse.  This means LRU'd items remain in our index, however when we scan the index we check the block number and treat LRU'd items as free spaces in the index.

The index is allocated at startup to reduce the complexity of the code.  Mainly due to having items on disk where we'd have to do disk IO to read and hash the keys to insert the item into the new hash table.

















