# Mrcache

Mrcache is a key value cache with support for compression that is 25x faster than memcached.  Mrcache focuses on speed and limiting overhead to <20B per item (vs 60 for redis) which results in a few limitations described below.  It also requires linux kernel 5.5+ as it uses io_uring to hit the benchmark numbers below.

# Benchmarks

```
GET - 16B (ops/sec)

  mrcache        18.6m 
  redis           2.5m
  memcached       400k

GET - 10kb (ops/sec)

  mrcache         979k
  mrcache (zstd)  212k
  memcached       261k
  redis           313k

```

# Dependencies

* Linux kernel version 5.5+
* https://github.com/MarkReedZ/mrloop - io_uring based C event loop
* [Python asyncio client](https://github.com/MarkReedZ/asyncmrcache)
* [zstd compression](https://github.com/facebook/zstd)


# Usage

Use `make` to build and `mrcache` to run

```
Mrcache Version 0.1
    -h, --help                    This help
    -p, --port=<num>              TCP port to listen on (default: 7000)
    -m, --max-memory=<mb>         Maximum amount of memory in mb (default: 256)
    -i, --index-size=<mb>         Index size in mb (must be a power of 2 and sz/14 is the max number of items)
    -z, --zstd                    Enable zstd compression 
```

# Clients

[Python asyncio](https://github.com/MarkReedZ/asyncmrcache)
```
# pip install asyncmrcache

    rc = await asyncmrcache.create_client("localhost", loop)
    await rc.set(b"key", b"value")
    print( await rc.get(b"key") )

```

# Limitations

Key size is limited to < 32kb and values are limited to < 16mb due to the focus on maximizing speed and limiting per item memory overhead. 

You must also specify the size of your index as the index is allocated at startup and does not grow or shrink.  Index overhead is ~14 bytes meaning if you allocate 16mb to the index you can store a little over a million items before the cache becomes full.  If you have 4gb of memory and 1TB of disk space with 10kb items to store then you can fit 100 million items on disk and would need to allocate 1.4gb to the index.

# Internals

Mrcache has an item overhead of 18 bytes vs 50 to 60 for redis and memcached.  This is achieved by using an open addressing hash table and writing items into 16mb blocks in memory.  Instead of using pointers we store the block number and offset in order to retrieve the item which saves space allowing us to pack more information into the 8 bytes stored in the index hash table.  

The open addressing hash table is an in memory index for all of our items.  When hash collisions occur instead of having a linked list of items for that 'bucket' we increment the hash value and store our item in the first open spot.  To retrieve an item we compare our key against the item's key and increment the hash until we find our item or an empty spot in the index.  On average 2 items will be tested for each get if the cache is full.  For items stored on disk we keep the last byte of the key in the index to avoid unnecessary reads to disk for the key comparisons. 

Mrcache is a FIFO cache.  We store items into 16mb blocks in memory and when memory is full we drop the oldest block.  If you have 16gb of memory and are writing 200mb per second then you'll evict 1/16th of the cache every 80 seconds.  This can be made to be a pseudo LRU by rewriting soon to be evicted items at the front on a get. 

When a block is dropped the evicted items remain in our index, however when we scan the index we check the block number and treat evicted items as free spaces in the index.  The index is allocated at startup to reduce the complexity of the code mainly due to the intention of supporting items on disk. 




