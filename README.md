# mrcache
Mrcache is a key value store ala memcached with support for compression and using disk.  It requires linux kernel 5.3+ as it uses io_uring which is almost twice as fast as epoll.

# Benchmarks

```
GET - 10B

pipelining
  mrcache (iouring) 4.6m
  redis             1.2m
  memcached         700k

no pipelining
  mrcache (iouring) 215k
  redis             112k
  mrcache (epoll)   100k

GET - 100kb

  memcached          38k
  mrcache (iouring)  36k
  redis              10k

```
