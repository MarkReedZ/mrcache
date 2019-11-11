# mrcache
Mrcache is a key value store ala memcached with support for compression and using disk.  It requires linux kernel 5.3+ as it uses io_uring which is almost twice as fast as epoll.

# Benchmarks

```
GET

pipelining
mrcache (iouring) 2.5m
redis             1.5m
memcached         700k

no pipelining
mrcache (iouring) 380k
mrcache (epoll)   200k
redis             240k

```
