# Benchmark

`bench.go` is a pipelined GET/SET client. It speaks mrcache's binary protocol and Redis RESP (`-redis`), and it checks every GET body.

```
go run . -d 5 -c 16 -b 64 -k 16 -v 128 -keys 1000
go run . -redis -p 6379 -op get -d 5 -c 16 -b 64 -k 16 -v 128 -keys 1000
```

`go test` starts mrcache and, when `redis-server` is on `PATH`, checks the Redis parser against it.
