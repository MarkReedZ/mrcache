CC = gcc
CFLAGS = -O3 -Wall -Wextra -Wno-unused-function
LIBS = -luring

.PHONY: all test clean bench

all: mrcache

mrcache: mrcache.c net.c store.c protocol.h store.h net.h wyhash.h
	$(CC) $(CFLAGS) -o mrcache mrcache.c net.c store.c $(LIBS)

store_test: store_test.c store.c store.h protocol.h wyhash.h
	$(CC) $(CFLAGS) -o store_test store_test.c store.c

test: mrcache store_test
	./store_test
	cd bench && go test -count=1 -timeout 180s

bench: mrcache
	cd bench && go run . -d 3 -c 8 -b 32 -k 16 -v 128

clean:
	rm -f mrcache store_test
