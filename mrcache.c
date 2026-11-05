#include "net.h"
#include "store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void usage(void) {
    fprintf(stderr,
            "mrcache — in-memory key value cache\n"
            "  -p port         listen port (default 7000)\n"
            "  -m megabytes    arena size in MiB (default 256)\n"
            "  -M bytes        arena size in bytes (overrides -m)\n"
            "  -i megabytes    index size in MiB (default ~10%% of the arena)\n"
            "  -B bytes        block size, power of two (default 16 MiB, or the arena)\n"
            "  -h              help\n");
}

static size_t pow2_floor(size_t n) {
    size_t p = 1;
    while ((p << 1) && (p << 1) <= n) p <<= 1;
    return p;
}

int main(int argc, char **argv) {
    size_t arena = 256ull << 20;
    size_t index_bytes = 0;
    size_t block = 0;
    int port = 7000;
    store_t *store = NULL;
    int i;

    for (i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
            usage();
            return 0;
        }
        if (i + 1 >= argc) { usage(); return 2; }
        if (!strcmp(argv[i], "-p")) port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "-m")) arena = (size_t)atoi(argv[++i]) << 20;
        else if (!strcmp(argv[i], "-M")) arena = (size_t)strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "-i")) index_bytes = (size_t)atoi(argv[++i]) << 20;
        else if (!strcmp(argv[i], "-B")) block = (size_t)strtoull(argv[++i], NULL, 10);
        else { usage(); return 2; }
    }
    if (port <= 0 || port > 65535 || arena < 4096) { usage(); return 2; }
    if (block == 0) {
        block = arena >= (16ull << 20) ? (16ull << 20) : pow2_floor(arena);
        if (block < 4096) block = 4096;
    }
    if (block > arena) block = pow2_floor(arena);
    if (index_bytes == 0) {
        index_bytes = arena / 10;
        if (index_bytes < (1ull << 20) && arena >= (1ull << 20)) index_bytes = 1ull << 20;
        if (index_bytes < 4096) index_bytes = 4096;
    }
    if (store_init(&store, arena, block, index_bytes) != 0) {
        fprintf(stderr, "failed to allocate arena (%zu bytes) or index (%zu bytes)\n",
                arena, index_bytes);
        return 1;
    }
    fprintf(stderr, "arena %zu bytes, block %zu, index %zu\n", arena, block, index_bytes);
    return net_run((struct net_config){ .port = port, .store = store });
}
