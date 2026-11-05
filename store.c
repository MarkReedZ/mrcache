#include "store.h"
#include "protocol.h"
#include "wyhash.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Open-addressed index. Each slot is 8 bytes:
 *   empty    block == 0 && off == 0
 *   tombstone block == 0 && off != 0   (a deleted key; probe continues)
 *   occupied block  != 0               (off is the byte offset of the item)
 *
 * An occupied slot whose block sequence has fallen out of the arena is a
 * stale hole. Find skips it. Insert reuses it. Block sequences are monotonic
 * and the live window is tiny next to 2^32, so unsigned distance works.
 *
 * Items are packed into fixed blocks and dropped FIFO. A block with a
 * non-zero pin cannot be dropped: a socket reply still points into it.
 */

struct store {
    uint8_t *base;
    size_t block_size;
    uint32_t num_blocks;
    uint32_t min_block;
    uint32_t cur_block;
    uint32_t cur_off;
    uint32_t *counts;
    uint32_t *pins;

    struct {
        uint32_t block;
        uint32_t off;
    } *slots;
    uint32_t mask;
    uint32_t nslots;
    uint32_t live;
    uint32_t max_live;
    uint32_t used;

    uint64_t gets, hits, misses, sets, dels, evicts;
};

static int is_empty(uint32_t block, uint32_t off) {
    return block == 0 && off == 0;
}

static int block_live(const store_t *s, uint32_t block) {
    if (block == 0) return 0;
    return (uint32_t)(block - s->min_block) <= (uint32_t)(s->cur_block - s->min_block);
}

static uint8_t *item_ptr(const store_t *s, uint32_t block, uint32_t off) {
    uint32_t phys = block % s->num_blocks;
    return s->base + (size_t)phys * s->block_size + off;
}

static int key_eq(const store_t *s, uint32_t block, uint32_t off,
                  const uint8_t *key, uint16_t klen) {
    uint8_t *it = item_ptr(s, block, off);
    uint32_t vlen;
    uint16_t ik;
    memcpy(&vlen, it + 1, 4);
    memcpy(&ik, it + 5 + vlen, 2);
    if (ik != klen) return 0;
    return memcmp(it + 7 + vlen, key, klen) == 0;
}

static uint32_t pow2_floor(uint32_t n) {
    uint32_t p = 1;
    if (n == 0) return 1;
    while ((p << 1) && (p << 1) <= n) p <<= 1;
    return p;
}

int store_init(store_t **out, size_t arena_bytes, size_t block_size, size_t index_bytes) {
    store_t *s;
    uint32_t nslots;
    if (block_size < 1024 || (block_size & (block_size - 1)) != 0) return -1;
    if (arena_bytes < block_size) return -1;

    s = calloc(1, sizeof *s);
    if (!s) return -1;
    s->block_size = block_size;
    s->num_blocks = (uint32_t)(arena_bytes / block_size);
    if (s->num_blocks == 0) { free(s); return -1; }

    if (posix_memalign((void **)&s->base, 4096, (size_t)s->num_blocks * block_size) != 0) {
        free(s);
        return -1;
    }
    s->counts = calloc(s->num_blocks, sizeof(uint32_t));
    s->pins = calloc(s->num_blocks, sizeof(uint32_t));
    nslots = (uint32_t)(index_bytes / sizeof *s->slots);
    nslots = pow2_floor(nslots < 16 ? 16 : nslots);
    s->slots = calloc(nslots, sizeof *s->slots);
    if (!s->base || !s->counts || !s->pins || !s->slots) {
        store_free(s);
        return -1;
    }
    s->nslots = nslots;
    s->mask = nslots - 1;
    s->max_live = (uint32_t)((nslots * 7) / 10);
    if (s->max_live < 1) s->max_live = 1;
    s->min_block = 1;
    s->cur_block = 1;
    *out = s;
    return 0;
}

void store_free(store_t *s) {
    if (!s) return;
    free(s->base);
    free(s->counts);
    free(s->pins);
    free(s->slots);
    free(s);
}

uint32_t store_max_value(const store_t *s, uint16_t klen) {
    uint32_t overhead = mrc_item_bytes(klen, 0);
    if (overhead >= s->block_size) return 0;
    return (uint32_t)s->block_size - overhead;
}

size_t store_block_size(const store_t *s) { return s->block_size; }

void store_pin(store_t *s, uint32_t slot) {
    assert(slot < s->num_blocks);
    s->pins[slot]++;
}

void store_unpin(store_t *s, uint32_t slot) {
    assert(slot < s->num_blocks);
    assert(s->pins[slot] > 0);
    s->pins[slot]--;
}

void store_stat(const store_t *s, struct store_stats *o) {
    o->gets = s->gets;
    o->hits = s->hits;
    o->misses = s->misses;
    o->sets = s->sets;
    o->dels = s->dels;
    o->evicts = s->evicts;
    o->live = s->live;
}

/* Sequence 0 is the empty-slot marker, so it is never a real block. */
static uint32_t next_seq(uint32_t b) {
    b++;
    return b == 0 ? 1 : b;
}

static int evict_oldest(store_t *s) {
    uint32_t phys = s->min_block % s->num_blocks;
    if (s->pins[phys]) return -1;
    assert(s->counts[phys] <= s->live);
    s->live -= s->counts[phys];
    s->counts[phys] = 0;
    s->evicts++;
    if (s->min_block == s->cur_block) {
        /* The only live block was the one being written. Reopen it. */
        s->cur_block = next_seq(s->cur_block);
        s->min_block = s->cur_block;
        s->cur_off = 0;
    } else {
        s->min_block++;
    }
    return 0;
}

/* Make the open block large enough for `need` bytes. */
static int make_room(store_t *s, uint32_t need) {
    if (s->cur_off + need <= s->block_size) return 0;
    if ((uint32_t)(s->cur_block - s->min_block + 1) >= s->num_blocks) {
        if (evict_oldest(s) < 0) return STORE_BLOCKED;
    }
    if (s->cur_off + need <= s->block_size) return 0;
    s->cur_block = next_seq(s->cur_block);
    s->cur_off = 0;
    return 0;
}

static int alloc_item(store_t *s, const uint8_t *key, uint16_t klen,
                      const uint8_t *val, uint32_t vlen,
                      uint32_t *block, uint32_t *off) {
    uint32_t need = mrc_item_bytes(klen, vlen);
    uint8_t *p;
    int rc = make_room(s, need);
    if (rc < 0) return rc;
    *block = s->cur_block;
    *off = s->cur_off;
    p = item_ptr(s, *block, *off);
    p[0] = MRC_OK;
    memcpy(p + 1, &vlen, 4);
    if (vlen) memcpy(p + 5, val, vlen);
    memcpy(p + 5 + vlen, &klen, 2);
    if (klen) memcpy(p + 7 + vlen, key, klen);
    s->cur_off += need;
    s->counts[*block % s->num_blocks]++;
    return 0;
}

static void reinsert(store_t *s, uint32_t block, uint32_t off) {
    uint8_t *it = item_ptr(s, block, off);
    uint32_t vlen;
    uint16_t klen;
    uint64_t h;
    uint32_t i, step;
    memcpy(&vlen, it + 1, 4);
    memcpy(&klen, it + 5 + vlen, 2);
    h = wyhash(it + 7 + vlen, klen, 0, _wyp);
    i = (uint32_t)h & s->mask;
    for (step = 0; step <= s->mask; step++) {
        if (is_empty(s->slots[i].block, s->slots[i].off)) {
            s->slots[i].block = block;
            s->slots[i].off = off;
            s->used++;
            return;
        }
        i = (i + 1) & s->mask;
    }
}

static int rebuild_index(store_t *s) {
    typedef struct { uint32_t block, off; } slot_t;
    slot_t *old = (slot_t *)s->slots;
    slot_t *fresh;
    uint32_t i, n = s->nslots;
    fresh = calloc(n, sizeof *fresh);
    if (!fresh) return -1;
    s->slots = (void *)fresh;
    s->used = 0;
    for (i = 0; i < n; i++) {
        if (old[i].block != 0 && block_live(s, old[i].block))
            reinsert(s, old[i].block, old[i].off);
    }
    free(old);
    return 0;
}

/* found >= 0 on hit. hole is the first reusable slot. empty_idx is the
 * terminating empty slot when the key is absent. Returns 0 if the probe
 * got long enough that the caller should rebuild and retry. */
static int lookup(store_t *s, const uint8_t *key, uint16_t klen, uint64_t h,
                  int *found, int *hole, int *empty_idx) {
    uint32_t i = (uint32_t)h & s->mask;
    uint32_t step;
    *found = -1;
    *hole = -1;
    *empty_idx = -1;
    for (step = 0; step <= s->mask; step++) {
        uint32_t b = s->slots[i].block;
        uint32_t o = s->slots[i].off;
        if (is_empty(b, o)) {
            *empty_idx = (int)i;
            return 1;
        }
        if (b == 0 || !block_live(s, b)) {
            if (*hole < 0) *hole = (int)i;
        } else if (key_eq(s, b, o, key, klen)) {
            *found = (int)i;
            return 1;
        }
        if (step == 128 && s->used * 10 > s->nslots * 5)
            return 0;
        i = (i + 1) & s->mask;
    }
    return 0;
}

int store_set(store_t *s, const uint8_t *key, uint16_t klen,
              const uint8_t *val, uint32_t vlen) {
    uint64_t h;
    int attempt;
    if (klen == 0 || vlen > store_max_value(s, klen)) return STORE_TOO_BIG;
    h = wyhash(key, klen, 0, _wyp);

    for (attempt = 0; attempt < 4; attempt++) {
        int found, hole, empty_idx;
        uint32_t nb, no;
        int rc;
        if (!lookup(s, key, klen, h, &found, &hole, &empty_idx)) {
            if (rebuild_index(s) < 0) return STORE_BLOCKED;
            continue;
        }
        if (found >= 0) {
            uint32_t oldb = s->slots[found].block;
            rc = alloc_item(s, key, klen, val, vlen, &nb, &no);
            if (rc < 0) return rc;
            if (!block_live(s, oldb)) {
                s->live++;
            } else {
                uint32_t phys = oldb % s->num_blocks;
                assert(s->counts[phys] > 0);
                s->counts[phys]--;
            }
            s->slots[found].block = nb;
            s->slots[found].off = no;
            s->sets++;
            return STORE_OK;
        }

        while (s->live >= s->max_live) {
            if (evict_oldest(s) < 0) return STORE_BLOCKED;
        }
        rc = alloc_item(s, key, klen, val, vlen, &nb, &no);
        if (rc < 0) return rc;
        {
            int dest = hole >= 0 ? hole : empty_idx;
            int was_empty;
            assert(dest >= 0);
            /* Eviction can only turn live slots into holes. The slot we
             * picked is still a hole, but re-read it in case we should
             * count `used`. */
            was_empty = is_empty(s->slots[dest].block, s->slots[dest].off);
            assert(s->slots[dest].block == 0 || !block_live(s, s->slots[dest].block));
            s->slots[dest].block = nb;
            s->slots[dest].off = no;
            if (was_empty) s->used++;
            s->live++;
            s->sets++;
            return STORE_OK;
        }
    }
    return STORE_BLOCKED;
}

uint8_t *store_get(store_t *s, const uint8_t *key, uint16_t klen,
                   uint32_t *rlen, uint32_t *pin_slot) {
    uint64_t h = wyhash(key, klen, 0, _wyp);
    int found, hole, empty_idx;
    uint32_t vlen;
    uint8_t *it;
    s->gets++;
    if (!lookup(s, key, klen, h, &found, &hole, &empty_idx)) {
        if (rebuild_index(s) == 0)
            lookup(s, key, klen, h, &found, &hole, &empty_idx);
    }
    if (found < 0) {
        s->misses++;
        return NULL;
    }
    it = item_ptr(s, s->slots[found].block, s->slots[found].off);
    memcpy(&vlen, it + 1, 4);
    *rlen = 5u + vlen;
    *pin_slot = s->slots[found].block % s->num_blocks;
    s->hits++;
    return it;
}

int store_del(store_t *s, const uint8_t *key, uint16_t klen) {
    uint64_t h = wyhash(key, klen, 0, _wyp);
    int found, hole, empty_idx;
    uint32_t phys;
    if (!lookup(s, key, klen, h, &found, &hole, &empty_idx)) {
        if (rebuild_index(s) == 0)
            lookup(s, key, klen, h, &found, &hole, &empty_idx);
    }
    if (found < 0) return 1;
    phys = s->slots[found].block % s->num_blocks;
    assert(s->counts[phys] > 0);
    assert(s->live > 0);
    s->counts[phys]--;
    s->live--;
    s->slots[found].block = 0;
    s->slots[found].off = 1;
    s->dels++;
    return 0;
}
