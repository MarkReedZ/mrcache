#include "store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

static void expect(int cond, const char *msg) {
    if (!cond) {
        fprintf(stderr, "FAIL %s\n", msg);
        failures++;
    }
}

static void set_ok(store_t *s, const char *k, const char *v) {
    int rc = store_set(s, (const uint8_t *)k, (uint16_t)strlen(k),
                       (const uint8_t *)v, (uint32_t)strlen(v));
    if (rc != STORE_OK) {
        fprintf(stderr, "FAIL set %s rc %d\n", k, rc);
        failures++;
    }
}

static void get_is(store_t *s, const char *k, const char *v) {
    uint32_t rlen = 0, slot = 0, vlen = 0;
    uint8_t *p = store_get(s, (const uint8_t *)k, (uint16_t)strlen(k), &rlen, &slot);
    if (!p) {
        fprintf(stderr, "FAIL miss %s\n", k);
        failures++;
        return;
    }
    memcpy(&vlen, p + 1, 4);
    if (vlen != strlen(v) || memcmp(p + 5, v, vlen) != 0) {
        fprintf(stderr, "FAIL value %s\n", k);
        failures++;
    }
}

static void get_miss(store_t *s, const char *k) {
    uint32_t rlen, slot;
    uint8_t *p = store_get(s, (const uint8_t *)k, (uint16_t)strlen(k), &rlen, &slot);
    if (p) {
        fprintf(stderr, "FAIL expected miss %s\n", k);
        failures++;
    }
}

static void test_basic(void) {
    store_t *s = NULL;
    uint8_t *p;
    uint32_t rlen, slot, vlen;
    expect(store_init(&s, 64 * 1024, 4096, 64 * 1024) == 0, "init");
    set_ok(s, "a", "alpha");
    set_ok(s, "b", "");
    get_is(s, "a", "alpha");
    get_is(s, "b", "");
    get_miss(s, "c");
    set_ok(s, "a", "beta");
    get_is(s, "a", "beta");
    expect(store_del(s, (const uint8_t *)"a", 1) == 0, "del");
    get_miss(s, "a");
    get_is(s, "b", "");
    set_ok(s, "a", "again");
    get_is(s, "a", "again");

    p = store_get(s, (const uint8_t *)"a", 1, &rlen, &slot);
    expect(p != NULL, "pin target");
    memcpy(&vlen, p + 1, 4);
    expect(rlen == 5 + vlen, "reply covers status and value");
    store_free(s);
}

static void get_n(store_t *s, const char *k, const uint8_t *v, uint32_t n) {
    uint32_t rlen = 0, slot = 0, vlen = 0;
    uint8_t *p = store_get(s, (const uint8_t *)k, (uint16_t)strlen(k), &rlen, &slot);
    if (!p) {
        fprintf(stderr, "FAIL miss %s\n", k);
        failures++;
        return;
    }
    memcpy(&vlen, p + 1, 4);
    if (vlen != n || memcmp(p + 5, v, n) != 0) {
        fprintf(stderr, "FAIL value %s\n", k);
        failures++;
    }
}

static void test_evict(void) {
    store_t *s = NULL;
    char key[16];
    uint8_t val[3000];
    int i;
    memset(val, 'x', sizeof val);
    /* 4 blocks of 8 KiB. A 3000-byte value occupies most of a block. */
    expect(store_init(&s, 32 * 1024, 8192, 1024 * 1024) == 0, "init evict");
    for (i = 0; i < 20; i++) {
        snprintf(key, sizeof key, "k%02d", i);
        expect(store_set(s, (const uint8_t *)key, (uint16_t)strlen(key), val, sizeof val) == 0, key);
    }
    get_n(s, "k19", val, sizeof val);
    get_miss(s, "k00");
    {
        struct store_stats st;
        store_stat(s, &st);
        expect(st.live < 20, "eviction reduced live keys");
        expect(st.evicts > 0, "evict counter");
        expect(st.hits >= 1 && st.misses >= 1, "hit and miss counted");
    }
    store_free(s);
}

static void test_pin_blocks_evict(void) {
    store_t *s = NULL;
    uint8_t big[3000];
    uint32_t rlen, slot;
    uint8_t *p;
    int rc;
    memset(big, 'z', sizeof big);
    expect(store_init(&s, 4096, 4096, 64 * 1024) == 0, "init pin");
    expect(store_set(s, (const uint8_t *)"A", 1, big, 2000) == STORE_OK, "set A");
    p = store_get(s, (const uint8_t *)"A", 1, &rlen, &slot);
    expect(p != NULL, "get A");
    store_pin(s, slot);
    rc = store_set(s, (const uint8_t *)"B", 1, big, 3000);
    expect(rc == STORE_BLOCKED, "pinned block is not dropped");
    store_unpin(s, slot);
    expect(store_set(s, (const uint8_t *)"B", 1, big, 3000) == 0, "set B after unpin");
    get_miss(s, "A");
    p = store_get(s, (const uint8_t *)"B", 1, &rlen, &slot);
    expect(p != NULL, "get B");
    store_free(s);
}

static void test_index_pressure(void) {
    store_t *s = NULL;
    char key[16];
    int i;
    /* 16 slots, max_live = 11. Arena is large so the index evicts first. */
    expect(store_init(&s, 256 * 1024, 4096, 128) == 0, "init index");
    for (i = 0; i < 40; i++) {
        snprintf(key, sizeof key, "i%02d", i);
        set_ok(s, key, "v");
    }
    get_is(s, "i39", "v");
    {
        struct store_stats st;
        store_stat(s, &st);
        expect(st.live <= 11, "index cap held");
    }
    store_free(s);
}

static void test_churn(void) {
    store_t *s = NULL;
    uint8_t key[8], val[64];
    uint8_t shadow[64][64];
    uint8_t slen[64];
    uint8_t present[64];
    uint32_t rng = 1;
    int n;
    memset(present, 0, sizeof present);
    expect(store_init(&s, 1024 * 1024, 64 * 1024, 1024 * 1024) == 0, "init churn");
    for (n = 0; n < 20000; n++) {
        uint32_t op, id, rlen, slot, vlen;
        uint8_t *p;
        rng = rng * 1664525u + 1013904223u;
        id = rng % 64;
        op = (rng >> 16) % 10;
        key[0] = (uint8_t)id;
        memset(key + 1, 0xab, 7);
        if (op < 6) {
            uint32_t len = (rng >> 8) % 64;
            memset(val, (uint8_t)(rng), len);
            expect(store_set(s, key, 8, val, len) == 0, "churn set");
            memcpy(shadow[id], val, len);
            slen[id] = (uint8_t)len;
            present[id] = 1;
        } else if (op < 8) {
            store_del(s, key, 8);
            present[id] = 0;
        } else {
            p = store_get(s, key, 8, &rlen, &slot);
            if (!present[id]) {
                expect(p == NULL, "churn miss");
            } else {
                expect(p != NULL, "churn hit");
                if (p) {
                    memcpy(&vlen, p + 1, 4);
                    expect(vlen == slen[id] && memcmp(p + 5, shadow[id], vlen) == 0,
                           "churn bytes");
                }
            }
        }
    }
    store_free(s);
}

static void test_too_big(void) {
    store_t *s = NULL;
    uint8_t *buf;
    expect(store_init(&s, 8192, 4096, 4096) == 0, "init big");
    buf = malloc(5000);
    memset(buf, 1, 5000);
    expect(store_set(s, (const uint8_t *)"k", 1, buf, 5000) == STORE_TOO_BIG, "too big");
    get_miss(s, "k");
    free(buf);
    store_free(s);
}

int main(void) {
    test_basic();
    test_evict();
    test_pin_blocks_evict();
    test_index_pressure();
    test_churn();
    test_too_big();
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("store_test ok\n");
    return 0;
}
