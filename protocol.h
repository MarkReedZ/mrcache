#pragma once

#include <stdint.h>

/* Little-endian binary protocol. One outstanding reply per command, in order.
 *
 * Request:
 *   u8  version   = 1
 *   u8  op
 *   u16 key_len
 *   u32 val_len          SET: value length. Every other op: 0
 *   u8  key[key_len]
 *   u8  val[val_len]     SET only
 *
 * GET reply:  u8 status, u32 val_len, u8 val[val_len]
 *   status 0 = hit (val_len may be 0), 1 = miss (val_len is 0)
 * SET reply:  u8 status   0 stored, 2 value does not fit in a block
 * DEL reply:  u8 status   0 deleted, 1 not found
 * PING reply: u8 0        key_len and val_len must be 0
 * STAT reply: u8 0, then seven u64 counters:
 *   gets, hits, misses, sets, dels, evicts, live_keys
 */

#define MRC_VERSION 1

enum mrc_op {
    MRC_GET  = 1,
    MRC_SET  = 2,
    MRC_DEL  = 3,
    MRC_PING = 4,
    MRC_STAT = 5
};

enum mrc_status {
    MRC_OK       = 0,
    MRC_NOTFOUND = 1,
    MRC_TOO_BIG  = 2
};

#define MRC_HDR 8

static inline uint32_t mrc_item_bytes(uint16_t klen, uint32_t vlen) {
    return 5u + vlen + 2u + klen; /* status, vlen, value, klen, key */
}
