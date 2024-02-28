
#pragma once

#include "mrloop.h"
#include "common.h"

enum cmds {
  GET = 1,
  SET,
  GETZ,
  SETZ,
  STAT,
  CMDS_END
};

typedef struct _conn my_conn_t;

typedef struct __attribute__((__packed__)) _item {
  uint16_t keysize;
  uint32_t size;
  char data[];
} item;

typedef struct _disk_item_t {
  struct iovec iov;
  void *qi; 
  my_conn_t *conn;
} disk_item_t;

typedef struct _getq_item_t {
  item *item;
  char *buf;
  int cur, sz;
  uint64_t block;
  void *next;

  int type; // TODO use this and share pointers
  // Disk reads
  disk_item_t disk_items[3];
  int num_reads, reads_done;
  char *key;
  int keylen;

} getq_item_t;




