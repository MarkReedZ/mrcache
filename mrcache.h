
#pragma once
#include "common.h"
#include "hashtable.h"
#include "mrloop.h"

extern hashtable_t *mrq_ht;
extern hashtable_t *mrq_htnew;

enum cmds {
  GET = 1,
  SET,
  CMDS_END
};

struct settings {
  int port; 
  int max_memory; // mb
  int disk_size;  // mb
  uint32_t flags;
  mr_loop_t *loop;
};

#define ENABLE_COMPRESSION  settings.flags |= 0x1
#define COMPRESSION_ENABLED (settings.flags & 0x1)

extern struct settings settings;

typedef struct _conn my_conn_t;

typedef struct __attribute__((__packed__)) _item {
  uint16_t keysize;
  uint32_t size;
  char data[];
} item;

typedef struct _getq_item_t {
  item *item;
  char *buf;
  int cur, sz;
  uint64_t block;
  void *next;
} getq_item_t;

//static void flush();


//static void flush();

void can_write( void *conn, int fd );

