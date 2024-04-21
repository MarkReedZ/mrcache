
#pragma once

#include "mrloop.h"

typedef struct _disk_item_t {
  struct iovec iov;
  void *qi;
} disk_item_t;


mr_loop_t *loop;

