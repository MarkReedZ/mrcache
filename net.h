#pragma once

#include "store.h"

struct net_config {
    int port;
    store_t *store;
};

/* Runs until SIGINT or SIGTERM. */
int net_run(struct net_config cfg);
