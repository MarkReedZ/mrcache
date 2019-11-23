
#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>


#define DBG if(0) 
#define DBG_READ if(0)
#define DBG_SET if(0)

#define likely(x)    __builtin_expect (!!(x), 1)
#define unlikely(x)  __builtin_expect (!!(x), 0)

// TODO why? LOL
typedef  uint32_t       u4b;
typedef  uint16_t       u2b;
typedef  unsigned  char ub;


