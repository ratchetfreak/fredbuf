#pragma once

#include <string.h>

#include "macros.h"

SUPPRESS_MEMSET_NON_TRIVIAL_WARNING();
template <typename T>
void zero_bytes(T* x, uint64_t count = 1)
{
    memset(x, 0, sizeof(T) * count);
}
ENABLE_MEMSET_NON_TRIVIAL_WARNING();