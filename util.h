#pragma once

#include <string.h>
#include <bit>

#include "macros.h"

SUPPRESS_MEMSET_NON_TRIVIAL_WARNING();
template <typename T>
void zero_bytes(T* x, uint64_t count = 1)
{
    memset(x, 0, sizeof(T) * count);
}
ENABLE_MEMSET_NON_TRIVIAL_WARNING();


template<typename It, typename T>
It branchless_lower_bound(It begin, It end, const T & value)
{
    size_t length = end - begin;
    if (length == 0)
        return end;
    size_t step = std::bit_floor(length);
    if (step != length && (begin[step] < value))
    {
        length -= step + 1;
        if (length == 0)
            return end;
        step = std::bit_ceil(length);
        begin = end - step;
    }
    for (step /= 2; step != 0; step /= 2)
    {
        if ((begin[step]< value))
            begin += step;
    }
    return begin + (*begin < value);
}