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

template<typename T, class Compare>
T* SSLMerge(T* a,T* b, Compare cmp)
{
    T* res;
    T** tail = &res;
    while(a!=nullptr&&b!=nullptr)
    {
        if(cmp(a, b) < 0)
        {
            *tail = a;
            tail = &a->next;
            a = a->next;
            if(a == nullptr)
            {
                *tail = b;
                b = nullptr;
            }
        }
        else
        {
            *tail = b;
            tail = &b->next;
            b = b->next;
            if(b == nullptr)
            {
                *tail = a;
                a = nullptr;
            }
        }
    }
    if(a != nullptr)
    {
        *tail = a;
    }
    else
    {
        *tail = b;
    }
    return res;
}

template<typename T, class Compare>
T* SLLSort(T* start, Compare cmp)
{
    T* temp[64]{};
    
    T* p = start;
    int count = 0;
    while(p)
    {
        T* n = p;
        p = p->next;
        n->next = nullptr;
        
        int i = 0;
        for(; (count>>i)&1; i++)
        {
            T* t = temp[i];
            temp[i] = n;
            n = t;
        }
        if(temp[i] !=nullptr)
        {
            temp[i] = SLLMerge(temp[i], n, cmp);
        }
        else 
        {
            temp[i] = n;
        }
        count++;
    }
    T* result = nullptr;
    for EachIndex(i, 64)
    {
        if(temp[i] != nullptr)
        {
            if(result!= nullptr)
            {
                result = SLLMerge(temp[i], result, cmp);
            }
            else
            {
                result = temp[i];
            }
        }
    }
    return result;
}

template<typename It, typename T>
T* SLLtoDLL(T*first)
{
    T* last = first;
    for EachNode(n, first->next)
    {
        n->prev = last;
        last = n;
        
    }
    return last;
}

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