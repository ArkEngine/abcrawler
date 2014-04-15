#include <stdio.h>
#include <stdint.h>
#include "membitmap.h"

MemBitMap::MemBitMap(const int max_size)
{
    buff = new uint32_t[max_size];
    for (int i = 0; i < max_size; i++) {
        buff[i] = 0;
    }
}

MemBitMap::~MemBitMap()
{
    if (buff != NULL)
        delete[] buff;
}

void MemBitMap::set(const int i)
{
    buff[i/UINT_BIT] |= (1<<i%UINT_BIT);
}

void MemBitMap::clear(const int i)
{
    buff[i/UINT_BIT] &= ~(1<<i%UINT_BIT);
}

bool MemBitMap::check(const int i)
{
    return (buff[i/UINT_BIT] & (1<<i%UINT_BIT));
}
