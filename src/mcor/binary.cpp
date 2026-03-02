//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <iostream>
#include <stdio.h>

#include <ctype.h>
#include <stdlib.h>

#include "binary.h"


namespace cor
{

uint16_t
Unpack16(const unsigned char** buffer)
{
    uint16_t r =
            (uint32_t)(*buffer)[0] << 8 |
            (uint32_t)(*buffer)[1];
    *buffer += 2;
    return r;
}

uint16_t
Unpack16(const unsigned char* buffer)
{
    const unsigned char* c = buffer;
    return Unpack16(&c);
}

uint32_t
Unpack32(const unsigned char** buffer)
{
    uint32_t r =
            (uint32_t)(*buffer)[0] << 24 |
            (uint32_t)(*buffer)[1] << 16 |
            (uint32_t)(*buffer)[2] << 8  |
            (uint32_t)(*buffer)[3];
    *buffer += 4;
    return r;
}

uint32_t
Unpack32(const unsigned char* buffer)
{
    const unsigned char* c = buffer;
    return Unpack32(&c);
}

uint64_t
Unpack64(const unsigned char** buffer)
{
    unsigned int shift = 56;
    uint64_t r = 0;
    for (size_t i = 0; i < 8; i++)
    {
        uint64_t c = (*buffer)[i];
        r = r | (uint64_t)(c << shift);
        shift -= 8;
    }

    *buffer += 8;
    return r;
}

uint64_t
Unpack64(const unsigned char* buffer)
{
    const unsigned char* c = buffer;
    return Unpack64(&c);
}

double
UnpackDouble(const unsigned char** buffer)
{
    uint64_t u = Unpack64(buffer);
    return *(double*)(&u);
}

double
UnpackDouble(const unsigned char* buffer)
{
    const unsigned char* c = buffer;
    uint64_t u = Unpack64(&c);
    return *(double*)(&u);
}

void
Pack16(unsigned char** buffer, uint16_t n)
{
    (*buffer)[0] = n >> 8;
    (*buffer)[1] = n;

    *buffer += 2;
}

void
Pack16(unsigned char* buffer, uint16_t n)
{
    unsigned char* c = buffer;
    Pack16(&c, n);
}

void
Pack32(unsigned char** buffer, uint32_t n)
{
    (*buffer)[0] = n >> 24;
    (*buffer)[1] = n >> 16;
    (*buffer)[2] = n >>  8;
    (*buffer)[3] = n;

    *buffer += 4;
}

void
Pack32(unsigned char* buffer, uint32_t n)
{
    unsigned char* c = buffer;
    Pack32(&c, n);
}

void
Pack64(unsigned char** buffer, uint64_t n)
{
    unsigned int shift = 56;
    for (size_t i = 0; i < 8; i++)
    {
        (*buffer)[i] = n >> shift;
        shift -= 8;
    }

    *buffer += 8;
}

void
Pack64(unsigned char* buffer, uint64_t n)
{
    unsigned char* c = buffer;
    Pack64(&c, n);
}

void
PackDouble(unsigned char** buffer, double n)
{
    uint64_t u = *(uint64_t*)(&n);
    Pack64(buffer, u);
}

void
PackDouble(unsigned char* buffer, double n)
{
    uint64_t u = *(uint64_t*)(&n);
    unsigned char* c = buffer;
    Pack64(&c, u);
}

}