//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
//  This work is based on timespec math routines originally by
//  Daniel Collins (2017) and Alex Forencich (2019).


#include <stdio.h>
#include <iostream>

#include "mexception.h"

#include "bigtime.h"

#define ASEC_PER_SEC 1000000000000000000ll
#define ATTOSECONDS_PER_NANOSECOND 1000000000ll

BigTime::BigTime() : mSeconds(0), mAttoSeconds(0)
{}

BigTime::BigTime(int64_t seconds, int64_t attoSeconds, bool normalize) :
    mSeconds(seconds),
    mAttoSeconds(attoSeconds)
{
    if (normalize)
        Normalize();
}

BigTime::BigTime(double d)
{
    mSeconds = (int64_t)d;
    mAttoSeconds = (d - (int64_t)(d)) * ASEC_PER_SEC;

    Normalize();
}

BigTime&
BigTime::operator +=(BigTime other)
{
    other.Normalize();
    *this = *this + other;
    return *this;
}

BigTime&
BigTime::operator -=(BigTime other)
{
    other.Normalize();
    *this = *this - other;
    return *this;
}

BigTime::operator double() const
{
    return ((double)(mSeconds) + ((double)(mAttoSeconds) / ASEC_PER_SEC));
}

void
BigTime::FromTimeSpec(const struct timespec &ts)
{
    mSeconds = ts.tv_sec;
    mAttoSeconds = ts.tv_nsec * ATTOSECONDS_PER_NANOSECOND;
}

struct timespec
BigTime::ToTimeSpec() const
{
    struct timespec r;
    r.tv_sec = mSeconds;
    r.tv_nsec = mAttoSeconds / ATTOSECONDS_PER_NANOSECOND;
    return r;
}

BigTime
BigTime::MaxPositive()
{
    const int64_t v = ~0x8000000000000000;
    return BigTime(v, v);
}

BigTime
BigTime::MaxNegative()
{
    const int64_t v = 0x8000000000000000;
    return BigTime(v, v);
}

void
BigTime::Normalize()
{
    while (mAttoSeconds >= ASEC_PER_SEC)
    {
        ++(mSeconds);
        mAttoSeconds -= ASEC_PER_SEC;
    }

    while(mAttoSeconds <= -ASEC_PER_SEC)
    {
        --(mSeconds);
        mAttoSeconds += ASEC_PER_SEC;
    }

    if (mAttoSeconds < 0)
    {
        // Decrement mSeconds and roll mAttoSeconds over.

        --(mSeconds);
        mAttoSeconds = (ASEC_PER_SEC + mAttoSeconds);
    }
}

std::string
BigTime::PrintFractional() const
{
    char buffer[120];
    // print integer part
    size_t n = snprintf(buffer, sizeof(buffer), "%ld", mSeconds);

    // decimal potion
    if (mAttoSeconds != 0)
    {
        buffer[n++] = '.';
        uint64_t a = mAttoSeconds;
        uint64_t d = ASEC_PER_SEC / 10;
        for (size_t i = 0; i < 64; i++)
        {
            if (a == 0)
            {
                break;
            }
            unsigned digit = (a / d);
            buffer[n++] = digit + '0'; // ASCII only
            a -= d * digit;
            d /= 10;
        }
        buffer[n] = 0; // terminate
    }

    return buffer;
}

std::string
BigTime::PrintFractionalHex() const
{
    char buffer[120];
    // print integer part
    size_t n = snprintf(buffer, sizeof(buffer), "0x%016lx", mSeconds);
    snprintf(buffer + n, sizeof(buffer) - n," 0x%016lx", mAttoSeconds);

    return buffer;
}

std::string
BigTime::PrintUnifiedHex() const
{
    char buffer[120];
    // print integer part
    size_t n = snprintf(buffer, sizeof(buffer), "%016lx%016lx", mSeconds, mAttoSeconds);

    return buffer;
}

std::string
BigTime::Print() const
{
    char buffer[120];
    snprintf(buffer, sizeof(buffer), "%ld sec, %ld asec", mSeconds, mAttoSeconds);
    return buffer;
}

std::string
BigTime::HexDump() const
{
    char buffer[120];
    snprintf(buffer, sizeof(buffer), "0x%lx sec, 0x%lx asec", mSeconds, mAttoSeconds);
    return buffer;
}

void
BigTime::FromUnifiedHex(const std::string& hex)
{
    if (hex.size() != 32)
        throw cor::Exception("BigTime::FromUnifiedHex() -- argument is not valid");
    sscanf(hex.c_str(), "%16lx", &mSeconds);
    sscanf(hex.c_str() + 16, "%16lx", &mAttoSeconds);
}

BigTime operator+(BigTime ts1, BigTime ts2)
{
	ts1.Normalize();
	ts2.Normalize();
	
	ts1.mSeconds += ts2.mSeconds;
	ts1.mAttoSeconds += ts2.mAttoSeconds;
	
	ts1.Normalize();
    return ts1;
}

BigTime operator-(BigTime ts1, BigTime ts2)
{
	ts1.Normalize();
	ts2.Normalize();
	
	ts1.mSeconds -= ts2.mSeconds;
	ts1.mAttoSeconds -= ts2.mAttoSeconds;
	
	ts1.Normalize();
    return ts1;
}

bool operator==(BigTime ts1, BigTime ts2)
{
	ts1.Normalize();
	ts2.Normalize();
	
	return (ts1.mSeconds == ts2.mSeconds &&
            ts1.mAttoSeconds == ts2.mAttoSeconds);
}

bool operator>(BigTime ts1, BigTime ts2)
{
	ts1.Normalize();
	ts2.Normalize();
	
	return (ts1.mSeconds > ts2.mSeconds ||
           (ts1.mSeconds == ts2.mSeconds && 
            ts1.mAttoSeconds > ts2.mAttoSeconds));
}

bool operator>=(BigTime ts1, BigTime ts2)
{
	ts1.Normalize();
	ts2.Normalize();
	
	return (ts1.mSeconds > ts2.mSeconds ||
           (ts1.mSeconds == ts2.mSeconds &&
            ts1.mAttoSeconds >= ts2.mAttoSeconds));
}

bool operator<(BigTime ts1, BigTime ts2)
{
	ts1.Normalize();
	ts2.Normalize();
	
	return (ts1.mSeconds < ts2.mSeconds ||
           (ts1.mSeconds == ts2.mSeconds &&
            ts1.mAttoSeconds < ts2.mAttoSeconds));
}

bool operator<=(BigTime ts1, BigTime ts2)
{
	ts1.Normalize();
	ts2.Normalize();
	
	return (ts1.mSeconds < ts2.mSeconds ||
           (ts1.mSeconds == ts2.mSeconds && 
            ts1.mAttoSeconds <= ts2.mAttoSeconds));
}
