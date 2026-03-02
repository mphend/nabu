//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <stdio.h>

#include "profiler.h"

namespace cor
{


/** Timespec utility functions and operators
 *
 */

const unsigned long oneBillion = 1000000000L;

struct timespec
operator-(const struct timespec &later, const struct timespec &earlier)
{
    struct timespec r;
    if ((later.tv_nsec - earlier.tv_nsec) < 0) {
        r.tv_nsec = oneBillion + later.tv_nsec - earlier.tv_nsec;
        r.tv_sec = later.tv_sec - earlier.tv_sec - 1;

    } else {
        r.tv_sec = later.tv_sec - earlier.tv_sec;
        r.tv_nsec = later.tv_nsec - earlier.tv_nsec;
    }

    return r;
}

struct timespec
operator+(const struct timespec &t1, const struct timespec &t2)
{
    struct timespec r = t1;
    r.tv_nsec += t2.tv_nsec;
    r.tv_sec  += t2.tv_sec;

    // normalize
    if (r.tv_nsec > oneBillion)
    {
        r.tv_nsec -= oneBillion;
        r.tv_sec += 1;
    }

    return r;
}

void
operator+=(struct timespec &t1, const struct timespec &t2)
{
    t1 = t1 + t2;
}


std::ostream& operator<<(std::ostream& os, const timespec& ts)
{
    char b[30];
    snprintf(b, sizeof(b), "%lld.%.9lld", (long long)ts.tv_sec, (long long)ts.tv_nsec);
    os << b;
    return os;
}


/** Profiler implementation
 *
 */

Profiler::Profiler() : mMutex("Profiler")
{}

Profiler::~Profiler()
{}

void Profiler::Start(const std::string& what)
{
    cor::ObjectLocker ol(mMutex);
    clock_gettime(CLOCK_REALTIME, &(mTally[what].mStart));
}

void Profiler::Stop(const std::string& what)
{
    cor::ObjectLocker ol(mMutex);
    imp& entry = mTally[what];
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    entry.mDuration += (now - entry.mStart);
}


void
Profiler::Count(const std::string& what)
{
    cor::ObjectLocker ol(mMutex);
    mCounts[what]++;
}

void
Profiler::Clear()
{
    cor::ObjectLocker ol(mMutex);
    mTally.clear();
    mCounts.clear();
}

std::ostream& operator<<(std::ostream& os, const Profiler& p)
{
    cor::ObjectLocker ol(p.mMutex);
    Profiler::BaseMap::const_iterator ci = p.mTally.begin();
    if (!p.mTally.empty())
        os << "Times:" << std::endl;
    struct timespec total;
    total.tv_nsec = 0;
    total.tv_sec = 0;
    for (; ci != p.mTally.end(); ci++)
    {
        struct timespec d = ci->second.mDuration;
        total += d;
        os << ci->first << " : " << d << std::endl;
    }
    os << "--------------" << std::endl;
    os << "total: " << total << std::endl;
    if (!p.mCounts.empty())
        os << "Counts:" << std::endl;
    Profiler::CountMap ::const_iterator cmi = p.mCounts.begin();
    for (; cmi != p.mCounts.end(); cmi++)
    {
        os << cmi->first << " : " << cmi->second << std::endl;
    }
    return os;
}

} // end namespace
