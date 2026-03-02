//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include "async_status.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace nabu
{

AsyncStatus::AsyncStatus() : mMutex("AsyncStatus"), mTotal(0), mProcessed(0), mCount(0), mRunning(false)
{}
AsyncStatus::AsyncStatus(const AsyncStatus& other) : mMutex("AsyncStatus")
{
    Copy(other);
}

void
AsyncStatus::operator=(const AsyncStatus& other)
{
    Copy(other);
}

size_t
AsyncStatus::Total() const
{
    cor::ObjectLocker ol(mMutex);
    return mTotal;
}

void
AsyncStatus::Total(size_t x)
{
    cor::ObjectLocker ol(mMutex);
    mTotal = x;
}

void
AsyncStatus::TotalInc(size_t x)
{
    cor::ObjectLocker ol(mMutex);
    mTotal += x;
}

size_t
AsyncStatus::Processed() const
{
    cor::ObjectLocker ol(mMutex);
    return mProcessed;
}

void
AsyncStatus::Processed(size_t x)
{
    cor::ObjectLocker ol(mMutex);
    mProcessed = x;
}

void
AsyncStatus::ProcessedInc(size_t x)
{
    cor::ObjectLocker ol(mMutex);
    mProcessed += x;
}

size_t
AsyncStatus::Count() const
{
    cor::ObjectLocker ol(mMutex);
    return mCount;
}

void
AsyncStatus::Count(size_t x)
{
    cor::ObjectLocker ol(mMutex);
    mCount = x;
}

void
AsyncStatus::CountInc(size_t x)
{
    cor::ObjectLocker ol(mMutex);
    mCount += x;
}

bool
AsyncStatus::Done() const
{
    cor::ObjectLocker ol(mMutex);
    return !mRunning;
}

bool
AsyncStatus::SetRunning(bool b)
{
    cor::ObjectLocker ol(mMutex);
    bool old = mRunning;
    mRunning = b;
    return old;
}

void
AsyncStatus::Copy(const AsyncStatus& other)
{
    cor::ObjectLocker ol(mMutex);
    mTotal = other.mTotal;
    mProcessed = other.mProcessed;
    mCount = other.mCount;
    mRunning = other.mRunning;
}

} //  end namespace
