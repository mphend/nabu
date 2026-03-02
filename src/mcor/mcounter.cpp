//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <stdio.h>

#include "mcounter.h"

namespace cor {
 

Counter::Counter(int max) : mMutex("Counter"), mMax(max), mCount(0)
{}

Counter::~Counter()
{}

bool
Counter::IsExpired() const
{
    cor::ObjectLocker ol(mMutex);
    return mCount > mMax;
}

bool
Counter::IsExpired(double percentage) const
{
    cor::ObjectLocker ol(mMutex);
    return mCount > (mMax * (percentage / 100.0));
}

int
Counter::GetMax() const
{
    cor::ObjectLocker ol(mMutex);
    return mMax;
}

void
Counter::Reset(int newMax)
{
    cor::ObjectLocker ol(mMutex);
    mCount = 0;
    if (newMax > 0)
        mMax = newMax;
}

bool
Counter::Tick()
{
    cor::ObjectLocker ol(mMutex);
    mCount++;
    return mCount == (mMax + 1);
}


    
}
