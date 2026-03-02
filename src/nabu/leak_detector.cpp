//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <stdio.h>

#include "leak_detector.h"

#define DISABLE

namespace nabu
{

LeakDetector&
LeakDetector::Get()
{
    static LeakDetector* singleton = 0;
    if (singleton == 0)
    {
        singleton = new LeakDetector;
    }
    return *singleton;
}

void
LeakDetector::Increment(const std::string& type)
{
#ifdef DISABLE
    return;
#endif
    cor::ObjectLocker ol(mMutex);
    mImp[type]++;
}

void
LeakDetector::Decrement(const std::string& type)
{
#ifdef DISABLE
    return;
#endif
    cor::ObjectLocker ol(mMutex);
    Imp::iterator i = mImp.find(type);

    // DEFENSIVE
    if (i == mImp.end())
        throw cor::Exception("LeakDetector::Decrement() -- type '%s' not in map\n", type.c_str());

    if (i->second == 0)
        throw cor::Exception("LeakDetector::Decrement() -- type '%s' already 0\n", type.c_str());

    i->second--;
}

// print all counts
void
LeakDetector::Dump()
{
#ifdef DISABLE
    return;
#endif
    cor::ObjectLocker ol(mMutex);
    printf("ReferenceCounts:\n");
    Imp::iterator i = mImp.begin();
    for (; i != mImp.end(); i++)
    {
        printf("   %s : %ld\n", i->first.c_str(), i->second);
    }
}


LeakDetector::LeakDetector() : mMutex("LeakDetector")
{}

LeakDetector::~LeakDetector()
{}

} //  end namespace
