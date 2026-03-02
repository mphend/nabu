//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor_wall_clock_thread__
#define __mcor_wall_clock_thread__

#include <iostream>
#include <map>
#include <memory>

#include "mcondition.h"
#include "mthread.h"
#include "time.h"

namespace cor {
    
template <class T>
class WallClockThread : public Thread
{
public:
    WallClockThread(const std::string& name);
    virtual ~WallClockThread();

    // Enqueue item for processing at time 'when';
    // IThis should not be called by the inherited object in the context
    // of its handler; use protected method PutItemReentrant in this
    // case.
    void PutItem(const cor::Time& when, const T& item);
    bool HasItem(const cor::Time& when) const;

    void Stop();

    
protected:
    typedef std::map<cor::Time, T> Imp;

    mutable MMutex mMutex;
    Imp mImp;

    MCondition mCondition;
    
    virtual void HandleItem(const cor::Time& scheduled, const T& item) = 0;

    void PutItemReentrant(const cor::Time& when, const T& item);

    void ThreadFunction();
};
    
template <class T>
WallClockThread<T>::WallClockThread(const std::string& name) :
    Thread(name),
    mMutex(name, false),
    mCondition(name, mMutex)
{
}
 
template <class T>
WallClockThread<T>::~WallClockThread()
{
    mCondition.Signal();
}

template <class T>
void
WallClockThread<T>::PutItem(const cor::Time& when, const T& item)
{
    {
        cor::ObjectLocker ol(mMutex);
        // don't allow replacements
        if (mImp.find(when) != mImp.end())
        {
            throw cor::Exception("Attempted to schedule duplicate work at time '%s'", when.Print().c_str());
        }

        mImp[when] = item;
    }
    mCondition.Signal();
}

template <class T>
void
WallClockThread<T>::PutItemReentrant(const cor::Time& when, const T& item)
{
    // don't allow replacements
    if (mImp.find(when) != mImp.end())
    {
        throw cor::Exception("Attempted to schedule duplicate work at time '%s'", when.Print().c_str());
    }

    mImp[when] = item;
}

template <class T>
bool
WallClockThread<T>::HasItem(const cor::Time& when) const
{
    cor::ObjectLocker ol(mMutex);
    return (mImp.find(when) != mImp.end());
}

template <class T>
void
WallClockThread<T>::Stop()
{
    mStopNow = true;
    mCondition.Signal();
    Thread::Stop();
}

template <class T>
void
WallClockThread<T>::ThreadFunction()
{
    while (!StopNow())
    {
        cor::ObjectLocker ol(mMutex);
        if (mImp.empty())
        {
            mCondition.Wait(); // wait until signalled
            if (mImp.empty())
                continue;
        }

        cor::Time next = mImp.begin()->first;

        if (mCondition.Wait(next.ToTimeSpec()))
        {
            // did not time out, so contents changed; start over
            continue;
        }

        T t = mImp.begin()->second;
        mImp.erase(mImp.begin()); // pop the front
        HandleItem(next, t);
    }
}
    
}

#endif
