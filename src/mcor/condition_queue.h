//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __threader__condition_queue__
#define __threader__condition_queue__

#include <iostream>
#include <list>

#include "mcondition.h"

namespace cor
{
    
/** Template ConditionQueue
 *
 */
template <class T>
class ConditionQueue {
    std::list<T> mQueue;
    size_t mMaxSize;
    
    mutable MMutex mMutex;
    MCondition mCondition;
    
public:
    ConditionQueue(size_t maxSize);
    virtual ~ConditionQueue();
    
    // enqueues item in the back, if there is room; returns true if this succeeds
    bool PutItem(const T& item);

    // requeues item in the front, if there is room; returns true if this succeeds
    bool RequeueItem(const T& item);

    // returns true if t is valid, false if timeout occurred
    bool GetItem(int msec, T& t);
    // returns true if t is valid and newly allocated object, false if timeout occurred
    bool GetItem(int msec, T** t);

    size_t Size() const
    {
        cor::ObjectLocker ol(mMutex);
        return mQueue.size();
    }

    void Clear();
};

template <class T>
ConditionQueue<T>::ConditionQueue(size_t maxSize) :
    mMaxSize(maxSize),
    mMutex("ConditionQueue", false),
    mCondition("ConditionQueue", mMutex)
{
}
    
template <class T>
ConditionQueue<T>::~ConditionQueue()
{
}
    
template <class T>
bool
ConditionQueue<T>::PutItem(const T& item)
{
    bool success = false;
    {
        ObjectLocker ol(mMutex);
        // XXX calling size() on a list can be order N expensive
        if (mQueue.size() < mMaxSize)
        {
            success = true;
            mQueue.push_back(item);
        }
    }
    if (success)
        mCondition.Signal();
    return success;
}

template <class T>
bool
ConditionQueue<T>::RequeueItem(const T &item)
{
    bool success = false;
    {
        ObjectLocker ol(mMutex);
        // XXX calling size() on a list can be order N expensive
        if (mQueue.size() < mMaxSize)
        {
            success = true;
            mQueue.push_front(item);
        }
    }
    if (success)
        mCondition.Signal();
    return success;
}
    
template <class T>
bool
ConditionQueue<T>::GetItem(int msec, T& t)
{
    ObjectLocker ol(mMutex);
    if (mQueue.empty())
        mCondition.Wait(msec);
    if (!mQueue.empty())
    {
        t = mQueue.front();
        mQueue.pop_front();
        return true;
    }
    return false;
}

template <class T>
bool
ConditionQueue<T>::GetItem(int msec, T** t)
{
    ObjectLocker ol(mMutex);
    if (mQueue.empty())
        mCondition.Wait(msec);
    if (!mQueue.empty())
    {
        *t = new T(mQueue.front());
        mQueue.pop_front();
        return true;
    }
    return false;
}

template <class T>
void
ConditionQueue<T>::Clear()
{
    ObjectLocker ol(mMutex);
    mQueue.clear();
}
    
} // end namespace

#endif /* defined(__threader__condition_queue__) */
