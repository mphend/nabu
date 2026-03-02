//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __threader__queue_thread__
#define __threader__queue_thread__

#include <iostream>
#include <memory>

#include "mthread.h"
#include "condition_queue.h"
#include "time.h"

namespace cor {
    
template <class T>
class ConstQueueThread : public Thread
{
public:
    ConstQueueThread(const std::string& name, const size_t maxSize = 2);
    virtual ~ConstQueueThread();

    // puts item in the back of the queue
    // returns true if there is room in the queue for the new item; otherwise, it was
    // not inserted and nothing happens.
    bool PutItem(const T& item);

    // puts item in the front of the queue
    // returns true if there is room in the queue for the new item; otherwise, it was
    // not inserted and nothing happens.
    bool RequeueItem(const T& item);

    size_t Size() const { return mConditionQueue.Size(); }
    
protected:
    
    virtual void HandleItem(const T& item) = 0;

    // override to do something when no item arrives in a polling interval
    virtual void HandleNoItem(const cor::Time& timeSinceLastItem) {};

    void ThreadFunction();
    
    ConditionQueue<T> mConditionQueue;
    cor::Time mLastItemTime;
};
    
template <class T>
ConstQueueThread<T>::ConstQueueThread(const std::string& name, size_t maxSize) :
    Thread(name),
    mConditionQueue(maxSize),
    mLastItemTime(cor::Time::eInvalid)
{
}
 
template <class T>
ConstQueueThread<T>::~ConstQueueThread()
{
}
  
template <class T>
bool
ConstQueueThread<T>::PutItem(const T& item)
{
    return mConditionQueue.PutItem(item);
}

template <class T>
bool
ConstQueueThread<T>::RequeueItem(const T& item)
{
    return mConditionQueue.RequeueItem(item);
}

template <class T>
void
ConstQueueThread<T>::ThreadFunction()
{
    while (!StopNow())
    {
        // need to use pointer because we T may not have a default constructor
        // but it better have a copy constructor
        T *item;
        if (mConditionQueue.GetItem(1000, &item))
        {
            mLastItemTime = cor::Time(); // now
            std::unique_ptr<T> iptr(item);
            HandleItem(*iptr);
        }
        else
        {
            if (!mLastItemTime.Valid())
                HandleNoItem(mLastItemTime);
            else
                HandleNoItem(cor::Time() - mLastItemTime);
        }
    }
}



template <class T>
class QueueThread : public Thread
{
public:
    QueueThread(const std::string& name, const size_t maxSize = 2);
    virtual ~QueueThread();

    // puts item in the back of the queue
    // returns true if there is room in the queue for the new item; otherwise, it was
    // not inserted and nothing happens.
    bool PutItem(T& item);

    // puts item in the front of the queue
    // returns true if there is room in the queue for the new item; otherwise, it was
    // not inserted and nothing happens.
    bool RequeueItem(T& item);

    size_t Size() const { return mConditionQueue.Size(); }

protected:

    virtual void HandleItem(T& item) = 0;

    void ThreadFunction();

    ConditionQueue<T> mConditionQueue;
};

template <class T>
QueueThread<T>::QueueThread(const std::string& name, size_t maxSize) :
        Thread(name),
        mConditionQueue(maxSize)
{
}

template <class T>
QueueThread<T>::~QueueThread()
{
}

template <class T>
bool
QueueThread<T>::PutItem(T& item)
{
    return mConditionQueue.PutItem(item);
}

template <class T>
bool
QueueThread<T>::RequeueItem(T& item)
{
    return mConditionQueue.RequeueItem(item);
}

template <class T>
void
QueueThread<T>::ThreadFunction()
{
    while (!StopNow())
    {
        // need to use pointer because we T may not have a default constructor
        // but it better have a copy constructor
        T *item;
        if (mConditionQueue.GetItem(1000, &item))
        {
            std::unique_ptr<T> iptr(item);
            HandleItem(*iptr);
        }
    }
}

}

#endif /* defined(__threader__queue_thread__) */
