//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_MCOR_THREAD_SET_H
#define GICM_MCOR_THREAD_SET_H

#include <set>
#include <unistd.h>

#include "mthread.h"

namespace cor
{


/** class ThreadSet : maintains a set of threads to do work in parallel and
 *  block until they are done.
 *
 *  This class takes ownership of the thread, starting it and freeing its
 *  memory.
 *
 *  Use:
 *
 *  ThreadSet<MyThread> threadSet;
 *  for (size_t i = 0; i < N; i++)
 *     threadSet.Add(new MyThread());
 *
 *  threadSet.WaitUntilDone();
 *
 */
template <class ThreadType>
class ThreadSet : public std::set<ThreadType*>
{
public:
    ThreadSet();
    virtual ~ThreadSet();

    // starts the thread
    void Add(ThreadType* thread);

    // blocks until all threads are no longer running
    void WaitUntilDone();

    //typedef std::set<ThreadType*> Imp;
    //Imp mImp;
};

template <class ThreadType>
ThreadSet<ThreadType>::ThreadSet()
{
}

template <class ThreadType>
ThreadSet<ThreadType>::~ThreadSet()
{
    WaitUntilDone();

    // stop all threads--even though they are already done--to clean up resources
    // by calling 'join'
    typename std::set<ThreadType*>::const_iterator i = std::set<ThreadType*>::begin();
    for (; i != std::set<ThreadType*>::end(); i++)
    {
        (*i)->Stop();
        delete *i;
    }
}

template <class ThreadType>
void
ThreadSet<ThreadType>::Add(ThreadType* thread)
{
    thread->Start();
    std::set<ThreadType*>::insert(thread);
}

template <class ThreadType>
void
ThreadSet<ThreadType>::WaitUntilDone()
{
    while (true)
    {
        typename std::set<ThreadType*>::const_iterator i = std::set<ThreadType*>::begin();
        for (; i != std::set<ThreadType*>::end(); i++)
        {
            if ((*i)->IsRunning())
            {
                usleep(10000);
                break;
            }
        }
        if (i == std::set<ThreadType*>::end())
            break;
    }
}

}

#endif

