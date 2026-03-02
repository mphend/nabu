//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include "mexception.h"
#include "mthread.h"
#include "thread_analysis.h"

#include <pthread.h>
#include <stdio.h>

namespace cor {

Thread::Thread(const std::string& name) :
    mThreadName(name),
    mThreadId(static_cast<pthread_t>(NULL)),
    mStopMutex(name + "_stop_mutex", false), // false => non-recursive
    mStopNow(false),
    mStopCondition(name + "_stop_cond", mStopMutex),
    mRunning(false),
    mDetached(false)
{
}
    
Thread::~Thread()
{
    {
        cor::ObjectLocker ol(mStopMutex);
        mStopCondition.Signal();
    }
    ThreadAnalysis::Get().UnregisterThread(this);
//    assert(!IsRunning());
}
    
void
Thread::Start()
{
    if (!IsRunning())
    {
        pthread_attr_t attr;
        int n;
        n = pthread_attr_init(&attr);
        if (n != 0)
            throw cor::ErrException(n,"Thread '%s' Start", mThreadName.c_str());

        // default state is joinable, no need to adjust anything
        mRunning = true;
        n = pthread_create(&mThreadId, &attr, ThreadStart, (void*)this);
        if (n != 0)
            throw cor::ErrException(n,"Thread '%s' Start", mThreadName.c_str());

        // thread ID is now valid
        ThreadAnalysis::Get().RegisterThread(this);

        pthread_attr_destroy(&attr);
    }
}
    
void
Thread::Stop()
{
    AskStop();

    if (!mDetached && (mThreadId != 0))
    {
        int n = pthread_join(mThreadId, NULL);
        if (n != 0)
        {
            printf("Stop error: this = %lu, thread = %lu\n",
                   (unsigned long) pthread_self(),
                   (unsigned long) mThreadId);

            throw cor::ErrException(n, "Thread '%s' Stop", mThreadName.c_str());
        }
        mRunning = false;
    }
}

pthread_t
Thread::Self()
{
    return pthread_self();
}

void
Thread::StartDetached()
{
    if (!mRunning)
    {
        pthread_attr_t attr;
        int n;
        n = pthread_attr_init(&attr);
        if (n != 0)
            throw cor::ErrException(n,"Thread '%s' Detach", mThreadName.c_str());

        n = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (n != 0)
            throw cor::ErrException(n,"Thread '%s' Detach", mThreadName.c_str());

        mRunning = true;
        mDetached = true;
        n = pthread_create(&mThreadId, &attr, ThreadStart, (void*)this);
        if (n != 0)
            throw cor::ErrException(n,"Thread '%s' Detach", mThreadName.c_str());

    }
}

void
Thread::AskStop()
{
    cor::ObjectLocker ol(mStopMutex);
    mStopNow = true;
    mStopCondition.Signal();
}

bool
Thread::StopNow() const
{
    cor::ObjectLocker ol(mStopMutex);
    return mStopNow;
}

bool
Thread::Wait(int seconds)
{
    cor::ObjectLocker ol(mStopMutex);

    bool r = true;
    if (!mStopNow)
    {
        r = mStopCondition.Wait(seconds * 1000); // convert to msec
    }

    return r;
}

void* Thread::ThreadStart(void* thisPtr)
{
	Thread* thread = (Thread*)(thisPtr);

    /*  POSIX.1 says that the effect of using return, break, continue, or  goto
       to  prematurely  leave  a  block  bracketed  pthread_cleanup_push() and
       pthread_cleanup_pop() is undefined.  Portable applications should avoid
       doing this.*/
	pthread_cleanup_push(ThreadDelete, thisPtr);
	try
    {
        thread->ThreadFunction();
    }
    // ThreadFunction should handle all exceptions...but just in case...
    catch (const std::exception& err) {
        std::cout << "Thread '" << thread->GetName() << "' error backstop: " << err.what() << std::endl;
    }
    pthread_cleanup_pop(0);
    
    thread->mRunning = false;

    if (thread->IsDetached())
    {
    	delete thread;
    }

    return NULL;
}

void Thread::ThreadDelete(void* thisPtr)
{
	try
    {
        Thread* thread = (Thread*)(thisPtr);
        delete thread;
    }
    catch (const std::exception& err) {
        std::cout << "Thread cleanup backstop: " << err.what() << std::endl;
    }
}
    
}
