//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __threader__mthread__
#define __threader__mthread__

#include <iostream>
#include <pthread.h>
#include <string>

#include "mcondition.h"

namespace cor
{

    
/** class Thread
 *
 *  Wraps a pthread thread.
 *
 */
class Thread
{
public:
    Thread(const std::string& name);
    virtual ~Thread();

    // Starts a thread that is to be Stopped; see StartDetached below
    virtual void Start();
    // blocks until thread is joined
    virtual void Stop();

    static pthread_t Self();

    // Runs a thread that is fire-and-forget (detached) and is responsible
    // for stopping itself
    // this object is destroyed automatically too
    void StartDetached();
    bool IsDetached() const { return mDetached; }

    // tells thread to stop but does not join
    void AskStop();

    bool IsRunning() const { return mRunning; }
    
    std::string GetName() const { return mThreadName; }

    // return thread ID
    unsigned long GetId() const { return (unsigned long)mThreadId; }

protected:
    bool StopNow() const;

    virtual void ThreadFunction() = 0;

    // sleep for n seconds, unless asked to Stop; returns true if
    // stop request interrupts the sleep, false otherwise
    virtual bool Wait(int seconds);

    const std::string mThreadName;
    pthread_t mThreadId;

    // the next three members implement thread stopping logic
    mutable MMutex mStopMutex;
    bool mStopNow;
    mutable MCondition mStopCondition;

    bool mRunning;
    bool mDetached;
    
    // class static function to invoke the thread function
    static void* ThreadStart(void* thisPtr);
    // class static function to clean up detached thread object
    static void ThreadDelete(void* thisPtr);
};
    
} // end namespace
#endif /* defined(__threader__mthread__) */
