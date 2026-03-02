//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <sys/time.h>

#include "mcondition.h"

namespace cor {
    
MCondition::MCondition(const std::string& name, MMutex& mutex) : mName(name), mMutex(mutex)
{
    // On OSX, recurive mutexes cannot work with condition variables
    if (mMutex.IsRecursive())
        throw cor::Exception("MCondition::MCondition() -- condition '%s' using recursive mutex '%s'",
                             mName.c_str(),
                             mMutex.GetName().c_str());
    assert(pthread_cond_init(&mCondition, NULL) == 0);
}
    
MCondition::~MCondition()
{
    pthread_cond_destroy(&mCondition);
}
    
bool
MCondition::Wait(int msec) throw()
{
    timespec ts;
    struct timeval tv;

    // must compute absolute time, and can't have nsec larger than one second
    gettimeofday(&tv, NULL);
    ts.tv_sec = tv.tv_sec + msec / 1000;
    ts.tv_nsec = tv.tv_usec * 1000 + 1000 * 1000 * (msec % 1000);
    ts.tv_sec += ts.tv_nsec / (1000 * 1000 * 1000);
    ts.tv_nsec %= (1000 * 1000 * 1000);

    int rc = pthread_cond_timedwait(&mCondition, &(mMutex.mMutex), &ts);

    if (rc == 0)
        return true;
    if (rc == ETIMEDOUT)
        return false;
    throw cor::Exception("MCondition %s: %s", mName.c_str(), strerror(rc));

}

bool
MCondition::Wait(const cor::Time& wallClockTime) throw()
{
    timespec ts = wallClockTime.ToTimeSpec();
    int rc = pthread_cond_timedwait(&mCondition, &(mMutex.mMutex), &ts);

    if (rc == 0)
        return true;
    if (rc == ETIMEDOUT)
        return false;
    throw cor::Exception("MCondition %s: %s", mName.c_str(), strerror(rc));
}


bool
MCondition::SleepUntil(const cor::Time &wallClockTime) throw()
{
    pthread_cond_t condition;
    pthread_mutex_t mutex;
    pthread_mutexattr_t attr;
    int rc;
    if ((rc = pthread_mutexattr_init(&attr)) != 0)
        throw Exception("MCondition::SleepUntil() -- pthread_mutexattr_init rc = %d, %s",
                        rc, strerror(rc));
    if ((rc = pthread_mutex_init(&mutex, NULL)) != 0)
        throw Exception("MCondition::SleepUntil() -- pthread_mutexattr_init rc = %d, %s",
                           rc, strerror(rc));
    if ((rc = pthread_cond_init(&condition, NULL)) != 0)
        throw Exception("MCondition::SleepUntil() -- pthread_cond_init rc = %d, %s",
                           rc, strerror(rc));

    timespec ts = wallClockTime.ToTimeSpec();
    rc = pthread_cond_timedwait(&condition, &mutex, &ts);

    // cleanup and exit unless an error occurred
    if ((rc == 0) || (rc == ETIMEDOUT))
    {
        if ((rc = pthread_cond_destroy(&condition)) != 0)
            throw Exception("MCondition::SleepUntil() -- pthread_cond_destroy rc = %d, %s",
                               rc, strerror(rc));
        if ((rc = pthread_mutex_destroy(&mutex)) != 0)
            throw Exception("MCondition::SleepUntil() -- pthread_mutex_destroy rc = %d, %s",
                               rc, strerror(rc));

        return rc == 0; // false, if timeout
    }

    throw cor::Exception("MCondition::SleepUntil : rc = %d, %s",
                         rc, strerror(rc));
}

void
MCondition::Wait()
{
    int rc;
    if ((rc = pthread_cond_wait(&mCondition, &(mMutex.mMutex))) != 0)
        throw cor::Exception("Mcondition::Wait() -- (%s)", mName.c_str(), strerror(rc));
}
    
void
MCondition::Signal() throw()
{
    int rc = pthread_cond_signal(&mCondition);
    if (rc != 0)
    {
        printf("MCondition::Signal() : %d %s\n", rc, strerror(rc));
    }
}
    
}
