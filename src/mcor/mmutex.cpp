//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include "mmutex.h"
#include "mexception.h"
#include "thread_analysis.h"

#include <errno.h>

namespace cor {
    
MMutex::MMutex(const std::string& name, bool recursive) : mName(name), mRecursive(recursive)
{
    pthread_mutexattr_t attr;

    if (pthread_mutexattr_init(&attr) == -1)
        throw ErrException("MMutex '%s' constructor -- pthread_mutexattr_init: ", mName.c_str());

    if (mRecursive)
    {
        if (pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) == -1)
            throw ErrException("MMutex '%s' constructor -- pthread_mutexattr_settype: ", mName.c_str());
    }

    if (pthread_mutex_init(&mMutex, &attr) == -1)
        throw ErrException("MMutex '%s' constructor -- pthread_mutex_init: ", mName.c_str());
}
    
MMutex::~MMutex()
{
    pthread_mutex_destroy(&mMutex);
}
    
void
MMutex::Lock()
{
    int r = pthread_mutex_lock(&mMutex);
    if (r == EDEADLK) // OS X only?
        throw Exception("Deadlock in MMutex");
    if (r == EINVAL)
        throw Exception("Lock() -- Invalid pthread object in MMutex '%s'", GetName().c_str()); // defensive!
}

bool
MMutex::TryLock()
{
    int r = pthread_mutex_trylock(&mMutex);
    if (r == EBUSY)
        return false;
    if (r == EINVAL)
        throw Exception("TryLock() -- Invalid pthread object in MMutex"); // defensive!
    return true;
}
    
void
MMutex::Unlock()
{
    pthread_mutex_unlock(&mMutex);
}


MPhonyMutex::MPhonyMutex(const std::string& name)  : MMutex(name)
{
}

MPhonyMutex::~MPhonyMutex()
{
}

void
MPhonyMutex::Lock()
{
}

bool
MPhonyMutex::TryLock()
{
    return true;
}

void
MPhonyMutex::Unlock()
{

}


/** class ObjectLocker Implementation
 *
 */

ObjectLocker::ObjectLocker() : mMutex(NULL), mLocked(false)
{}

ObjectLocker::ObjectLocker(MMutex& mutex, const std::string& context) :
    mMutex(&mutex),
    mContext(context)
{
    ThreadAnalysis::Get().RegisterTryLock(mMutex, mContext);
    mMutex->Lock();
    //printf("---- 0x%lx : %s at %s\n", cor::Thread::Self(), mMutex->GetName().c_str(), mContext.c_str());
    ThreadAnalysis::Get().RegisterGotLock(mMutex, mContext);
    mLocked = true;
}

ObjectLocker::ObjectLocker(MMutexPtr mutexPtr, const std::string& context) :
        mMutex(NULL),
        mMutexPtr(mutexPtr),
        mContext(context)
{
    ThreadAnalysis::Get().RegisterTryLock(mMutexPtr.get(), mContext);
    mMutexPtr->Lock();
    //printf("---- 0x%lx : %s at %s\n", cor::Thread::Self(), mMutexPtr->GetName().c_str(), mContext.c_str());
    ThreadAnalysis::Get().RegisterGotLock(mMutexPtr.get(), mContext);
    mLocked = true;
}

void
ObjectLocker::Subsume(MMutex& mutex)
{
    mMutex = &mutex;
    ThreadAnalysis::Get().RegisterTryLock(mMutexPtr.get(), mContext);
    mMutex->Lock();
    //printf("---- 0x%lx : %s at %s\n", cor::Thread::Self(), mMutex->GetName().c_str(), mContext.c_str());
    ThreadAnalysis::Get().RegisterGotLock(mMutexPtr.get(), mContext);
    mLocked = true;
}

ObjectLocker::~ObjectLocker()
{
    Unlock();
}

void
ObjectLocker::Unlock()
{
    if (mLocked)
    {
        if (mMutex != NULL)
        {
            ThreadAnalysis::Get().RegisterUnLock(mMutex);
            mMutex->Unlock();
        }
        else
        {
            ThreadAnalysis::Get().RegisterUnLock(mMutexPtr.get());
            mMutexPtr->Unlock();
        }
        mLocked = false;
    }
}


} // end namespace
