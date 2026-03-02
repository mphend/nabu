//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <errno.h>

#include "rwlock.h"
#include "mexception.h"
#include "thread_analysis.h"


namespace cor {
    
RWLock::RWLock(const std::string& name) : mName(name)
{
    pthread_rwlockattr_t attr;

    if (pthread_rwlockattr_init(&attr) == -1)
        throw ErrException("RWLock '%s' constructor -- pthread_rwlockattr_init: ", mName.c_str());

    if (pthread_rwlock_init(&mRWLock, &attr) == -1)
        throw ErrException("RWLock '%s' constructor -- pthread_rwlock_init: ", mName.c_str());
}
    
RWLock::~RWLock()
{
    pthread_rwlock_destroy(&mRWLock);
}

void
RWLock::ReadLock()
{
    int r = pthread_rwlock_rdlock(&mRWLock);
    if (r == EDEADLK) // OS X only?
        throw Exception("Deadlock in ReadLock");
    if (r == EINVAL)
        throw Exception("Invalid pthread object in RWLock"); // defensive!
}

void
RWLock::WriteLock()
{
    int r = pthread_rwlock_wrlock(&mRWLock);
    if (r == EDEADLK) // OS X only?
        throw Exception("Deadlock in WriteLock");
    if (r == EINVAL)
        throw Exception("Invalid pthread object in RWLock"); // defensive!
}
    
void
RWLock::Unlock()
{
    pthread_rwlock_unlock(&mRWLock);
}

ReadLocker::ReadLocker() : mRWLock(NULL), mLocked(false) {}
ReadLocker::ReadLocker(RWLock& rWLock, const std::string& context) :
    mRWLock(&rWLock),
    mContext(context)
{
    mLocked = true;
    ThreadAnalysis::Get().RegisterTryReadLock(mRWLock, mContext);
    mRWLock->ReadLock();
    ThreadAnalysis::Get().RegisterGotReadLock(mRWLock, mContext);
}

ReadLocker::~ReadLocker()
{
    if (mLocked)
    {
        ThreadAnalysis::Get().RegisterUnLock(mRWLock);
        mRWLock->Unlock();
        mLocked = false;
    }
}

void
ReadLocker::Subsume(RWLock& rWLock)
{
    mRWLock = &rWLock;
    ThreadAnalysis::Get().RegisterTryReadLock(mRWLock, mContext);
    mRWLock->ReadLock();
    ThreadAnalysis::Get().RegisterGotReadLock(mRWLock, mContext);
    mLocked = true;
}


WriteLocker::WriteLocker() : mRWLock(NULL), mLocked(false)
{}

WriteLocker::WriteLocker(RWLock& rWLock, const std::string& context) :
    mRWLock(&rWLock),
    mContext(context)
{
    ThreadAnalysis::Get().RegisterTryWriteLock(mRWLock, mContext);
    mRWLock->WriteLock();
    ThreadAnalysis::Get().RegisterGotWriteLock(mRWLock, mContext);
    mLocked = true;
}

WriteLocker::~WriteLocker()
{
    if (mLocked)
    {
        ThreadAnalysis::Get().RegisterUnLock(mRWLock);
        mRWLock->Unlock();
    }
}

void
WriteLocker::Subsume(RWLock& rWLock)
{
    mRWLock = &rWLock;
    ThreadAnalysis::Get().RegisterTryWriteLock(mRWLock, mContext);
    mRWLock->WriteLock();
    ThreadAnalysis::Get().RegisterGotWriteLock(mRWLock, mContext);
    mLocked = true;
}


} // end namespace
