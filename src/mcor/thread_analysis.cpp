//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <stdio.h>
#include <stdlib.h>

#include "thread_analysis.h"

namespace cor {

void
ThreadAnalysis::RegisterTryLock(MMutex* mutex, const std::string& context)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif

    mMutex.Lock();

    pthread_t id = cor::Thread::Self();

    LockInfoVector& miv = mThreadMap[id].mLockInfo;
    // It is not an error for a lock to re-enter the try lock state; this can
    // happen if Unlock() is called on the ObjectLocker. Such an action is
    // done to allow another thread (such as the one calling this function)
    // to lock the mutex.
    if (!miv.empty())
    {
        // check if back is already this mutex
        LockInfo& info = miv.back();
        if (info.mAccessedMutex == mutex)
        {
            if (info.mState != eLocked) // DEFENSIVE; this should not happen
            {
                printf("Mutex %s was locking, but already existed in an unlocked state on this thread (%s)\n",
                       mutex->GetName().c_str(), GetThreadName(id).c_str());
            }
        }
    }
    LockInfo newInfo;
    newInfo.mState = eBlocked;
    newInfo.mAccessedMutex = mutex;
    newInfo.mContext = context;
    miv.push_back(newInfo);

    mMutex.Unlock();
}

void
ThreadAnalysis::RegisterGotLock(MMutex* mutex, const std::string& context)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif

    mMutex.Lock();

    pthread_t id = cor::Thread::Self();

    LockInfoVector& miv = mThreadMap[id].mLockInfo;

    if (miv.empty()) // DEFENSIVE
    {
        printf("Mutex %s registered as locked but is not known\n", mutex->GetName().c_str());
        mMutex.Unlock();
        return;
    }

    // this mutex is normally the most recent one registered, but in a race
    // condition, a thread could get into the TryLock state, get switched out,
    // the new thread get into the TryLock state and also get swtiched out, and
    // then the original thread proceed to locking the mutex. Possible, but more
    // likely a programming error, and unlikely all around.
    LockInfo& mi = miv.back();
    if (mi.mAccessedMutex != mutex) // DEFENSIVE
    {
        printf("Mutex %s registered as locked but is not the most recent (possibly OK, but unlikely)\n", mutex->GetName().c_str());
        //  XXX search rest of stack for this mutex
        printf("Exiting\n");
        exit(0);
    }
    mi.mState = eLocked;
    mi.mContext = context;

    mMutex.Unlock();
}

void
ThreadAnalysis::RegisterUnLock(MMutex* mutex)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif

    mMutex.Lock();

    pthread_t id = cor::Thread::Self();

    LockInfoVector& miv = mThreadMap[id].mLockInfo;

    if (miv.empty()) // DEFENSIVE
    {
        printf("Mutex %s registered as unlocked but is not known\n", mutex->GetName().c_str());
        mMutex.Unlock();
        return;
    }

    // this mutex is usually the most recent one registered, but might not be if it
    // is manually Unlocked

    LockInfo& mi = miv.back();
    if (mi.mAccessedMutex != mutex)
    {
        // search rest of stack for this mutex
        for (size_t i = 0; i < miv.size(); i++)
        {
            if (miv[i].mAccessedMutex == mutex)
            {
                miv.erase(miv.begin() + i);

                mMutex.Unlock();
                return;
            }
        }
        printf("Mutex %s registered as unlocked but is not found\n", mutex->GetName().c_str());
        printf("Exiting\n");
        exit(0);
    }
    miv.pop_back();

    mMutex.Unlock();
}

void
ThreadAnalysis::RegisterTryWriteLock(RWLock* rwLock, const std::string& context)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif

    mMutex.Lock();

    pthread_t id = cor::Thread::Self();

    LockInfoVector& miv = mThreadMap[id].mLockInfo;

    LockInfo newInfo;
    newInfo.mState = eWriteBlocked;
    newInfo.mAccessedRWLock = rwLock;
    newInfo.mContext = context;
    miv.push_back(newInfo);

    mMutex.Unlock();
}

void
ThreadAnalysis::RegisterTryReadLock(RWLock* rwLock, const std::string& context)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif

    mMutex.Lock();

    pthread_t id = cor::Thread::Self();

    LockInfoVector& miv = mThreadMap[id].mLockInfo;

    LockInfo newInfo;
    newInfo.mState = eReadBlocked; // XXX only difference with above function!
    newInfo.mAccessedRWLock = rwLock;
    newInfo.mContext = context;
    miv.push_back(newInfo);

    mMutex.Unlock();
}

void
ThreadAnalysis::RegisterGotWriteLock(RWLock* rwLock, const std::string& context)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif

    mMutex.Lock();

    pthread_t id = cor::Thread::Self();

    LockInfoVector& miv = mThreadMap[id].mLockInfo;

    if (miv.empty()) // DEFENSIVE
    {
        printf("RWLock %s registered as locked but is not known\n", rwLock->GetName().c_str());
        mMutex.Unlock();
        return;
    }

    // this mutex is normally the most recent one registered, but in a race
    // condition, a thread could get into the TryLock state, get switched out,
    // the new thread get into the TryLock state and also get swtiched out, and
    // then the original thread proceed to locking the mutex. Possible, but more
    // likely a programming error, and unlikely all around.
    LockInfo& mi = miv.back();
    if (mi.mAccessedRWLock != rwLock) // DEFENSIVE
    {
        printf("RWlock %s registered as locked but is not the most recent (possibly OK, but unlikely)\n", rwLock->GetName().c_str());
        //  XXX search rest of stack for this mutex
        printf("Exiting\n");
        exit(0);
    }
    mi.mState = eWriteLocked;
    mi.mContext = context;

    mMutex.Unlock();
}

void
ThreadAnalysis::RegisterGotReadLock(RWLock* rwLock, const std::string& context)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif

    mMutex.Lock();

    pthread_t id = cor::Thread::Self();

    LockInfoVector& miv = mThreadMap[id].mLockInfo;

    if (miv.empty()) // DEFENSIVE
    {
        printf("RWLock %s registered as locked but is not known\n", rwLock->GetName().c_str());
        mMutex.Unlock();
        return;
    }

    // this mutex is normally the most recent one registered, but in a race
    // condition, a thread could get into the TryLock state, get switched out,
    // the new thread get into the TryLock state and also get swtiched out, and
    // then the original thread proceed to locking the mutex. Possible, but more
    // likely a programming error, and unlikely all around.
    LockInfo& mi = miv.back();
    if (mi.mAccessedRWLock != rwLock) // DEFENSIVE
    {
        printf("RWlock %s registered as locked but is not the most recent (possibly OK, but unlikely)\n", rwLock->GetName().c_str());
        //  XXX search rest of stack for this mutex
        printf("Exiting\n");
        exit(0);
    }
    mi.mState = eReadLocked; // XXX only difference with above function!
    mi.mContext = context;

    mMutex.Unlock();
}

void
ThreadAnalysis::RegisterUnLock(RWLock* rwLock)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif

    mMutex.Lock();

    pthread_t id = cor::Thread::Self();

    LockInfoVector& miv = mThreadMap[id].mLockInfo;

    if (miv.empty()) // DEFENSIVE
    {
        printf("RWLock %s registered as unlocked but is not known\n", rwLock->GetName().c_str());
        mMutex.Unlock();
        return;
    }

    // this mutex is usually the most recent one registered, but might not be if it
    // is manually Unlocked

    LockInfo& mi = miv.back();
    if (mi.mAccessedRWLock != rwLock)
    {
        // search rest of stack for this mutex
        for (size_t i = 0; i < miv.size(); i++)
        {
            if (miv[i].mAccessedRWLock == rwLock)
            {
                miv.erase(miv.begin() + i);

                mMutex.Unlock();
                return;
            }
        }
        printf("RWLock %s registered as unlocked but is not found\n", rwLock->GetName().c_str());
        printf("Exiting\n");
        exit(0);
    }
    miv.pop_back();

    mMutex.Unlock();
}

void
ThreadAnalysis::RegisterThread(Thread* thread)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif

    mMutex.Lock();

    // DEFENSIVE -- check to see if thread is already registered
    // this can happen if Start() is called more than once
    ThreadMap::const_iterator i = mThreadMap.find(thread->GetId());
    if (i != mThreadMap.end())
    {
        printf("Thread id 0x%lx ('%s') is registered already (0x%lx)\n",
               thread->GetId(),
               thread->GetName().c_str(),
               i->first);
        mMutex.Unlock();
        return;
    }

    mThreadMap[thread->GetId()].mThread = thread;
    mMutex.Unlock();
}

void
ThreadAnalysis::UnregisterThread(Thread* thread)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif

    // This could be called from a destructor of a Thread that was never
    // started; ignore this.
    if (thread->GetId() == 0)
    {
        return;
    }

    mMutex.Lock();

    // DEFENSIVE -- make sure thread is registered
    ThreadMap::iterator i = mThreadMap.find(thread->GetId());
    if (i == mThreadMap.end())
    {
        printf("Thread id 0x%lx ('%s') is not registered\n", thread->GetId(), thread->GetName().c_str());
        mMutex.Unlock();
        return;
    }

    if (!i->second.mLockInfo.empty())
    {
        const LockInfoVector& miv = i->second.mLockInfo;
        printf("Thread id 0x%lx ('%s') unregistering with %ld mutexes\n", thread->GetId(), thread->GetName().c_str(), miv.size());
        for (size_t j = 0; j < miv.size(); j++)
        {
            printf("   %s\n", miv[j].GetLiteral().c_str());
        }
    }

    mThreadMap.erase(i);
    mMutex.Unlock();
}

bool
ThreadAnalysis::PushContext(pthread_t thread, const std::string& newContext)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return false;
#endif

    mMutex.Lock();
    ThreadMap::iterator i = mThreadMap.find(thread);

    if (i == mThreadMap.end())
    {
        // this could happen if the context is pushed for a non-Thread thread (like main)
        printf("Can't push context '%s', thread 0x%lx is not registered\n", newContext.c_str(), (long)thread);
        mMutex.Unlock();
        return false;
    }
    i->second.mContextStack.push_back(newContext);
    mMutex.Unlock();
    return true;
}

void
ThreadAnalysis::PopContext(pthread_t thread)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif

    mMutex.Lock();
    ThreadMap::iterator i = mThreadMap.find(thread);

    if (i == mThreadMap.end()) // DEFENSIVE -- should never happen
    {
        printf("Can't pop context, thread is not registered\n");
        mMutex.Unlock();
        printf("Exiting\n");
        exit(0);
    }

    i->second.mContextStack.pop_back();
    mMutex.Unlock();
}

std::string
ThreadAnalysis::GetThreadName(pthread_t id) const
{
#ifndef THREAD_ANALYSIS_ENABLE
    return "Must re-build app with THREAD_ANALYSIS_ENABLE for GetThreadName to work";
#endif
    mMutex.Lock();

    // if thread is unknown to us, or the thread object is NULL (because,
    // for instance, the thread is not a cor::Thread)
    std::string s = "unknown";

    ThreadMap::const_iterator i = mThreadMap.find(id);
    if (i != mThreadMap.end())
    {
        Thread* t = i->second.mThread;
        if (t != NULL)
            s = t->GetName();
    }

    mMutex.Unlock();
    return s;
}

void
ThreadAnalysis::Dump(bool onlyThreadsThatHoldLocks) const
{
#ifndef THREAD_ANALYSIS_ENABLE
    printf("Must re-build app with THREAD_ANALYSIS_ENABLE");
#endif
    mMutex.Lock();

    ThreadMap::const_iterator i = mThreadMap.begin();
    for (; i != mThreadMap.end(); i++)
    {
        const LockInfoVector& miv = i->second.mLockInfo;

        // print nothing for threads that have no locks at the moment
        if (miv.empty())
            continue;

        // if thread is simply blocked on something and holding no lock itself,
        // it isn't part of a deadlock cycle.
        if (onlyThreadsThatHoldLocks)
        {
            if (miv.size() == 1)
                if (miv[0].mState == eBlocked)
                    continue;
        }

        //printf("0x%lx Thread '%s':\n", i->first, i->second.mThread == NULL ? "unknown" : i->second.mThread->GetName().c_str());
        printf("0x%lx Thread '%s':\n", (unsigned long)i->first, i->second.GetLiteral().c_str());
        for (size_t j = 0; j < miv.size(); j++)
        {
            printf("   %s\n", miv[j].GetLiteral().c_str());
        }
    }

    mMutex.Unlock();
}

// singleton
ThreadAnalysis&
ThreadAnalysis::Get()
{
    static ThreadAnalysis instance;
    return instance;
}

class Decoder : public std::map<ThreadAnalysis::LockState, std::string>
{
public:
    Decoder()
    {
        (*this)[ThreadAnalysis::eLocked] = "Held";
        (*this)[ThreadAnalysis::eBlocked] = "Blocked";
        (*this)[ThreadAnalysis::eReadBlocked] = "ReadBlocked";
        (*this)[ThreadAnalysis::eWriteBlocked] = "WriteBlocked";
        (*this)[ThreadAnalysis::eReadLocked] = "ReadHeld";
        (*this)[ThreadAnalysis::eWriteLocked] = "WriteHeld";
    }
} decoder;

std::string
ThreadAnalysis::StateToString(LockState lockState)
{
    return decoder.at(lockState);
}

std::string
ThreadAnalysis::LockInfo::GetLiteral() const
{
    std::string r;
    if (mAccessedMutex != NULL)
    {
        r = "Mutex " + mAccessedMutex->GetName();
    }
    else if (mAccessedRWLock != NULL)
    {
        r = "RWLock " + mAccessedRWLock->GetName();
    }
    else // DEFENSIVE
    {
        printf("NULL mutex and rwlock in GetLiteral\n");
        printf("Exiting\n");
        exit(0);
    }

    if (!mContext.empty())
        r += " (at " + mContext + ")";
    r += std::string(" -- ") + StateToString(mState);

    return r;
}

std::string
ThreadAnalysis::ThreadInfo::GetLiteral() const
{
    std::string r = mThread == NULL ? "unknown" : mThread->GetName();

    for (size_t i = 0; i < mContextStack.size(); i++)
    {
        if (i == 0)
            r += "::";
        r += mContextStack[i];
        if (i != mContextStack.size() - 1)
            r += "->";
    }
    return r;
}

ThreadAnalysis::ThreadAnalysis() : mMutex("ThreadAnalysis")
{}



ContextScope::ContextScope(const std::string& context)
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif
    mArmed = ThreadAnalysis::Get().PushContext(cor::Thread::Self(), context);
}

ContextScope::~ContextScope()
{
#ifndef THREAD_ANALYSIS_ENABLE
    return;
#endif
    if (mArmed)
        ThreadAnalysis::Get().PopContext(cor::Thread::Self());
}

} // end namespace
