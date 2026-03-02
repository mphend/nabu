//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_MCOR_THREAD_ANALYSIS_INCLUDED
#define GICM_MCOR_THREAD_ANALYSIS_INCLUDED

#include <map>
#include <vector>

#include "mmutex.h"
#include "mthread.h"
#include "rwlock.h"


// The following must be defined and then the app re-built for thread
// analysis to be enabled.
//#define THREAD_ANALYSIS_ENABLE

namespace cor {


/** class ContextScope : manually added context to act similar to
 *  a call stack in debugging thread issues.
 */
class ContextScope
{
public:
    ContextScope(const std::string& context);
    virtual ~ContextScope();
private:
    bool mArmed;
};


/** class ThreadAnalysis : tracks state of synchronization objects
 *
 *  This only tracks locking and unlocking performed by ObjectLocker and
 *  ReadLocker/WriteLocker objects. It will handle non-cor::Thread threads
 *  (such as main) but won't have context information for those threads.
 *
 *  The intent of this class is to catch deadlocks and then dump the
 *  thread state in order to find problematic lock ordering. This is most
 *  easily done through catching a signal. See tests/thread_analysis.cpp
 *  for an example of usage.
 */
class ThreadAnalysis
{
public:

    // ObjectLocker interface
    void RegisterTryLock(MMutex* mutex, const std::string& context);
    void RegisterGotLock(MMutex* mutex, const std::string& context);
    void RegisterUnLock(MMutex* mutex);

    // ReadLocker and WriteLocker interface
    void RegisterTryWriteLock(RWLock* rwLock, const std::string& context);
    void RegisterTryReadLock(RWLock* rwLock, const std::string& context);
    void RegisterGotWriteLock(RWLock* rwLock, const std::string& context);
    void RegisterGotReadLock(RWLock* rwLock, const std::string& context);
    void RegisterUnLock(RWLock* rwLock);

    // Thread interface
    void RegisterThread(Thread* thread);
    void UnregisterThread(Thread* thread);

    // ContextScope interface
    bool PushContext(pthread_t thread, const std::string& newContext);
    void PopContext(pthread_t thread);

    // fetches name of thread
    std::string GetThreadName(pthread_t) const;

    // dump the current state to std out
    // if you are looking for the cause of a deadlock, then you want to filter
    // out threads that hold no locks (default).
    // if you are wondering what lock is keeping a particular thread from running
    // then show all the locks.
    void Dump(bool onlyThreadsThatHoldLocks = true) const;

    // singleton
    static ThreadAnalysis& Get();

    enum LockState { eBlocked, eReadBlocked, eWriteBlocked, eLocked, eReadLocked, eWriteLocked };
    static std::string StateToString(LockState state);

private:
    ThreadAnalysis();

    // this object's own synchronization
    mutable MMutex mMutex;

    struct LockInfo {
        LockInfo() : mAccessedMutex(NULL), mAccessedRWLock(NULL) {}
        MMutex* mAccessedMutex;
        RWLock* mAccessedRWLock;
        LockState mState;
        std::string mContext;

        std::string GetLiteral() const;
    };
    typedef std::vector<LockInfo> LockInfoVector;

    typedef std::vector<std::string> ContextVector;

    struct ThreadInfo {
        ThreadInfo() : mThread(NULL) {};
        Thread* mThread;
        LockInfoVector mLockInfo;
        ContextVector mContextStack;

        std::string GetLiteral() const;
    };
    typedef std::map<pthread_t, ThreadInfo> ThreadMap;

    ThreadMap mThreadMap;

    // prohibit
    ThreadAnalysis(ThreadAnalysis& other);
    void operator=(const ThreadAnalysis& other);
    
};



} // end namespace

#endif
