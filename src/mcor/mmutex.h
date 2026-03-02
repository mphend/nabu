//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __threader__mmutex__
#define __threader__mmutex__

#include <iostream>
#include <memory>
#include <string>
#include <sstream>

#include <pthread.h>

#include "mexception.h"

namespace cor {

    
/** class MMutex
 *  Wraps a pthread mutex.
 *
 *  If this class isn't obvious to you, you should read up on mutexes elsewhere
 *
 */
class MCondition;
class MMutex
{
public:
    MMutex(const std::string& name, bool recursive = true);
    virtual ~MMutex();

    bool IsRecursive() const { return mRecursive; }
    
    virtual void Lock();
    virtual void Unlock();

    // returns true if lock was obtained, false if already busy
    virtual bool TryLock();

    const std::string& GetName() const { return mName; }
    
protected:
    const std::string mName;
    const bool mRecursive;
    pthread_mutex_t mMutex;
    
    friend class MCondition;
};

typedef std::shared_ptr<MMutex> MMutexPtr;


/** class MPhonyMutex
 *
 *  Stubs out a mutex; useful for debugging
 *
 */
class MPhonyMutex : public MMutex
{
public:
    MPhonyMutex(const std::string& name);
    virtual ~MPhonyMutex();

    void Lock();
    void Unlock();

    bool TryLock();
};


/** class ObjectLocker
 *
 *  XXX: make a template on any type that implements Lock and Unlock
 *
 */
    
class ObjectLocker
{
public:
    ObjectLocker();
    ObjectLocker(MMutex& mutex, const std::string& context = "");
    ObjectLocker(MMutexPtr mutex, const std::string& context = "");

    void Subsume(MMutex& mutex);
    
    virtual ~ObjectLocker();

    void Unlock();
private:
    MMutex* mMutex;
    MMutexPtr mMutexPtr;
    std::string mContext;
    bool mLocked;
};

/** class ObjectTryLocker
 *
 *  Throws a UnlockedException (defined within the class) if the lock
 *  cannot be made because someone else has the lock.
 *
 *  XXX: make a template on any type that implements TryLock and Unlock
 *
 */

class ObjectTryLocker
{
public:
    class UnlockedException : public Exception
    {
        public:
            UnlockedException(const std::string& s) :
                Exception(s.c_str())
            {}
    };

    ObjectTryLocker(MMutex& mutex) : mMutex(mutex)
    {
        if (!mMutex.TryLock())
        {
            std::string s;
            std::ostringstream oss;
            throw UnlockedException(oss.str());
        }
    }

    virtual ~ObjectTryLocker()
    {
        mMutex.Unlock();
    }
private:
    MMutex& mMutex;
};
    
    
}

#endif /* defined(__threader__mmutex__) */
