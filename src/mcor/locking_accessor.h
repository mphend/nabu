//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __gicm_mcor_locking_accessor__
#define __gicm_mcor_locking_accessor__

#include <memory>

#include "mmutex.h"
#include "rwlock.h"


namespace cor
{

template <class Lockable, class ScopeLocker, class Ptr>
class LockingAccessor : public std::shared_ptr<Ptr>
{
public:
    LockingAccessor() {}
    LockingAccessor(Lockable& lockable, Ptr* ptr) :
        std::shared_ptr<Ptr>(ptr),
        mLocker(lockable)
    {

    }

    LockingAccessor(Lockable& lockable, std::shared_ptr<Ptr>& ptr) :
            std::shared_ptr<Ptr>(ptr),
            mLocker(lockable)
    {

    }

    // the policy of ScopeLocker copy constructor must be to safely subsume
    // ownership of the Lockable without touching it
    LockingAccessor(LockingAccessor& other) :
            mLocker(other.mLocker)
    {}

    void Subsume(Lockable& lockable, std::shared_ptr<Ptr>& ptr)
    {
        mLocker.Subsume(lockable);
        std::shared_ptr<Ptr>::operator=(ptr);
    }

    /*void operator=(const LockingAccessor& other)
    {
        *this = *other; // smart pointer copy
        mLocker = other.mLocker;
    }*/

    virtual ~LockingAccessor()
    {
    }

private:
    ScopeLocker mLocker;
};

/*
template <class Lockable, class Ptr>
class ReadLockingAccessor : std::shared_ptr<Ptr>
{
public:
    ReadLockingAccessor(Lockable& lockable, Ptr* ptr) :
            std::shared_ptr<Ptr>(ptr),
            mLocker(lockable)
    {

    }

    virtual ~ReadLockingAccessor()
    {
    }

private:
    cor::ReadLocker mLocker;
};


template <class Lockable, class Ptr>
class WriteLockingAccessor : std::shared_ptr<Ptr>
{
public:
    WriteLockingAccessor(Lockable& lockable, Ptr* ptr) :
            std::shared_ptr<Ptr>(ptr),
            mLocker(lockable)
    {

    }

    virtual ~WriteLockingAccessor()
    {
    }

private:
    cor::WriteLocker mLocker;
};
*/

}

#endif
