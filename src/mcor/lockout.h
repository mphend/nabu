//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor_lockout_included__
#define __mcor_lockout_included__


#include <set>

#include "mmutex.h"

/* template class Lockout
 *
 * maintains a set of unique keys, with callbacks to:
 *    OnLock on the transition from empty to non-empty
 *    OnUnlock() on the transition from non-empty to empty
 *
 * Named after the idea of each worker having their own lock to make sure a piece
 * of machinery does not start without their permission (because, for instance, they
 * are crawling around inside it).
 */

namespace cor {
template <class KeyType>
class Lockout
{
public:
    Lockout(const std::string& name);
    virtual ~Lockout();

    void Lock(const KeyType& key);
    void Unlock(const KeyType& key);

    bool IsLocked() const;

protected:
    typedef std::set<KeyType> KeySet;
    mutable MMutex mMutex;
    KeySet mKeySet;

    // called on transition from empty to non-empty
    virtual void OnLock() {}
    // called on transition from non-empty to empty
    virtual void OnUnlock() {}
};

template <class KeyType>
Lockout<KeyType>::Lockout(const std::string& name) :
    mMutex(name + "_lockout_mutex")
{
}

template<class KeyType>
Lockout<KeyType>::~Lockout()
{}

template<class KeyType>
void
Lockout<KeyType>::Lock(const KeyType &key)
{
    cor::ObjectLocker locker(mMutex);

    bool onLock = mKeySet.empty();
    mKeySet.insert(key);
    if (onLock)
        OnLock();
}

template<class KeyType>
void
Lockout<KeyType>::Unlock(const KeyType &key)
{
    cor::ObjectLocker locker(mMutex);

    bool notEmpty = !mKeySet.empty();
    mKeySet.erase(key);
    if (notEmpty && mKeySet.empty())
        OnUnlock();
}

template<class KeyType>
bool
Lockout<KeyType>::IsLocked() const
{
    cor::ObjectLocker locker(mMutex);
    return !mKeySet.empty();
}


}

#endif
