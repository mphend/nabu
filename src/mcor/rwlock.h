//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#ifndef __mcor_rwlock_h__
#define __mcor_rwlock_h__

#include <iostream>
#include <string>
#include <sstream>

#include <pthread.h>

#include "mexception.h"

namespace cor {

    
/** class RWLock
 *  Wraps a pthread read write lock.
 *
 *  Note that this primitive, unlike a mutex, will not allow recursion
 *
 */

class RWLock
{
public:
    RWLock(const std::string& name);
    virtual ~RWLock();
    
    void WriteLock();
    void ReadLock();
    void Unlock();

    const std::string& GetName() const { return mName; }
    
private:
    const std::string mName;
    pthread_rwlock_t mRWLock;
};


/** class ReadLocker
 *
 */
    
class ReadLocker
{
public:
    ReadLocker();
    ReadLocker(RWLock& rWLock, const std::string& context = "");
    virtual ~ReadLocker();

    void Subsume(RWLock& rWLock);

private:
    RWLock* mRWLock;
    std::string mContext;
    bool mLocked;
};


/** class WriteLocker
 *
 */

class WriteLocker
{
public:
    WriteLocker();
    WriteLocker(RWLock& rWLock, const std::string& context = "");
    virtual ~WriteLocker();

    void Subsume(RWLock& rWLock);
private:
    RWLock* mRWLock;
    std::string mContext;
    bool mLocked;
};
    
    
}

#endif
