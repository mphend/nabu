//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __threader__mcondition__
#define __threader__mcondition__

#include <iostream>

#include <pthread.h>
#include <string>

#include "mmutex.h"
#include "mtime.h"

namespace cor {

/** class MCondition
 *
 */

class MCondition
{
public:
    MCondition(const std::string& name, MMutex& mutex);
    virtual ~MCondition();

    // returns false on timeout, true otherwise
    bool Wait(int msec) throw();
    bool Wait(const cor::Time& wallClockTime) throw();

    static bool SleepUntil(const cor::Time& wallClockTime) throw();

    void Wait(); // indefinite wait

    void Signal() throw();
    
private:
    const std::string mName;
    pthread_cond_t mCondition;
    MMutex& mMutex;
};

    
}

#endif /* defined(__threader__mcondition__) */
