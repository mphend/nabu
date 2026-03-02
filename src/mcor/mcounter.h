//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor_mcounter__
#define __mcor_mcounter__

#include <sys/time.h>
#include <signal.h>

#include "mexception.h"
#include "mmutex.h"

namespace cor
{

/** class Counter : manages count like a watchdog timer where the count
 *  can be exceeded or reset.
 *
 */
class Counter
{
public:
    Counter(int max);
    virtual ~Counter();

    bool IsExpired() const;
    bool IsExpired(double percentage) const;
    int GetMax() const;

    // resets count to zero, optionally setting a new maximum count
    void Reset(int max = 0);
    // advances count, returns true when transitions to expired (first time only)
    bool Tick();

private:
    mutable cor::MMutex mMutex;
    int mMax;
    int mCount;
};
    
    
}

#endif
