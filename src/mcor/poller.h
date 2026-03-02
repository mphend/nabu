//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor_poller__
#define __mcor_poller__

#include <set>

#include "mexception.h"
#include "mmutex.h"
#include "mthread.h"

namespace cor
{


/** class Pollable : something that implements "void Poll()"
 */

class Pollable
{
public:
    virtual ~Pollable() {}
    virtual void Poll() = 0;
};

/** class PollingThread : calls Poll() on a collection at a certain
 *  frequency
 *
 *  Threadsafe.
 */
class PollingThread : public cor::Thread
{
public:
    PollingThread(const std::string& name, int pollingPeriodSeconds);
    virtual ~PollingThread() {  Stop(); }

    // Adjust the collection of things Poll()ed
    void AddPollable(Pollable* p);
    void RemovePollable(Pollable* p);

protected:
    int mPollingPeriodSeconds;
    mutable cor::MMutex mMutex;
    typedef std::set<Pollable*> Imp;
    Imp mImp;

    void ThreadFunction();
};




}

#endif
