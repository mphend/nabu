//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor_profiler__
#define __mcor_profiler__

#include <iostream>
#include <map>
#include <time.h>
#include <string>

#include <mcor/mmutex.h>

namespace cor
{

/** Class profiler : use to roughly figure out where time is going; counts
    wall clock time, not CPU time.

    Also can tally counts of events.

 
    Use as so:

     cor::Profiler p;

     p.Start("t1");
     for( size_t i = 0; i < 100000000; i++);
     p.Stop("t1");

     p.Start("a");
     for( size_t i = 0; i < 100000000; i++);
     p.Switch("b"); // avoids Stop("a");
     sleep(1);
     p.Stop("b");
 
     // note nesting is OK; but not with same key

     p.Start("t2");
     for( size_t i = 0; i < 200000000; i++)
         if (( i % 1000) == 0)
             p.Count("Millennium");
     p.Start("t3");
     for( size_t i = 0; i < 200000000; i++);
     p.Stop("t2");
     p.Stop("t3");

     std::cout << p << std::endl;
 */

class Profiler
{
public:
    Profiler();
    virtual ~Profiler();

    void Start(const std::string& what);
    void Stop(const std::string& what);

    void Count(const std::string& what);

    void Clear();

    // dump the results
    friend std::ostream& operator<<(std::ostream&, const Profiler& p);

private:
    struct timespec mClock;

    // bookkeeping
    struct imp {
        struct timespec mStart;    // when we started last
        struct timespec mDuration; // total so far
    };

    mutable cor::MMutex mMutex;
    typedef std::map<std::string, imp> BaseMap;
    BaseMap mTally;

    typedef std::map<std::string, size_t> CountMap;
    CountMap mCounts;
};

std::ostream& operator<<(std::ostream&, const Profiler& p);

}

#endif
