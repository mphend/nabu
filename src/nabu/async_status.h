//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_ASYNC_STATUS_H
#define GICM_NABU_ASYNC_STATUS_H


#include <cstdio>

#include <mcor/mmutex.h>

namespace nabu
{

/** class AsyncStatus : thread-safe handling of progress state in iteration routines
     *
    */
class AsyncStatus
{
public:
    AsyncStatus();
    AsyncStatus(const AsyncStatus& other);

    void operator=(const AsyncStatus& other);

    // total number
    size_t Total() const;           // get
    void Total(size_t x);           // set
    void TotalInc(size_t x);        // increment

    // number processed so far
    size_t Processed() const;       // get
    void Processed(size_t x);       // set
    void ProcessedInc(size_t x);    // increment

    // number identified, altered, etc.
    size_t Count() const;           // get
    void Count(size_t x);           // set
    void CountInc(size_t x);        // increment

    // returns if asynchronous activity is completed
    bool Done() const;
    // returns old running state
    bool SetRunning(bool running);

    void Copy(const AsyncStatus& other);
private:
    mutable cor::MMutex mMutex;
    size_t mTotal;
    size_t mProcessed;
    size_t mCount;
    bool mRunning;
};

} // end namespace

#endif
