//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor_mtimer__
#define __mcor_mtimer__

#include <sys/time.h>
#include <signal.h>

#include <mcor/mmutex.h>
#include "mexception.h"

namespace cor
{

/** class Timer : monotonic timer
 *
 *  This class is threadsafe
 */

class Timer
{
public:
    Timer(double timeoutSec);

    // resets the timer; positive numbers are new timeout
    void Reset(double timeoutSec = 0.0);

    // returns seconds since last Reset(), or since constructed
    double ElapsedSeconds();
    // returns true if seconds since last Reset() equal or exceed 'seconds'
    bool HasElapsedSeconds(double seconds);
    // returns seconds remaining.  This will be negative after it IsExpired()
    double RemainingSeconds();
    // returns whether timer has expired (RemainingSeconds() <= 0.0)
    bool IsExpired();

    // returns current timeout
    double GetTimeout() const;

private:
    mutable cor::MMutex mMutex;
    double mTimeoutSec;
    struct timespec mResetTime;
};


/** class WallClockTimer : a timer that expires on an integer number of
 *  wall-clock based seconds, plus a partial delay within that second.
 *
 *  This class lets you set up a timer that occurs every top-of-second,
 *  half-way through every second, or half-way through every 27th second.
 *
 *  It does not support a timer that goes off twice a second, or every .7 seconds.
 *
 *  Get it?  An integer number of second period, plus a partial second phase.
 *
 *  You must inherit and define Expired() method, which runs in the context of
 *  a signal handler.  So it should handle all exceptions.  Also, in regards to
 *  shared data, I found this advice:
 *
 *  "Pretty much the only shared data you can safely access in a signal handler
 *  is a sig_atomic_t type, in practice other kinds of primitive types [are] usually safe too.
 *  What you really should do in a signal handler is just set a global flag
 *  check that flag elsewhere in code when suitable, and take action."
 *
 */

#ifndef __APPLE__
class WallClockTimer
{
public:
	WallClockTimer(int periodSec, double delayNsec);
	virtual ~WallClockTimer();

	// sets timer period; behavior if called while timer is running is undefined
	void SetPeriod(int periodSec, double delayNsec);

	void StartTimer();
	void StopTimer();

	int GetOverruns() const { return mOverrunCount; }

	int GetCount() const { return mCount; }
	void ResetCount() { mCount = 0; }


private:
	timer_t mTimerId;
	int mPeriodSec;
	double mDelayNsec;
	int mCount;

	static void SignalHandler(int signal, siginfo_t* sigInfo, void* unused) throw();

	virtual void Expired(struct timespec) throw() = 0;

	int mOverrunCount;
};
#endif
    
}

#endif
