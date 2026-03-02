//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <stdio.h>
#include <time.h>

#include "mtime.h"
#include "mtimer.h"

namespace cor {


Timer::Timer(double timeoutSec) : mMutex("Timer"), mTimeoutSec(timeoutSec)
{
    if (timeoutSec <= 0.0)
        throw cor::Exception("Timer::Timer() -- Cannot have a nonpositive timeout in constructor");
    Reset();
}

void
Timer::Reset(double timeoutSec)
{
    cor::ObjectLocker ol(mMutex);
    if (timeoutSec > 0)
        mTimeoutSec = timeoutSec;
    clock_gettime(CLOCK_MONOTONIC, &mResetTime);
}

double
Timer::ElapsedSeconds()
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);

    cor::ObjectLocker ol(mMutex);
    double e = now - mResetTime;
    return e;
}

bool
Timer::HasElapsedSeconds(double seconds)
{
    return ElapsedSeconds() >= seconds;
}

double
Timer::RemainingSeconds()
{
    double e = ElapsedSeconds();

    cor::ObjectLocker ol(mMutex);
    return mTimeoutSec - e;
}

bool
Timer::IsExpired()
{
    return RemainingSeconds() <= 0;
}

double
Timer::GetTimeout() const
{
    cor::ObjectLocker ol(mMutex);
    return mTimeoutSec;
}

#ifndef __APPLE__
WallClockTimer::WallClockTimer(int periodSec, double delayNsec) :
		mTimerId(0),
		mPeriodSec(periodSec),
		mDelayNsec(delayNsec),
		mCount(0),
		mOverrunCount(0)
{
	// Establish handler for timer signal
	{
		struct sigaction sigAction;
		sigAction.sa_flags = SA_SIGINFO;
		sigAction.sa_sigaction = SignalHandler;
		sigemptyset(&sigAction.sa_mask);
		if (sigaction(SIGRTMIN, &sigAction, NULL) == -1)
			throw cor::ErrException("Error in sigaction");
	}

	SetPeriod(mPeriodSec, mDelayNsec);

    // Create the timer
    {
        struct sigevent sigEvent;

        sigEvent.sigev_notify = SIGEV_SIGNAL;
        sigEvent.sigev_signo = SIGRTMIN;
        sigEvent.sigev_value.sival_ptr = this;

        if (timer_create(CLOCK_REALTIME, &sigEvent, &mTimerId) == -1)
            throw cor::ErrException("Error in timer_create");
    }

}

WallClockTimer::~WallClockTimer()
{
	StopTimer();
}

void
WallClockTimer::SetPeriod(int periodSec, double delayNsec)
{
    // make sure period is sane
    if (periodSec < 0)
        throw cor::Exception("Negative wall clock timer period %d", periodSec);
    if (delayNsec < 0)
        throw cor::Exception("Negative wall clock timer delay %f", (float)(delayNsec));
    if (delayNsec >= 1e9)
        throw cor::Exception("Wall clock timer delay %f greater than a full second", (float)(delayNsec));

    mPeriodSec = periodSec;
    mDelayNsec = delayNsec;
}

void
WallClockTimer::StartTimer()
{
	struct timespec spec;
	int rc = clock_gettime(CLOCK_REALTIME, &spec);
	if (rc == -1)
		throw cor::ErrException("StartTimer::clock_gettime");

	// compute the nanosecond portion of the amount of time to sleep, based
	// on how much time remains until the next top-of-second, the desired
	// delay after top-of-second, and a portion of the error.
	//
	// (The error removal ["correction", below] part of this is trying to adjust
	// to the latency that is in the timer system.  The timer is guaranteed
	// to be later, and never early.  Given the granularity of the timer mechanism,
	// this might be worse than useless.)
	long nsecSleep = 0; // nanosecond portion of next sleep
	const double pllGain = 0; // proportional
	long nsecAfterTOS = spec.tv_nsec;
	long nsecNext = 1e9 + mDelayNsec - nsecAfterTOS;
	long correction = (long)(pllGain*(nsecAfterTOS - mDelayNsec));
	nsecSleep = nsecNext - correction;
	if (nsecSleep <= 0)
		nsecSleep = nsecNext;

	struct itimerspec timerSpec;
	timerSpec.it_value.tv_sec = mPeriodSec;
	// if we are waiting more than a second, subtract one to account for the partial second
	// we will wait; for instance, to wait 7 seconds, we want to set a timer for 6.9-odd seconds
	if (timerSpec.it_value.tv_sec > 0)
		timerSpec.it_value.tv_sec--;
	timerSpec.it_value.tv_nsec = nsecSleep;
	timerSpec.it_interval.tv_sec = timerSpec.it_value.tv_sec;
	timerSpec.it_interval.tv_nsec = timerSpec.it_value.tv_nsec;

	if (timer_settime(mTimerId, 0, &timerSpec, NULL) == -1)
		throw cor::ErrException("WallClockTimer::StartTimer::timer_settime");
}

void
WallClockTimer::StopTimer()
{
    if (mTimerId != 0)
        timer_delete(mTimerId);
    mTimerId = 0;
}

void
WallClockTimer::SignalHandler(int signalUnused, siginfo_t *sigInfo, void *unused) throw()
{
	WallClockTimer* wct = (WallClockTimer*)(sigInfo->si_value.sival_ptr);

	int oc = timer_getoverrun(wct->mTimerId);
	wct->mOverrunCount += oc;
	wct->mCount++;

	// get the current time and invoke the callback
	{
		struct timespec spec;
		try
		{
			int rc = clock_gettime(CLOCK_REALTIME, &spec);
			if (rc == -1)
				throw cor::ErrException("clock_gettime");
		} catch (const cor::ErrException& err)
		{
			// DEFENSIVE -- this should not occur unles CLOCK_REALTIME isn't available
			// Can't throw in a signal handler, I think, so just print and hope someone
			// sees it.
			printf("%s\n", err.what());
		}

		try
		{
		    wct->Expired(spec);
		}
		catch (const std::exception& err)
		{
			// DEFENSIVE -- derived class should handle
			// Can't throw in a signal handler, I think, so just print and hope someone
			// sees it.
			printf("WallClockTimer::Expired uncaught exception: %s\n", err.what());
		}
		catch (...)
		{
			// DEFENSIVE -- derived class should handle
			// Can't throw in a signal handler, I think, so just print and hope someone
			// sees it.
			printf("WallClockTimer::Expired uncaught exception: unknown\n");
		}

	}

	// arm the timer again; this fetches the current time again, in order to account
	// for anything lengthy that may have occurred in the Expired() callback.
	try
	{
		wct->StartTimer();
	} catch (const cor::ErrException& err)
	{
		// DEFENSIVE -- this should not occur unles CLOCK_REALTIME isn't available
		// Can't throw in a signal handler, I think, so just print and hope someone
		// sees it.
		printf("%s\n", err.what());
	}
}
#endif

}
