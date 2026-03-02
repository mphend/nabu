//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include "access.h"

#define DEBUG 1
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {
namespace server {

SelectProxyThread::SelectProxyThread(nabu::AccessImp* access,
                                     const std::set<MetaKey>& selectColumns,
                                     WrittenColumnsMap& writtenColumnsMap,
                                     int waitSeconds) :
    cor::Thread("SelectProxyThread"),
    mAccess(access),
    mSelectColumns(selectColumns),
    mTimeout(waitSeconds),
    mMutex("SelectProxyThreadMutex", false), // non-recursive
    mCondition("SelectProxyCondition", mMutex),
    mComplete(false),
    mResult(eComplete),
    mWcm(writtenColumnsMap),
    mError(false),
    mAbort(false)
{
}

SelectProxyThread::~SelectProxyThread()
{
    DEBUG_LOCAL("~SelectProxyThread%s\n", "");
    mAbort = true;

    DEBUG_LOCAL("~SelectProxyThread 1%s\n", "");
    mCondition.Signal();
    DEBUG_LOCAL("~SelectProxyThread 2%s\n", "");
    mAccess->AbortSelect();
    DEBUG_LOCAL("~SelectProxyThread 3%s\n", "");
    Stop(); // joins
    DEBUG_LOCAL("~SelectProxyThread 4%s\n", "");
}


SelectResult
SelectProxyThread::Wait(const cor::Time& waitTime)
{
    DEBUG_LOCAL("SelectProxyThread::Wait()%s\n", "");
    cor::ObjectLocker ol(mMutex);

    // if an exception occurred, throw it now
    if (mError)
        throw mException;

    // if Select completed already, return the result of it
    if (mComplete)
        return mResult;

    // wait this thread to see if the Select completes, up to waitTime
    bool b = mCondition.Wait(waitTime.Seconds() * 1000); // convert to msec
    // check for abort; this object is being destroyed. If this occurs,
    // it is likely that the client has gone away, and this thread (the
    // server handler) will be replying to a client that is no longer there.
    if (mAbort)
        return eAbort;

    // condition was signaled; return the result
    if (b)
        return mResult;

    // timeout, but only of proxy (inner loop); caller should wait again
    return eWaitAgain;
}

void
SelectProxyThread::ThreadFunction()
{
    DEBUG_LOCAL("SelectProxyThread 0x%lx started\n", cor::Thread::Self());
    try
    {
        // note the use of the base-class (AccessImp) specification, to
        // prevent calling the child class 'Select' recursively here.
        SelectResult b = mAccess->AccessImp::Select(mSelectColumns,
                                 mWcm,
                                 mTimeout.IsInfinite() ? -1 : mTimeout.Seconds());
        DEBUG_LOCAL("SelectProxyThread 0x%lx select has completed\n", cor::Thread::Self());
        cor::ObjectLocker ol(mMutex);

        mComplete = true;
        mResult = b;

        DEBUG_LOCAL("SelectProxyThread 0x%lx 1...\n", cor::Thread::Self());
        mCondition.Signal();
        DEBUG_LOCAL("SelectProxyThread 0x%lx 2...\n", cor::Thread::Self());

    } catch (const cor::Exception& err)
    {
        mError = true;
        mException = err;
        printf("SelectProxyThread Error: %s\n", err.what());
    }
    DEBUG_LOCAL("SelectProxyThread 0x%lx dropping off end...\n", cor::Thread::Self());
}


Access::Access(std::shared_ptr<BranchImp> branch,
                 const std::string& tagName,
                 const muuid::uuid& handle,
                 const cor::TimeRange& timeRange,
                 const cor::TimeRange& snappedRange,
                 AccessType type,
                 int selectTimeoutSec) :
        AccessImp(branch, tagName, handle, timeRange, snappedRange, type),
        mSelectTimeoutSeconds(selectTimeoutSec)
{
    DEBUG_LOCAL("Creating server Access without select timeout of %d seconds\n", selectTimeoutSec);
}

Access::~Access()
{
    DEBUG_LOCAL("~Access (server) %s\n", Print().c_str());
}

SelectResult
Access::Select(const std::set<MetaKey>& selectColumns,
                          WrittenColumnsMap& writtenColumnsMap,
                          int waitSeconds)
{
    DEBUG_LOCAL("Access(server)::Select -- %d seconds\n", waitSeconds);
    std::shared_ptr<SelectProxyThread> thread;
    {
        cor::ObjectLocker ol(mMutex);
        if (!mSelectProxyThread)
        {
            mSelectProxyThread.reset(new SelectProxyThread(this,
                                                           selectColumns,
                                                           writtenColumnsMap,
                                                           waitSeconds));
            mSelectProxyThread->Start();
        }
        thread = mSelectProxyThread;
    }
    nabu::SelectResult result = thread->Wait(mSelectTimeoutSeconds);

    /*printf("   waited %d seconds, latest result is %s\n",
                      mSelectTimeoutSeconds,
                      result == nabu::eAbort ? "aborted" :
                      result == nabu::eTimeout ? "timeout" :
                      result == nabu::eWaitAgain ? "waitagain" :
                      "complete");*/

    return result;
}


} // end namespace
} // end namespace
