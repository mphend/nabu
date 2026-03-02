//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_ACCESS_H
#define GICM_NABU_SERVER_ACCESS_H


#include <nabu/database_imp.h>

namespace nabu {
namespace server {


/* SelectProxyThread : this class calls Select on behalf of a remote
 *   thread, blocking for as long as desired. The remote thread will
 *   block for a shorter amount of time to wait for the Select to
 *   complete, and can do this multiple times. The presence of this
 *   thread avoids race conditions if this is done.
 */
class SelectProxyThread : public cor::Thread
{
public:
    SelectProxyThread(nabu::AccessImp* access,
                      const std::set<MetaKey>& selectColumns,
                      WrittenColumnsMap& writtenColumnsMap,
                      int waitSeconds = -1);
    virtual ~SelectProxyThread();

    SelectResult Wait(const cor::Time& waitTime);

protected:
    nabu::AccessImp* mAccess;

    const std::set<MetaKey>& mSelectColumns;
    const cor::Time mTimeout;

    mutable cor::MMutex mMutex;
    cor::MCondition mCondition;
    bool mComplete;
    SelectResult mResult;
    WrittenColumnsMap& mWcm;
    bool mError;
    bool mAbort;
    cor::Exception mException;

    void ThreadFunction();
};


/** class Access : an AccessImp with a SelectProxyThread to
 *  handle remote blocking on a Select
 */
class Access : public AccessImp
{
public:
    Access(std::shared_ptr<BranchImp> branch,
    const std::string& tagName,
    const muuid::uuid& handle,
    const cor::TimeRange& timeRange,
    const cor::TimeRange& snappedRange,
            AccessType type,
            int selectTimeoutSec);

    ~Access();

protected:

    int mSelectTimeoutSeconds;

    SelectResult Select(const std::set<MetaKey>& selectColumns, WrittenColumnsMap& writtenColumnsMap, int msecWait = -1);

    // this object must be accessed while holding AccessImp mMutex
    std::shared_ptr<SelectProxyThread> mSelectProxyThread;
};

}
}
#endif
