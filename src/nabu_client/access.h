//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_CLIENT_ACCESS_H
#define GICM_NABU_CLIENT_ACCESS_H

#include <memory>

#include <nabu/access.h>
#include <nabu/data_file.h>
#include <nabu/interface_types.h>
#include <nabu/metakey.h>

#include <mcor/profiler.h>
#include <mcor/mthread.h>
#include <mcor/mtimer.h>
#include <mcor/url.h>


namespace nabu {

class Branch;
class Tag;

namespace client {


class IOOperation;

/** class Access :
 */
class Access : public nabu::Access
{
public:

    Access(const cor::Url& url,
           std::shared_ptr<Branch> branch,
           const muuid::uuid& handle,
           nabu::AccessType type,
           const cor::TimeRange& timeRange,
           const std::string& tagName);
    virtual ~Access();

    std::shared_ptr<nabu::IOOperation> CreateIOOperation(const cor::TimeRange& timeRange,
                                                         IterationDirection direction);

    bool Open(int secWait);
    bool IsOpen() const;

    // negative timeout value means wait forever
    SelectResult Select(const std::set<nabu::MetaKey>& columns,
                        nabu::WrittenColumnsMap& wcm,
                        int waitSeconds = -1);
    bool AbortSelect();
    void Extents(const std::set<MetaKey>& selectColumns, WrittenColumnsMap& wcm);

    void Tick();

    void DisableGarbageCollection();
    void EnableGarbageCollection();

    // this needs to be defined for template in proc/nabu_server/resource_cache.h
    void GetChildKeys(std::set<std::string>& keys) {}

    const muuid::uuid& GetHandle() const;
    const cor::TimeRange& TimeRange() const { return mTimeRange; }

    void Render(Json::Value& v);

    const cor::Url& GetURL()const { return mUrl; }

    std::shared_ptr<Branch> GetBranch() { return mBranch; }

    void Borrow() { mBorrowed = true; mOpen = true; }

protected:
    cor::Url mUrl;
    std::shared_ptr<Branch> mBranch;

    bool mOpen;
    bool mBorrowed;
    cor::Thread* mTickThread;

    // deletes the proxy on the far end
    void Close();

    // XXX make default and copy constructors hidden
    //IOPImp();
    //IOPImp(const IOPImp& other);
    //void operator=(const IOPImp& other);
};

typedef std::shared_ptr<Access> AccessPtr;

}

}

#endif
