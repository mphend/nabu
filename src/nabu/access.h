//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_ACCESS__
#define __PKG_NABU_ACCESS__


#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <mcor/timerange.h>
#include <mcor/muuid.h>

#include "interface_types.h"
#include "metakey.h"
#include "wcm.h"


namespace nabu
{

class IOOperation;
class Branch;
class Tag;


/** class Access : contains the information about an ongoing or requested access
 *  of the database, from a data (not security) point of view.
 *
 *  The state of the access (whether it is granted access or not) is the
 *  responsibility of the AccessTable for the Branch that the Access is
 *  addressing. That state is tracked in the entries of the AccessTable
 *  (AccessState), not in this object.
 *
 */
class Access
{
public:
    Access();
    Access(/*const std::string& branchPath,*/
           std::shared_ptr<Branch> branch,
           const muuid::uuid& handle,
           AccessType type,
           const cor::TimeRange& timeRange,
           const std::string& tagName);
    virtual ~Access();

    bool IsRead() const;
    bool IsWrite() const;
    bool Valid() const;
    std::shared_ptr<Branch> GetBranch();
    const std::string GetBranchPath() const;
    muuid::uuid GetHandle() const;
    const std::string GetTagName() const { return mTagName; }
    const cor::TimeRange& GetTimeRange() const { return mTimeRange; }

    // waits up to waitSeconds seconds for access to be ready, and returns
    // true if it is ready or false if it timed out; if waitSeconds is negative,
    // waits indefinitely
    virtual bool Open(int waitSeconds = -1) = 0;
    // convenience method to loop forever on Open while printing a string
    // every few seconds
    void Open(const char* format, ... );
    virtual void Close() = 0;

    // A Read or Write Access can Select after it is Opened on a set of column
    // names. When this occurs successfully, the access will return from Select
    // as soon as a write has occurred inside the time range of the access in
    // any of the selected columns, and the Access will be Active, as though it
    // just returned from a successful Open. It will receive a digest of the
    // time ranges and columns that were written since it entered Select.
    //
    // If Select is unsuccessful, such as when a timeout expires, the Access is
    // no longer Open and operations (including creating an IOOperation) will
    // not succeed. In this state, the Access must continue to call Open/Select
    // until it is successful, or be abandoned.
    SelectResult Select(WrittenColumnsMap& writtenColumnsMap, int waitSeconds = -1);
    virtual SelectResult Select(const std::set<MetaKey>& selectColumns,
                        WrittenColumnsMap& writtenColumnsMap,
                        int waitSeconds = -1) = 0;
    // abort any Select that the access might be doing; returns true if a
    // Select was actually aborted, false otherwise
    virtual bool AbortSelect() = 0;

    // returns the extents of data available in the columns indicated; if selectColumns
    // is empty, return extents of all columns
    virtual void Extents(const std::set<MetaKey>& selectColumns,
                         WrittenColumnsMap& writtenColumnsMap) = 0;
    void Extents(const MetaKey& column, cor::TimeRange& timeRange);

    // keep access open
    virtual void Tick() = 0;

    // disable garbage collection; this is used by database internals only
    virtual void DisableGarbageCollection() = 0;
    virtual void EnableGarbageCollection() = 0;

    // this needs to be defined for template in proc/nabu_server/resource_cache.h
    virtual void GetChildKeys(std::set<std::string>& keys) = 0;

protected:
    //std::string mBranchPath;
    std::shared_ptr<Branch> mBranch;
    muuid::uuid mHandle;
    AccessType mType;
    cor::TimeRange mTimeRange;
    std::string mTagName;

    friend class AccessPtr;
    virtual std::shared_ptr<IOOperation> CreateIOOperation(const cor::TimeRange& timeRange,
                                                           IterationDirection direction = eIterateForwards) = 0;

};

//typedef std::shared_ptr<Access> AccessPtr;

class AccessPtr : public std::shared_ptr<Access>
{
public:
    AccessPtr() : std::shared_ptr<Access>() {}
    AccessPtr(Access* ptr) : std::shared_ptr<Access>(ptr) {}
    AccessPtr(const std::shared_ptr<Access>& other) : std::shared_ptr<Access>(other) {}

    std::shared_ptr<Access>& operator=(const std::shared_ptr<Access>& other) {
        return std::shared_ptr<Access>::operator=(other);
    }
    std::shared_ptr<IOOperation> CreateIOOperation(const cor::TimeRange& timeRange,
                                                   IterationDirection direction = eIterateForwards);
};

}

#endif
