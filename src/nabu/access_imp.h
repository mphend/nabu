//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef PKG_NABU_ACCESS_IMP_INCLUDED
#define PKG_NABU_ACCESS_IMP_INCLUDED


#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <mcor/mcondition.h>
#include <mcor/timerange.h>


#include <nabu/access.h>
#include <nabu/metadata_table.h>

namespace nabu
{

class AccessTable;
class BranchImp;
class DatabaseImp;
class IOOperationImp;
class Tag;


/** class AccessImp : contains the information about an ongoing or requested access
 *  of the database, from a data (not security) point of view.
 *
 *  The state of the access (whether it is granted access or not) is the
 *  responsibility of the AccessTable for the Branch that the Access is
 *  addressing. That state is tracked in the entries of the AccessTable
 *  (AccessState), not in this object.
 *
 */
class AccessImp : public Access
{
public:
    virtual ~AccessImp();

    bool IsRead() const { return mType == eRead; }
    bool IsWrite() const { return mType == eWrite; }
    bool Valid() const { return mType != eInvalid; }

    muuid::uuid GetHandle() const { return mHandle; }
    const cor::TimeRange& GetTimeRange() const { return mTimeRange; }
    const cor::TimeRange& GetSnappedTimeRange() const { return mSnapped; }

    void AddOperation(IOOperationImp* iop);
    void RemoveOperation(IOOperationImp* iop);

    std::string Print() const;

    std::shared_ptr<BranchImp> GetBranchImp() { return mBranchImp; }

    const WrittenColumnsMap& GetWrittenColumns(cor::TimeRange& extents) const;

    cor::MMutexPtr GetColumnMutex(const MetaKey& column);

    // ---- Access methods
    std::shared_ptr<IOOperation> CreateIOOperation(const cor::TimeRange& timeRange,
                                                   IterationDirection direction);
    bool Open(int waitSeconds = -1);
    void Close();
    SelectResult Select(const std::set<MetaKey>& selectColumns,
                        WrittenColumnsMap& writtenColumnsMap,
                        int waitSeconds = -1);
    bool AbortSelect();
    void Extents(const std::set<MetaKey>& selectColumns, WrittenColumnsMap& writtenColumnsMap);
    void Tick();

    void DisableGarbageCollection();
    void EnableGarbageCollection();

    // this needs to be defined for template in proc/nabu_server/resource_cache.h
    void GetChildKeys(std::set<std::string>& keys);

protected:
    friend class AccessTable;
    friend class DatabaseImp;

    AccessImp(std::shared_ptr<BranchImp> branch,
              const std::string& tagName,
              const muuid::uuid& handle,
              const cor::TimeRange& timeRange,
              const cor::TimeRange& snappedRange,
              AccessType type);

    std::shared_ptr<BranchImp> mBranchImp;

    MetaDataTable GetRoot();

    // time range snapped to the time grid; the first moment of the first
    // file to the last moment of the final file
    cor::TimeRange mSnapped;

    mutable cor::MMutex mMutex;

    // the IOOperations that are using this access
    typedef std::map<muuid::uuid, IOOperationImp* > IOOperationMap;
    IOOperationMap mIOOperations;

    WrittenColumnsMap mWrittenColumnsMap;
    cor::TimeRange mWrittenTimeRange;

    std::shared_ptr<Tag> mTag;

    typedef std::map<MetaKey, cor::MMutexPtr> ColumnMutexMap;
    ColumnMutexMap mColumnMutexMap;
};

/*class AccessImpPtr : public AccessPtr
{
public:
    std::shared_ptr<IOOperation> CreateIOOperation(const cor::TimeRange& timeRange);
};*/
typedef std::shared_ptr<AccessImp> AccessImpPtr;


/*bool operator >(const AccessImp& a1, const AccessImp& a2);
bool operator <(const AccessImp& a1, const AccessImp& a2);
bool operator ==(const AccessImp& a1, const AccessImp& a2);
bool operator !=(const AccessImp& a1, const AccessImp& a2);*/

std::ostream& operator<<(std::ostream& os, const AccessImp& a);


}

#endif
