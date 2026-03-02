//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_IO_OP__
#define __PKG_NABU_IO_OP__

#include <memory>
#include <vector>

#include <mcor/timerange.h>

#include "access.h"
#include "branch.h"
#include "interface_types.h"
#include "metakey.h"
#include "record.h"


namespace nabu
{

struct SearchResult
{
    cor::TimeRange mTimeRange;
    Json::Value mData;
    std::string Print() const;

    void Parse(const Json::Value& v);
    void Render(Json::Value& v) const;
};

class SearchResultVector : public std::vector<SearchResult>
{
public:
    std::string Print() const;

    void Parse(const Json::Value& v);
    void Render(Json::Value& v) const;
};


/** class IOOperation : an IO operation on the data
 *
 */

class IOOperation
{
public:
    IOOperation(const cor::TimeRange& timeRange,
                IterationDirection direction,
                const muuid::uuid& handle);
    virtual ~IOOperation();

    cor::TimeRange GetTimeRange() const;
    muuid::uuid GetHandle() const;

    // This smart pointer guarantees that the Access upon which this operation
    // is opened lives at least as long as it does. This object is not used
    // except for this purpose. Actions on the access are done on the child
    // class (AccessImp or client::Access) as appropriate.
    void SetAccessLifetime(AccessPtr accessLifetimePtr)
    {
        mAccessLifetime = accessLifetimePtr;
    }

    // ---- Read operations

    // Read up to count records into records up to and including 'until'
    // If count is zero, then all valid records up to and including 'until'
    // will be returned.
    // For Region columns, count and until are ignored, and all regions in
    // the IO time range are returned.
    virtual void Read(const MetaKey& column,
              RecordVector& records,
              size_t count = 0,
              cor::Time until = cor::Time::PositiveInf()) = 0;
    bool Read(const MetaKey& column,
                      Record& record,
                      cor::Time until = cor::Time::PositiveInf());

    // read all records from column that intersect any time range in 'which'
    void Read(const MetaKey& column, const SearchResultVector& which, RecordVector& records);

    // parallel reads
    static void Read(nabu::AccessPtr access,
                     IterationDirection direction,
                     const std::vector<MetaKey>& columns,
                     std::vector<RecordVector>& records,
                     const cor::TimeRange& tr);
    // read up to count records in parallel
    static void Read(nabu::AccessPtr access,
                     IterationDirection direction,
                     RecordMap& records,
                     const cor::TimeRange& tr);
    static void Read(nabu::AccessPtr access,
                     IterationDirection direction,
                     const WrittenColumnsMap& wcm,
                     RecordMap& records);


    // skip over records, to resume at whence, the next record that will be read
    // returns true if there is data available, false if at end
    virtual bool ReadSeek(const MetaKey& column,
                          const cor::Time& whence) = 0;

    //virtual void Search(const MetaKey& column, const std::string& predicate, SearchResultVector& results) = 0;

    // ---- Write operations

    // data write
    virtual void Write(const MetaKey& column, const RecordVector& records) = 0;
    void Write(const MetaKey& column, const Record& record);

    virtual void Commit() = 0;
    virtual void Abort() = 0;

    // parallel operations
    // write records
    void Write(const std::vector<MetaKey>& columns, const std::vector<RecordVector>& records);
    // creates an IOOperationPtr per record extent, so to not erase any data in any column
    static void Write(nabu::AccessPtr access,
                      IterationDirection direction,
                      const std::vector<MetaKey>& columns,
                      const std::vector<RecordVector>& records);
    void Write(const std::map<MetaKey, RecordVector>& recordMap);
    // parallel write and commit of records, but with timerange of each entry
    // in records described in rangeMap
    static void Write(nabu::AccessPtr access,
                      IterationDirection direction,
                      const RecordMap& records,
                      const RangeMap& rangeMap);

    // creates an IO Operation per record, as needed for Region columns
    void WriteRegions(const std::map<MetaKey, RecordVector>& recordMap);

    // this needs to be defined for template in proc/nabu_server/resource_cache.h
    void GetChildKeys(std::set<std::string>& keys) {}

    // return the map of all columns, and the time range within, written
    // returns the time extents of the map
    //virtual void GetWrittenColumns(WrittenColumnsMap& writtenColumnsMap, cor::TimeRange& extents) = 0;

    // ---- tag operations
    // Creates a new tag or moves a tag depending on policy.
    // This is only valid for operations created on a writable Access. If the
    // policy is to move a tag, then the Access must have been created on the
    // source tag.
    //virtual void CreateTag(const std::string& newTagName, TagPolicy tagPolicy) = 0;
    // Marks tag for deletion, after a Commit, and upon destruction,
    // deferred to make Read operation safe until the object is gone.
    // This is only possible for IOOperations whose Access was created on a
    // tag. That tag is the target of DeleteTag.
    //virtual void DeleteTag() = 0;
    // Duplicates a tag.
    // This is only possible for IOOperations whose Access was created on a
    // tag. That tag is the source for DuplicateTag.
    //virtual void DuplicateTag(const std::string& newTagName) = 0;

protected:
    const cor::TimeRange mTimeRange;
    IterationDirection mDirection;
    muuid::uuid mHandle;
    AccessPtr mAccessLifetime;

    // semantic sugar
    bool IsForward() const { return mDirection == eIterateForwards; }
    bool IsBackward() const { return !IsForward(); }

    IterationDirection OppositeIteration() const
    { return mDirection == eIterateForwards ? eIterateBackwards : eIterateForwards; }

};

typedef std::shared_ptr<IOOperation> IOOperationPtr;

}

#endif
