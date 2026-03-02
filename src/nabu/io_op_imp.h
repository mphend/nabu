//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_IO_OP_IMP__
#define __PKG_NABU_IO_OP_IMP__

#include <memory>
#include <vector>

#include <mcor/mfile.h>
#include <mcor/mmutex.h>
#include <mcor/timerange.h>

#include "access_imp.h"
#include "access_table.h"
#include "branch_imp.h"
#include "data_file.h"
#include "io_op.h"
#include "metakey.h"
#include "metadata_table.h"

#include <mcor/muuid.h>

namespace nabu
{


class BranchImp;

/** class IOOperationImp : an IO operation on the data
 *
 */

class IOOperationImp : public nabu::IOOperation
{
public:
    enum Nesting { eNormal, eNested};

    IOOperationImp(DatabaseImp& database,
                   const cor::TimeRange& timeRange,
                   IterationDirection direction,
                   const muuid::uuid& handle,
                   AccessImp* accessImp,
                   Nesting nested = eNormal);

    virtual ~IOOperationImp();

    BranchImpPtr GetBranchImp() const { return mAccessImp->GetBranchImp(); }

    const std::string GetTagName() const { return mAccessImp->GetTagName(); }

    // ---- Read operations

    // returns whether the given column exists on the Branch
    bool HasColumn(const MetaKey& column);

    // Read up to count records into records up to and including 'until'
    // If count is zero, then all valid records up to and including 'until'
    // will be returned.
    // For Region columns, count and until are ignored, and all regions in
    // the IO time range are returned.
    void Read(const MetaKey& column,
              RecordVector& records,
              size_t count = 0,
              cor::Time until = cor::Time::PositiveInf());

    // skip over records, to resume at whence, the next record that will be read
    // returns true if there is data available, false if at end
    bool ReadSeek(const MetaKey& column,
                  const cor::Time& whence);

    //void Search(const MetaKey& column, const std::string& predicate, SearchResultVector& results);

    // ---- Write operations

    // data write
    void Write(const MetaKey& column, const RecordVector& records);

    void Commit();
    void Abort();

    // return the map of all columns, and the time range within, written
    // returns the time extents of the map
    void GetWrittenColumns(WrittenColumnsMap& writtenColumnsMap, cor::TimeRange& extents);

    // if necessary, write the edges of the column if they have not been
    // already written with new data.
    // This handles the case where part of the edge file should be deleted,
    // but not the part which is outside the IO time range.
    void FlushEdges(const MetaKey& column);

    // this needs to be defined for template in proc/nabu_server/resource_cache.h
    void GetChildKeys(std::set<std::string>& keys) {}

    std::string Print() const;

protected:
    DatabaseImp& mDataBase;

    AccessImp* mAccessImp;
    bool mAddedToAccess;
    Nesting mNesting;

    // The time-based access table
    AccessTable* mAccessTable;

    // data for read state of each column
    struct ReadImp
    {
        ReadImp();
        ~ReadImp();

        mutable cor::MMutex mMutex;

        TimePath mReadTimePath; // for Read iteration
        bool mReadPathValid;

        RecordVector mReadBuffer;
        cor::Time mLastTimeRead;
        size_t mReadIndex;

        size_t DataRemaining() const { return mReadBuffer.size() - mReadIndex; }
        void Read(Record& r)
        {
            r = mReadBuffer[mReadIndex++];
        }

        Record* mLastRegion;
    };

    // this must be held to access mReadImpMap
    mutable cor::MMutex mReadImpMutex;
    typedef std::map<MetaKey, ReadImp> ReadImpMap;
    ReadImpMap mReadImpMap;

    // data for write state of each column
    struct WriteImp
    {
        WriteImp();
        ~WriteImp();

        mutable cor::MMutex mMutex;

        cor::Time mLastTime;

        // the files that were altered
        std::map<TimePath, Garbage::Entry> mNewFiles;
        // the files that were affected
        std::map<TimePath, Garbage::Entry> mOldFiles;

        // the timerange of all records written
        cor::TimeRange mWrittenRange;

        RecordVector mWriteBuffer;
        // the data written by this operation that is in a shared file at the beginning
        RecordVector mLeadingEdge;
        // the data written by this operation that is in a shared file at the end
        RecordVector mTrailingEdge;

        // this keeps track of the end of the last region written; it is the
        // 'wet edge' of region rendering, and must be written only when the
        // next region is started. If it is eInvalid, there is no open region
        cor::Time mLastRegionEnd;

        // update write extents for given incoming record
        void UpdateWriteExtents(const Record& r);
    };

    // this must be held to access mWriteImpMap and before locking a node
    // of its contents; it must be Unlocked immediately
    // after acquiring the node lock
    mutable cor::MMutex mWriteImpMutex;
    typedef std::map<MetaKey, WriteImp> WriteImpMap;
    WriteImpMap mWriteImpMap;

    // this is used for Select, to keep track of what has been written;
    // it is not used for data or metadata version tracking
    WrittenColumnsMap mSelectMap;
    cor::TimeRange mSelectTimeRange;

    // ---- Read implementation functions

    void ReadImpl(const MetaKey& column,
              RecordVector& records,
              size_t count,
              cor::Time until);

    // Reads next file from disk into mReadBuffer
    // returns true if successful, false if end of data was reached
    bool FetchFromFile(const MetaKey& column, ReadImp* imp);

    // Reads all records in file described by column and timepath
    void FetchAllInFile(const nabu::MetaKey& column,
                   const TimePath& timePath,
                   RecordVector& records);

    // Reads the final previous record before or at a given time in a column,
    // respective of the direction of iteration for the IOOperation;
    // returns true if any such record exists, false otherwise
    enum Inclusivity { eInclusive, eExclusive };
    bool FetchLastRecord(const nabu::MetaKey& column,
                        const cor::Time& mark,
                        nabu::Record& record,
                        Inclusivity inclusivity);

    // Read from journal into mReadBuffer
    // returns true if successful, false if no records were found
    bool FetchFromJournal(const MetaKey& column, ReadImp* imp);

    // ---- Write implementation functions

    void WriteImpl(const MetaKey& column, const RecordVector& records);

    void CommitRegion(const MetaKey& column);

    void CommitColumn(const MetaKey& column);

    // Writes all records described by column and timepath, and updates
    // newFiles/oldFiles for persistence
    void WriteAllToFile(WriteImp* imp,
                        const nabu::MetaKey& column,
                        const TimePath& timePath,
                        RecordVector& records);

    // Write everything in mWriteBuffer to disk
    void FlushToFile(const MetaKey& mAddress, WriteImp* imp, const cor::Time& timeOfData);

    // put all records up to but excluding 'first' that exist in file into 'records'
    // if 'start' is Valid, records earlier than it from the file are ignored
    void FillBefore(const::std::string& fileName,
                    //const ColumnInfo& columnInfo,
                    RecordVector& records,
                    const cor::Time& first,
                    const cor::Time& start);
    // put all records after and including 'final' that exist in file into 'records'
    // if 'stop'is Valid, records later than it from the file are ignored
    void FillAfter(const::std::string& fileName,
                   //const ColumnInfo& columnInfo,
                   RecordVector& records,
                   const cor::Time& final,
                   const cor::Time& stop);

    // convert the regions in 'records' into PIT representation
    // Returns the final time of the last record, which is not
    // encoded into records (because it cannot yet be closed); if
    // records is empty, this time is eInvalid
    cor::Time ConvertRegionsToDeltas(RecordVector& records, const cor::Time& lastRegionEnd);

    // convert records, which should be start/end demarcations, into regions. The
    // preceding open (started) region should be provided as lastRegion, and if
    // an un-matched 'start' remains at the end of records it will be returned, or else
    // NULL
    Record* ConvertDeltasToRegions(RecordVector& records, Record* lastRegion);

    friend class DatabaseImp;

    // return either branch head or, if a mRootTag is valid, the root table for the tag
    MetaDataTable GetRoot();
};

typedef std::shared_ptr<IOOperationImp> IOOperationImpPtr;

}

#endif
