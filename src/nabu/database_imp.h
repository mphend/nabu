//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_DATABASE_IMP_H
#define GICM_NABU_DATABASE_IMP_H
#include "branch_imp.h"
#include <mcor/muuid.h>

#include "async_status.h"

#include "database.h"
#include "garbage.h"
#include "io_op_imp.h"
#include "journal.h"
#include "journal_thread.h"
#include "label_file.h"
#include "time_path.h"
#include "time_scheme.h"

namespace nabu
{

/** class DatabaseImp
 *
 */
class DatabaseImp : public Database
{
public:
    DatabaseImp(const std::string& rootDirectory);
    virtual ~DatabaseImp();

    // ---- Database implementation
    const std::string& GetInstanceName() const
    {
        throw cor::Exception("Database::GetInstanceName() -- called on concrete database object");
    }

    std::string GetDescriptiveLiteral() const
    {
        return mUrl.GetLiteral();
    }

    BranchPtr GetBranch(const MetaKey& branchPath);
    BranchPtr GetMain();
    BranchPtr CreateBranch(const MetaKey& branchPath);
    void DeleteBranch(const MetaKey& branchPath);

    BranchImpPtr GetBranchImp(const MetaKey& branchPath) const;
    BranchImpPtr GetMainImp();

    // create a brand-new database at the given directory;
    // throws if one already exists
    void CreateNew(const nabu::UtcTimeScheme& timeScheme);
    // tries to create a brand-new database at the given directory;
    // returns false if one already exists
    bool TryCreateNew(const nabu::UtcTimeScheme& timeScheme);

    // make a brand-new database identical to remote
    CopyStats Clone(const DatabasePtr remote);

    // initialize the database; must be called before operations can begin
    void Initialize();

    // set the metadate cache limit, in number of entries
    void SetCacheLimit(int entries);

    // returns whether there is a database at root
    bool Exists();

    // Create an access of the given type on the branch; a tag can be provided
    // to allow creation of IO Operations for a particular tag.
    // Although data for tags can't be written, the tag itself can be moved or
    // deleted, which is a write operation and follows read/write access rules.
    AccessPtr CreateAccess(const MetaKey& branchPath,
                                 const cor::TimeRange& timeRange,
                                 AccessType type,
                                 const std::string tagName = "");

    // returns whether the given node (data or metadata) is in use by any
    // branch or tag
    bool VersionInUse(const TableAddress& nodeAddress);

    void GetUnusedVersions(const TableAddress& ta, MetaDataMap diffs);

    virtual void WriteFile(const std::vector<unsigned char>& data,
                           const TableAddress& tableAddress,
                           FileType fileType);
    virtual void ReadFile(std::vector<unsigned char>& data,
                          const TableAddress& tableAddress,
                          FileType fileType) const;

    virtual bool GetMetadata(MetaDataMap metaDataMap, const TableAddress& ta) const;
    virtual void PersistMetadata(MetaDataMap metaDataMap, const TableAddress& ta) const;

    // NOTE: only one asynchronous action can be performed at a time

    // Start asynchronous cleaning of orphans
    void CleanOrphans();
    // Clean orphans synchronously, printing progress to stream
    AsyncStatus CleanOrphans(std::ostream& os);

    // Start asynchronous count of references to files that do not exist
    void CountMissing();
    // Count missing files synchronously, printing progress to stream
    AsyncStatus CountMissing(std::ostream& os);
    // **** private, but public due to implementation details; do not use this.
    void DoCountMissing();

    // Start asynchronous update of file names to current version
    void UpdateFiles();

    // returns percentage of total asynchrounous activity and updates stats
    double PercentDoneAsynchronous(AsyncStatus& stats);

    TimePath GetPath(const cor::Time& in);
    cor::TimeRange GetTime(const TimePath& in);
    // get all paths contained in the time range
    void GetPaths(std::vector<TimePath>& paths, const cor::TimeRange& timeRange) const
    {
        return mTimeScheme.GetPaths(paths, timeRange);
    }

    const UtcTimeScheme& GetTimeScheme() const { return mTimeScheme; }

    // send all data in journals to indexed storage; will block
    // until access is available to perform this action based on
    // data contents of all journals. This is not threadsafe, and
    // should only be called by the JournalThread.
    void FlushJournals();

    JournalThread& GetJournalThread() { return mJournalThread; }
    const JournalThread& GetJournalThread() const { return mJournalThread; }

    Garbage& GetGarbage() { return mGarbageCollector; }
    const Garbage& GetGarbage() const { return mGarbageCollector; }

    // print the tree of branches and labels
    void PrintLabels(std::ostream& os);

    // TESTING USE ONLY
    bool TestCompareLabels(DatabaseImp& other);

    // delete a root of the given version (probably a tag) though top-down
    // iteration; returns number of files deleted
    size_t DeleteRoot(const TableAddress& root);

    const FileNamer& GetFileNamer() const { return mFileNamer; }

    // make this look like remote; returns number of files transferred
    CopyStats Pull(const DatabasePtr remote);

    // make remote look like this; returns number of files transferred
    CopyStats Push(const DatabasePtr remote);

    // write all tags to the label file
    void PersistTags();

    // set the column description from the given JSON
    //void ParseColumns(const Json::Value& v);

    //ColumnInfo GetColumnInfo(const MetaKey& column) const;
    //void AddColumn(const MetaKey& column, const ColumnInfo& columnInfo);
    //void DeleteColumn(const MetaKey& column);
    //std::string PrintColumns() const;

    std::vector<unsigned int> GetPeriods() const
    {
        return GetTimeScheme().GetPeriods();
    }
    muuid::uuid GetFingerprint() const { return mFingerprint; }

    TagImpPtr GetTagImp(TagPtr tag);

protected:
    muuid::uuid mFingerprint;
    nabu::UtcTimeScheme mTimeScheme;

    FileNamer mFileNamer;

    JournalMap mJournals;

    Garbage mGarbageCollector;
    JournalThread mJournalThread;

    BranchImpPtr mMainBranch;

    //mutable cor::MMutex mColumnInfoMutex;
    //ColumnInfoMap mColumnInfo;
    //ColumnInfoFile mColumnInfoFile;

    // persistence of all branches and tags
    // XXX
    // This mutex is unnecessary when there is only one branch, since the
    // mLabelMutex on the branch is held anyway during label changes, and
    // having two mutexes introduces deadlock.
    // When multiple branches are supported, need to revisit this:
    //    One tag label file per branch, and one branch file per database?
    //    Put mutex back and be careful about lock ordering?
    //mutable cor::MMutex mLabelFileMutex;
    LabelFile mLabelFile;

    // parse the label description file(s), find the initial version of all branches,
    // and flush the journals for each branch
    void InitializeBranches();

    // parse the column information file(s)
    //void InitializeColumns();

    AsyncStatus mAsyncData;

private:
    DatabaseImp(const DatabaseImp&);
    void operator=(const DatabaseImp&);
};


typedef std::shared_ptr<DatabaseImp> DatabaseImpPtr;

} // end namespace

#endif
