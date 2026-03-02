//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef PKG_NABU_BRANCH_IMP_INCLUDED
#define PKG_NABU_BRANCH_IMP_INCLUDED

#include <map>
#include <memory>

#include <mcor/locking_accessor.h>
#include <mcor/mmutex.h>

#include "access_table.h"
#include "async_status.h"
#include "branch.h"
#include "interface_types.h"
#include "journal.h"
#include "metadata_table.h"
#include "tag_imp.h"
#include "version.h"

namespace nabu
{

class IOOperationImp;
class Database;
class DatabaseImp;


/** class BranchImp
 *
 */

class BranchImp : public Branch
{
public:
    BranchImp(DatabaseImp& database, const MetaKey& branchPath);
    virtual ~BranchImp();

    // ---- Branch implementation

    std::shared_ptr<IOOperationImp> CreateNestedIOOperation(const cor::TimeRange& timeRange,
                                            IterationDirection direction,
                                            AccessImp* access);

    size_t Append(const MetaKey& column, const RecordVector& newData);

    // return the extents of readable data for given column, including
    // that currently in a Journal. If there is no data, TimeRange will
    // be Invalid
    cor::TimeRange Extents(const MetaKey& column, ExtentsType extentsType);

    void GetTags(std::set<std::string>& tags);


    TagPtr TagHead(const std::string& tagName, TagPolicy policy)
    {
        Version v;
        return CreateTag(tagName, policy, v);
    }

    // move the head version
    void MoveHead(const Version& newHead);

    TagPtr CreateTag(const std::string& tagName, TagPolicy policy, const Version& version);

    TagPtr GetTag(const std::string& tagName);

    // returns true if the node is referenced by any tag or by head
    bool VersionInUse(const TableAddress& nodeAddress);

    // filter out the versions in 'diffs' that are in use in this database,
    // returning only those that are missing.
    void GetUnusedVersions(const TableAddress& ta, MetaDataMap diffs);

    // delete the tag of the given name; returns whether tag was found.
    // This puts the root of the tag in the garbage, which will result
    // in all files unique to the tag being deleted.
    bool DeleteTag(const std::string& tagName);
    bool DeleteTag(TagPtr tag) { return DeleteTag(tag->GetLiteral()); }

    // ---- concrete methods

    // remove all labels (recursively) found in this that are not in source
    void TrimLabels(Database& source);

    void TrimTags(const std::set<std::string>& tagSet);

    // print the tree of branches and tags, with tabSpaces indented
    void PrintLabels(std::ostream& os, int tabSpaces);
    // TESTING USE ONLU
    bool TestCompareLabels(std::shared_ptr<BranchImp> other) const;

    // commits pending to current
    void Commit(const MetaKey& column,
                const std::map<TimePath, Garbage::Entry>& writtenLeaves,
                const cor::TimeRange& extent);

    // remove the given column from the database
    void DeleteColumn(const MetaKey& column);

    // return the columns
    void GetColumns(const MetaKey& column, std::vector<MetaKey>& columns);
    bool HasColumn(const MetaKey& column);

    // returns the journals
    void GetJournals(JournalMap& journals);
    // return a specific journal; will be NULL if not found
    Journal GetJournal(const MetaKey& column);

    AccessTable&  GetAccessTable(const std::string& tagName);

    // send all data in journals to indexed storage; will block
    // until access is available to perform this action based on
    // data contents of all journals. This is not threadsafe, and
    // should only be called by the JournalThread.
    void FlushJournals();

    // load journals
    void LoadJournals();

    // descend tree and return branch, or NULL if not found
    std::shared_ptr<BranchImp> GetBranch(const MetaKey& branchPath);

    // make this branch identical to the remote; returns the number of files
    // moved from there to here in the process
    /*CopyStats Pull(BranchPtr remote)
    {
        return Sync(remote, *this, ePull);
    }*/

    // make remote branch identical to this; returns the number of files
    // moved from there to here in the process
    /*CopyStats Push(BranchPtr remote)
    {
        return Sync(*this, remote, ePush);
    }*/

    // Persistence of branch information -- not metadata
    void Render(Json::Value& into);
    void Parse(const Json::Value& from);

    // find all persistence files for this branch and determine the
    // latest version; flush any journals
    void Initialize(const Version& headVersion);

    // fetch the current head of this branch
    MetaDataTable GetHead();
    // fetch the named tag; if tag does not exit, table will be false
    MetaDataTable GetTagMetaData(const std::string& tagName);

    // Returns the database implementation
    virtual DatabaseImp& GetDatabaseImp()
    {
        return mDatabase;
    }
    // Returns the database implementation
    virtual const DatabaseImp& GetDatabaseImp() const
    {
        return mDatabase;
    }

    // iterates head, tags, and child branches to return the total node
    // count
    size_t TotalSize();

    // iterates head, tags, and child branches to determine if all files
    // exist, updating stats
    void CountMissing(AsyncStatus& stats);

    void ValidateAccess() { mAccessTable.Validate(); }

    TagImpPtr GetTagImp(TagPtr tag);

private:
    DatabaseImp& mDatabase;

    // Held as Write when the metadata is being changed (Commit), or when the
    // data of this class is being changed (like when adding a tag). This is
    // a Read/Write lock instead of Mutex primarily for deadlock avoidance
    // rather than performance. The deadlock threat occurs when multiple
    // databases are involved in concurrent operations, and while lock order is
    // reversed, only one database (the one being written to) needs the
    // locking.
    //mutable cor::RWLock mRWLock;
    mutable cor::MMutex mLabelMutex;

    typedef std::map<std::string, std::shared_ptr<BranchImp>> BranchTable;
    typedef std::map<std::string, TagImpPtr> TagImpTable;

    BranchTable mBranchTable;
    TagImpTable mTagImpTable;

    mutable cor::MMutex mHeadMutex;
    Version mHeadVersion;

    AccessTable mAccessTable;

    mutable cor::MMutex mJournalMutex;
    JournalMap mJournals;

public:
    enum SyncDirection { ePush, ePull };
    static CopyStats Sync(BranchPtr source, BranchPtr destination, SyncDirection direction);
};


typedef std::shared_ptr<BranchImp> BranchImpPtr;

}

#endif
