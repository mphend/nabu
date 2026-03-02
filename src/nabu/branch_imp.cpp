//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include <mcor/mthread.h>
#include <mcor/profiler.h>
#include <mcor/thread_analysis.h>

#include "branch_imp.h"
#include "cache.h"
#include "context.h"
#include "database_imp.h"
#include "io_op_imp.h"
#include "journal.h"
#include "leak_detector.h"
#include "trace.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {


BranchImp::BranchImp(DatabaseImp& database, const MetaKey& branchPath) :
    Branch(database, branchPath),
    mDatabase(database),
    //mRWLock("Branch"),
    mLabelMutex("BranchLabelMutex"),
    mHeadMutex("BranchHeadMutex"),
    mAccessTable(branchPath.GetLiteral()),
    mJournalMutex("BranchJournalMutex")
{
    DEBUG_LOCAL("BranchImp constructor '%s'\n", branchPath.GetLiteral().c_str());
    LeakDetector::Get().Increment("Branch");
}

BranchImp::~BranchImp()
{
    DEBUG_LOCAL("BranchImp destructor '%s'\n", mBranchPath.GetLiteral().c_str());
    LeakDetector::Get().Decrement("Branch");
}

IOOperationImpPtr
BranchImp::CreateNestedIOOperation(const cor::TimeRange& timeRange,
                                     IterationDirection direction,
                                     AccessImp* access)
{
    DEBUG_LOCAL("BranchImp::CreateNestedIOOperation %s\n", timeRange.Print().c_str());

    IOOperationImpPtr iop(new IOOperationImp(mDatabase,
                                           timeRange,
                                           direction,
                                           muuid::uuid::random(),
                                           access,
                                           IOOperationImp::eNested));


    return iop;
}

size_t
BranchImp::Append(const MetaKey& column, const RecordVector& records)
{
    if (records.empty())
        return 0;

    cor::Time ft = records.front().GetTime();
    cor::Time lt = records.back().GetTime();
    Context ctx("Branch::Append column %s, %s to %s  (%s to %s)",
                column.GetLiteral().c_str(),
                ft.Print().c_str(),
                lt.Print().c_str(),
                mDatabase.GetTimeScheme().GetPath(ft).GetLiteral().c_str(),
                mDatabase.GetTimeScheme().GetPath(lt).GetLiteral().c_str()
                );
    cor::ObjectLocker ol(mJournalMutex, "DB " + mDatabase.GetLiteral() + " Append");

    if (mJournals.find(column) == mJournals.end())
    {
        mJournals[column] = Journal(new JournalImp(this, column));
    }
    Journal& journal = mJournals[column];

    size_t i;
    for (i = 0; i < records.size(); i++)
    {
        Record r = records[i];
        const cor::Time& lastTime = journal->GetLastTime();
        if (lastTime.Valid() && (r.GetTime() <= lastTime))
        {
            printf("Warning: Ignoring non-increasing time value in Append to %s (current = %s, last = %s)\n",
                   column.GetLiteral().c_str(),
                   r.GetTime().Print().c_str(),
                   lastTime.Print().c_str());
            continue;
        }
        else
            break; // first valid record means rest are valid (assuming ascending time order!)
    }
    if (i == records.size())
        return 0; // nothing to do; all records were stale

    RecordVector v(records.size() - i);
    size_t j = 0;
    for (; i != records.size(); i++, j++)
        v[j] = records[i];

    journal->Append(v);

    return v.size();
}

cor::TimeRange
BranchImp::Extents(const MetaKey& column, ExtentsType extentsType)
{
    try {

        DEBUG_LOCAL("BranchImp::Extents %s\n",
                    column.GetLiteral().c_str());

        cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " Extents");
        //cor::ObjectLocker rl(mLabelMutex, "DB " + mDatabase.GetLiteral() + " Extents");
        cor::TimeRange tr = GetHead()->RootWriteExtents(mDatabase, column);

        // if looking for readable extents, include any records in the journal
        if (extentsType == eReadable)
        {
            Journal journal = GetJournal(column);
            if (journal)
            {
                tr = tr.SuperUnion(journal->Extents());
            }
        }

        return tr;

    } catch (cor::Exception& err)
    {
        err.SetWhat("BranchImp::Extents() -- %s", err.what());
        throw err;
    }
}

void
BranchImp::GetTags(std::set<std::string>& tags)
{
    cor::ObjectLocker rl(mLabelMutex, "DB " + mDatabase.GetLiteral() + " GetTags");

    TagImpTable::const_iterator i = mTagImpTable.begin();
    for (; i != mTagImpTable.end(); i++)
    {
        if (!i->second->IsTemporary())
            tags.insert(i->first);
    }
}

TagPtr
BranchImp::CreateTag(const std::string& tagName, TagPolicy tagPolicy, const Version& version)
{
    DEBUG_LOCAL("BranchImp::CreateTag database %s, branch %s adding %s tag %s\n",
                mDatabase.GetFileNamer().GetRoot().c_str(),
                GetLiteral().c_str(),
                tagPolicy == eTemporary ? "temporary" : "permanent",
                tagName.c_str());

    // make sure user is not asking for an illegal tag name (like "main" or ".mike")
    {
        std::string nameViolation;
        if (!FileNamer::ValidateLabelName(tagName, nameViolation))
        {
            throw cor::Exception("CreateTag -- tag name '%s' is invalid: %s", tagName.c_str(), nameViolation.c_str());
        }
    }

    // XXX holding this mutex while manipulating label file will deadlock if the
    // mLabelFileMutex in DatabaseImp is implemented. See notes in database_imp.h.
    // Need to hold this lock to not allow delete of a tag until it is fully created.
    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " CreateTag");

    // check for existing tag
    Version discarded;
    {
        TagImpTable::const_iterator i = mTagImpTable.find(tagName);
        if (i != mTagImpTable.end())
        {
            if (tagPolicy != eMove)
                throw cor::Exception("BranchImp:::CreateTag -- tag '%s' exists\n", tagName.c_str());
            discarded = i->second->GetVersion();
        }
    }

    Version tagVersion = version;
    if (tagVersion.Invalid())
        tagVersion = GetHead()->GetTableAddress().GetVersion();

    // create new tag
    TagImpPtr r;
    {


        if (!tagVersion.Valid())
        {
            //MetaDataTable head = GetHead();
            TableAddress ta(MetaKey(), TimePath(), mHeadVersion);
            MetaDataTable head = Cache::Get().Find(mDatabase, ta);

            tagVersion = head->GetTableAddress().GetVersion();
        }

        // DEFENSIVE; do not allow tag of empty branch
        if (tagVersion.Invalid())
        {
            printf("Branch::CreateTag (%s) -- branch is empty\n", tagName.c_str());
            return TagPtr();
        }

        if (discarded.Valid()) // i.e., moving an existing tag
        {
            // SANITY
            if (!mTagImpTable[tagName])
                printf("Moving an empty tag '%s' !!!\n", tagName.c_str());
            r = mTagImpTable[tagName];

            r->MoveVersion(tagVersion);
        }
        else // a new tag
        {
            r = std::shared_ptr<TagImp>(new TagImp(*this,
                                                   tagName,
                                                   tagVersion,
                                                   GetHead(),
                                                   tagPolicy == eTemporary));
            mTagImpTable[tagName] = r;
        }
    }

    // put contents of old tag, if present, into garbage
    // (Unless the tags are identical)
    if (discarded.Valid() && (discarded != version))
    {
        TableAddress ta(GetBranchPath(), TimePath(), discarded);
        Garbage::Entry entry(ta, FileType::eRoot);
        mDatabase.GetGarbage().Insert(entry);
    }

    // persist label file
    mDatabase.PersistTags();

    DEBUG_LOCAL("BranchImp::CreateTag tag %s added at version %s\n",
                tagName.c_str(),
                tagVersion.GetLiteral().c_str());

    return r;
}

TagPtr
BranchImp::GetTag(const std::string& tagName)
{
    //cor::ReadLocker wl(mRWLock,"DB " + mDatabase.GetLiteral() + " GetTag");
    cor::ObjectLocker ol(mLabelMutex, "DB " + mDatabase.GetLiteral() + " GetTag");
    TagImpTable::iterator i = mTagImpTable.find(tagName);
    if (i == mTagImpTable.end())
        return TagPtr();

    return i->second;
}

void
BranchImp::MoveHead(const Version& newHead)
{
    DEBUG_LOCAL("BranchImp::MoveHead branch %s moving head to %s\n",
                GetLiteral().c_str(),
                newHead.GetLiteral().c_str());

    cor::ObjectLocker ol(mHeadMutex, "DB " + mDatabase.GetLiteral() + " MoveHead");

    Version discarded = mHeadVersion;
    mHeadVersion = newHead;

    // put contents of old head, if present, into garbage
    // (unless the head didn't move!)
    if (discarded.Valid() && (discarded != newHead))
    {
        TableAddress ta(MetaKey(), TimePath(), discarded);
        Garbage::Entry entry(ta, FileType::eRoot);
        mDatabase.GetGarbage().Insert(entry);
    }
}

bool
BranchImp::DeleteTag(const std::string& tagName)
{
    {
        cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " DeleteTag");

        // check for existing tag
        // (this can't be const_iterator for pre C++11)
        TagImpTable::iterator i = mTagImpTable.find(tagName);
        //Version discarded;
        if (i == mTagImpTable.end())
        {
            DEBUG_LOCAL("BranchImp::DeleteTag branch %s -- tag %s not found\n",
                        GetLiteral().c_str(),
                        tagName.c_str());
            return false;
        }
        //discarded = i->second->GetVersion();

        // must remove from tag table now, or else it will appear to be in use
        // if garbage gets to it quickly
        //printf("0x%x DeleteTag -- erasing tag %s, this = 0x%x\n", cor::Thread::Self(), tagName.c_str(), (unsigned long)this);
        mTagImpTable.erase(i);

        // This is done in TagImp destructor
        //TableAddress ta(MetaKey(), TimePath(), discarded);
        //mDatabase.GetGarbage().Insert(Garbage::Entry(ta, FileType::eRoot));
    }

    // persist label file
    mDatabase.PersistTags();

    //printf("BranchImp::DeleteTag tag %s deleted\n", tagName.c_str());
    return true;
}

void
BranchImp::TrimTags(const std::set<std::string>& tags)
{
    DEBUG_LOCAL("BranchImp::TrimTags database %s\n", mDatabase.GetLiteral().c_str());

    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " TrimTags");
    // XXX iterate sub-branches

    // iterate tags
    TagImpTable::iterator i = mTagImpTable.begin();

    while (i != mTagImpTable.end())
    {
        // if non-temporary tag does not exist in other branch
        if (!i->second->IsTemporary() && (tags.find(i->first) == tags.end()))
            //if (tags.find(i->first) == tags.end())
        {
            // must remove from tag table now, or else it will appear to be in use
            // if garbage gets to it quickly
            Version discarded = i->second->GetVersion();
            // must advance iterator prior to erase with post-increment; pre C++11,
            // no iterator is returned by 'erase'
            TagImpTable::iterator ic = i++;
            mTagImpTable.erase(ic);

            TableAddress ta(MetaKey(), TimePath(), discarded);
            mDatabase.GetGarbage().Insert(Garbage::Entry(ta, FileType::eRoot));
        }
        else
        {
            i++;
        }
    }

}

void
BranchImp::PrintLabels(std::ostream& os, int tabSpaces)
{
    std::string sindent(tabSpaces, ' ');

    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " PrintLabels");

    if (!mTagImpTable.empty())
        os << sindent << "tags:" << std::endl;
    {
        std::string sindent(tabSpaces + 4, ' ');
        TagImpTable::const_iterator i = mTagImpTable.begin();
        for (; i != mTagImpTable.end(); i++)
        {
            os << sindent << i->first << " = " << i->second->GetVersion() << std::endl;
        }
    }

    if (!mBranchTable.empty())
        os << sindent << "branches:" << std::endl;
    {
        std::string sindent(tabSpaces + 4, ' ');
        BranchTable::const_iterator i = mBranchTable.begin();
        for (; i != mBranchTable.end(); i++)
        {
            std::cout << sindent << i->first << std::endl;
            i->second->PrintLabels(os, tabSpaces + 4);
        }
    }

}

bool
BranchImp::TestCompareLabels(BranchImpPtr other) const
{
    //or::WriteLocker ol(mRWLock);
    //cor::WriteLocker ol2(other->mRWLock);
    // don't compare object, which will compare smart pointers
    if (mTagImpTable.size() != other->mTagImpTable.size())
        return false;
    TagImpTable::const_iterator i = mTagImpTable.begin();
    TagImpTable::const_iterator j = other->mTagImpTable.begin();
    for (; i != mTagImpTable.end(); i++, j++)
    {
        if (i->first != j->first)
            return false;
        if (i->second->GetVersion() != j->second->GetVersion())
            return false;
    }
    return true;
}

void
BranchImp::Commit(const MetaKey& column,
            const std::map<TimePath, Garbage::Entry>& writtenLeaves,
            const cor::TimeRange& extent)
{
    DEBUG_LOCAL("BranchImp::Commit %s %s\n", column.GetLiteral().c_str(), extent.Print().c_str());
    cor::ObjectLocker ol(mHeadMutex, "DB " + mDatabase.GetLiteral() + " Commit");

    Context ctx("BranchImp::Commit column %s %s",
                column.GetLiteral().c_str(),
                extent.Print().c_str());

    // avoid using GetHead here due to recursion being prohibited by RWLock
    TableAddress ta(MetaKey(), TimePath(), mHeadVersion);
    MetaDataTable head = Cache::Get().Find(mDatabase, ta);

    MetaDataTable newHead(head->Replicate());
    // RootCommit will advance the version of newHead
    newHead->RootCommit(mDatabase, column, writtenLeaves, extent);

    // create or update symlink
    std::string symlinkName(mDatabase.GetFileNamer().GetRoot() + "/branches/" + mBranchPath.GetLiteral() + ".head");
    //printf("symlinkName: %s\n", symlinkName.c_str());
    std::string tempSymlinkName(mDatabase.GetFileNamer().GetRoot() + "/branches/" + newHead->GetTableAddress().GetVersion().GetLiteral() + ".head");
    //printf("tempSymlinkName: %s\n", tempSymlinkName.c_str());
    //std::string newRootPath = mDatabase.GetFileNamer().NameOf(newHead->GetTableAddress(), eMetaData);
    std::string newRootPath = "../root." + newHead->GetTableAddress().GetVersion().GetLiteral() + ".met";
    //printf("newRootPath: %s\n", newRootPath.c_str());
    cor::File::CreateSymlink(tempSymlinkName, newRootPath);
    cor::File::Rename(tempSymlinkName, symlinkName);
    //printf("renamed %s to %s\n", tempSymlinkName.c_str(), symlinkName.c_str());

    Cache::Get().Put(newHead);

    mHeadVersion = newHead->GetTableAddress().GetVersion();
}

void
BranchImp::DeleteColumn(const MetaKey& column)
{
    try {
        DEBUG_LOCAL("BranchImp::DeleteColumn %s\n", column.GetLiteral().c_str());

        // grab write access to eternity
        cor::TimeRange tr(cor::Time::NegativeInf(), cor::Time::PositiveInf());

        // don't care about any race condition here because the column could be
        // un-deleted (written to) at any time, including right after this...which
        // would look the same as if it occurred, for instance, right here.

        AccessPtr ac = mDatabase.CreateAccess(mBranchPath, tr, eWrite);
        ac->Open();
        IOOperationPtr op = ac.CreateIOOperation(tr, eIterateForwards);
        if (!op)
            return; // branch no longer exists!

        op->Write(column, RecordVector());
        op->Commit();

    } catch (cor::Exception& err)
    {
        err.SetWhat("BranchImp::DeleteColumn() -- %s", err.what());
        throw err;
    }

}

void
BranchImp::GetColumns(const MetaKey &column, std::vector<MetaKey> &columns)
{
    try {
        DEBUG_LOCAL("BranchImp::DeleteColumn %s\n", column.GetLiteral().c_str());

        // grab write access to range that includes all of this column's data
        MetaDataTable head = GetHead();

        head->RootGetColumns(column, columns);

    } catch (cor::Exception& err)
    {
        err.SetWhat("BranchImp::GetColumns() -- %s", err.what());
        throw err;
    }
}

bool
BranchImp::HasColumn(const nabu::MetaKey &column)
{
    try {
        DEBUG_LOCAL("BranchImp::HasColumn %s\n", column.GetLiteral().c_str());

        return GetHead()->HasColumn(column);
    } catch (cor::Exception& err)
    {
        err.SetWhat("BranchImp::HasColumn() -- %s", err.what());
        throw err;
    }
}

void
BranchImp::GetJournals(JournalMap& journals)
{
    cor::ObjectLocker ol(mJournalMutex, "DB " + mDatabase.GetLiteral() + " GetJournals");

    journals.clear();
    journals = mJournals;
}

Journal
BranchImp::GetJournal(const MetaKey& column)
{
    cor::ObjectLocker ol(mJournalMutex, "DB " + mDatabase.GetLiteral() + " GetJournal");

    Journal r;
    JournalMap::iterator i = mJournals.find(column);
    if (i != mJournals.end())
        r = i->second;
    return r;
}

AccessTable&
BranchImp::GetAccessTable(const std::string& tagName)
{
    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " GetAccessTable");

    if (tagName.empty())
        return mAccessTable;

    TagImpTable::const_iterator i = mTagImpTable.find(tagName);

    // The caller may be trying to see if the tag exists or not, and if so,
    // gain access to it, but throw and let them catch if this is the intent
    if (i == mTagImpTable.end())
    {
        throw cor::Exception("BranchImp::GetAccessTable() -- no tag '%s' in database '%s'",
            tagName.c_str(),
            GetDatabase().GetLiteral().c_str());
    }

    return i->second->GetAccessTable();
}

void
BranchImp::FlushJournals()
{
    //DEBUG_LOCAL("BranchImp::FlushJournals()%s\n","");
    Context ctx("FlushJournals");
    JournalMap journals;
    GetJournals(journals);

    if (journals.empty()) // nothing to do
    {
        DEBUG_LOCAL("Journals are empty, done flushing%s\n","");
        return;
    }

    // find extents of new data to write
    cor::TimeRange writeExtents;

    // The journals are not locked during the flushing, deliberately,
    // so that incoming data is not stalled. This means that the Journal
    // objects, though threadsafe, will be changing during the duration
    // of this call. This is fine, so long as the state of the Journal
    // is used from a single moment so that it is coherent.
    // To this point, the extents that are going to be written are
    // determined below, and the time range of each journal used for
    // calculating that extent is noted. When the Write occurs, this
    // time range is used instead of the current time range of the Journal
    // since it easily could have gotten new data in the meantime. That
    // is OK; the new data will be flushed next time around.

    // produce the extents of all columns, to make the data appear atomically
    {
        JournalMap::const_iterator jmci = journals.begin();
        for (; jmci != journals.end(); jmci++)
        {
            cor::TimeRange tr = jmci->second->Extents();
            if (tr.Valid())
            {
                if (!writeExtents.Valid())
                    writeExtents = tr;
                else
                    writeExtents = writeExtents.SuperUnion(tr);
            }
        }
    }

    if (writeExtents.Valid())
    {
        JournalMap::iterator jmi = journals.begin();
        for (; jmi != journals.end(); jmi++)
        {
            Journal journal = jmi->second;

            cor::TimeRange tr = journal->Extents();
            DEBUG_LOCAL("Flushing journal %s: %s...\n", jmi->first.GetLiteral().c_str(), tr.Print().c_str());

            // nothing to write
            if (!tr.Valid())
            {
                DEBUG_LOCAL("BranchImp::FlushJournals() -- extents empty, nothing to write%s\n","");
                continue;
            }

            AccessPtr ac = mDatabase.CreateAccess(mBranchPath, tr, eWrite);
            ac->Open();

            DEBUG_LOCAL("BranchImp::FlushJournals() -- %s flushing %ld to %ld\n",
                        jmi->first.GetLiteral().c_str(),
                        tr.First().SecondPortion(),
                        tr.Final().SecondPortion());
            RecordVector records;
            journal->Read(tr, records);
            DEBUG_LOCAL("BranchImp::FlushJournals() -- %s has %ld records to flush\n",
                        jmi->first.GetLiteral().c_str(), records.size());
            if (!records.empty())
            {
                DEBUG_LOCAL("Read %ld records, %ld to %ld from journal\n",
                            records.size(),
                            records.front().GetTime().SecondPortion(),
                            records.back().GetTime().SecondPortion());
            }
            // sanity
            if (records.empty())
            {
                throw cor::Exception("Non-empty write timerange %s for journal at %s but no data to write",
                                     tr.Print().c_str(),  jmi->first.GetLiteral().c_str());
            }

            IOOperationPtr op = ac.CreateIOOperation(tr, eIterateForwards);
            if (!op)
                return; // branch no longer exists!

            op->Write(jmi->first, records);
            op->Commit();

            DEBUG_LOCAL("BranchImp::FlushJournals() -- %s wrote %ld records ending at %ld, flushing up to %ld\n",
                        jmi->first.GetLiteral().c_str(),
                        records.size(),
                        records.back().GetTime().SecondPortion(),
                        tr.Final().SecondPortion());

            journal->Clear(tr.Final());
        }
    }
    else
    {
        //DEBUG_LOCAL("All journals are empty%s\n","");
    }
}

void
BranchImp::LoadJournals()
{
    DEBUG_LOCAL("BranchImp::LoadJournals()%s\n","");

    // create Journals for each Address
    MetaDataTable head = GetHead();

    std::vector<MetaKey> columns;
    head->RootGetColumns(MetaKey(), columns);
    for (size_t i = 0; i < columns.size(); i++)
    {
        const MetaKey k = columns[i];
        Journal newJournal = Journal(new JournalImp(this, k));

        try
        {
            newJournal->Load();

            cor::ObjectLocker ol(mJournalMutex, "DB " + mDatabase.GetLiteral() + " LoadJournals");
            mJournals[k] = newJournal;
        }
        catch (const cor::Exception& err)
        {
            printf("Error loading journal %s: %s\n", k.GetLiteral().c_str(), err.what());
        }
    }
}

std::shared_ptr<BranchImp>
BranchImp::GetBranch(const MetaKey& branchPath)
{
    // DEFENSIVE
    if (branchPath.Empty())
        throw cor::Exception("BranchImp::GetBranch() -- empty path");

    MetaKey v = branchPath;
    v.PopFront();

    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " GetBranch");
    BranchTable::iterator i = mBranchTable.find(branchPath.Front());

    BranchImpPtr r;
    if (i != mBranchTable.end())
        r = i->second;

    return r;
}

bool
BranchImp::VersionInUse(const TableAddress& nodeAddress)
{
    //printf("BranchImp::VersionInUse -- %s\n", nodeAddress.GetLiteral().c_str());
    Trace::Printf("%s BranchImp::VersionInUse -- %s\n", mDatabase.GetLiteral().c_str(), nodeAddress.GetLiteral().c_str());

    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " VersionInUse");

    // check head
    MetaDataTable head = GetHead();
    Version v = head->GetCurrentVersion(mDatabase, nodeAddress.GetColumn(), nodeAddress.GetTimePath());
    if (v == nodeAddress.GetVersion())
    {
        Trace::Printf("VersionInUse -- address '%s' is used by Head\n", nodeAddress.GetLiteral().c_str());
        return true;
    }

    // check tags
    {
        TagImpTable::const_iterator i = mTagImpTable.begin();
        for (; i != mTagImpTable.end(); i++)
        {
            MetaDataTable tag = GetTagMetaData(i->first);

            v = tag->GetCurrentVersion(mDatabase, nodeAddress.GetColumn(), nodeAddress.GetTimePath());
            Trace::Printf("VersionInUse -- tag %s = %s\n", i->first.c_str(), i->second->GetVersion().GetLiteral().c_str());
            Trace::Printf("   %d %d\n", nodeAddress.GetColumn().IsUnknown(), nodeAddress.GetTimePath().Valid());
            if (v == nodeAddress.GetVersion())
            {
                Trace::Printf("   in use\n");
                return true;
            }
            Trace::Printf("   not in use\n");
            Trace::Printf("   v = %s\n", v.GetLiteral().c_str());
            Trace::Printf("   nodeAddress.v = %s\n",
                          nodeAddress.GetVersion().GetLiteral().c_str());
        }
    }

    // check branches
    {
        BranchTable::const_iterator i = mBranchTable.begin();
        for (; i != mBranchTable.end(); i++)
        {
            if (i->second->VersionInUse(nodeAddress))
                return true;
        }
    }

    // not in use by this branch
    return false;
}

void
BranchImp::GetUnusedVersions(const TableAddress& ta, MetaDataMap diffs)
{
    DEBUG_LOCAL("BranchImp::GetUnusedVersions -- %s\n", ta.GetLiteral().c_str());
    Trace::Printf("%s BranchImp::GetUnusedVersions\n", mDatabase.GetLiteral().c_str());

    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " GetUnusedVersions");

    MetaDataMapImp::iterator j = diffs->begin();

    while (j!= diffs->end())
    {
        TableAddress nodeAddress(ta.GetColumn(), j->second.mPath, j->second.mVersion);

        // check head
        MetaDataTable head = GetHead();
        Version v = head->GetCurrentVersion(mDatabase, nodeAddress.GetColumn(), nodeAddress.GetTimePath());
        if (v == nodeAddress.GetVersion())
        {
            Trace::Printf("GetUnusedVersions -- address '%s' is used by Head\n", nodeAddress.GetLiteral().c_str());
            // stay compatible with pre C++11
            MetaDataMapImp::iterator jc = j++;
            diffs->erase(jc);
            continue;
        }

        // check tags
        {
            TagImpTable::const_iterator i = mTagImpTable.begin();
            for (; i != mTagImpTable.end(); i++)
            {
                MetaDataTable tag = GetTagMetaData(i->first);

                v = tag->GetCurrentVersion(mDatabase, nodeAddress.GetColumn(), nodeAddress.GetTimePath());
                Trace::Printf("GetUnusedVersions -- tag %s = %s\n", i->first.c_str(), i->second->GetVersion().GetLiteral().c_str());
                Trace::Printf("   %d %d\n", nodeAddress.GetColumn().IsUnknown(), nodeAddress.GetTimePath().Valid());
                if (v == nodeAddress.GetVersion())
                {
                    Trace::Printf("   in use\n");
                    // stay compatible with pre C++11
                    MetaDataMapImp::iterator jc = j++;
                    diffs->erase(jc);
                    // existence in any tag is sufficient; continue with next entry in outer
                    // loop
                    break;
                }
                Trace::Printf("   not in use\n");
                Trace::Printf("   v = %s\n", v.GetLiteral().c_str());
                Trace::Printf("   nodeAddress.v = %s\n",
                              nodeAddress.GetVersion().GetLiteral().c_str());
            }
            // early exit means entry was found
            if (i != mTagImpTable.end())
                continue;
        }


        // check branches
        if (0) {
            BranchTable::const_iterator i = mBranchTable.begin();
            for (; i != mBranchTable.end(); i++)
            {
                if (i->second->VersionInUse(nodeAddress))
                {
                    break;
                }
            }
            if (i != mBranchTable.end())
            {
                // stay compatible with pre C++11
                MetaDataMapImp::iterator jc = j++;
                diffs->erase(jc);
                continue;
            }
        }

        Trace::Printf("not in use\n");
        j++;
    }
}

CopyStats
BranchImp::Sync(BranchPtr source, BranchPtr destination, SyncDirection syncDirection)
{
    DEBUG_LOCAL("BranchImp::Sync() -- source '%s', destination '%s', branch '%s', %s\n",
        source->GetDatabase().GetLiteral().c_str(),
        destination->GetDatabase().GetLiteral().c_str(),
        destination->GetLiteral().c_str(),
        syncDirection == ePush ? "Push" : "Pull");

    // make sure both branches are the same
    if (source->GetBranchPath() != destination->GetBranchPath())
        throw cor::Exception("BranchImp::Sync() -- source '%s' is not same branch as destination '%s'",
            source->GetLiteral().c_str(),
            destination->GetLiteral().c_str());
    const MetaKey branchPath = source->GetBranchPath();
    
    CopyStats copied;

    // copy head of branch, possibly creating new destination branch in the process
    std::string s = FileNamer::UniqueLabelName();

    // create temporary tag on source branch
    /*AccessPtr sourceTagAccess = source->GetDatabase().CreateAccess(
            branchPath,
            cor::TimeRange(cor::Time::NegativeInf(), cor::Time::NegativeInf()),
            eWrite);

    sourceTagAccess->Open("Sync %s -> %s: Waiting for source branch '%s' to be available",
        source->GetDatabase().GetLiteral().c_str(),
        destination->GetDatabase().GetLiteral().c_str(),
        branchPath.GetLiteral().c_str());

    TagPtr sourceTag = sourceTagAccess->CreateTag(s, eTemporary);*/
    TagPtr sourceTag = source->CreateTag(s, eTemporary, Version());

    // Data Integrity During Push and Pull
    //
    // If there are modifications to the head of the branch on either end of the
    // synchronization action, a race condition exists that will cause one of
    // the competing transactions to "lose", the result of which is a social
    // problem. However, there is the danger of having an expected file not
    // exist at the end of the synch, which is database corruption. The
    // source of this danger is that one side is comparing its tree of (meta)data
    // against the other, and will skip a node as already synchronized just
    // before the competing operation deletes it through a local write.
    //
    // The source of the synch operation can be stabilized by a tag, or snapshot.
    //
    // The destination can't work this way. Consider a destination branch with one
    // tag. Some files on head are referenced by this tag, others are unique to the
    // tag. An incoming new tag will have its own mix of unique files as well as
    // some shared by the existing local tag.
    // Deleting the local tag can drop the final reference of some of the shared
    // files even as the incoming tag is being created; the result is that the new
    // tag references deleted data. Once the incoming tag is completely in place,
    // garbage collection will have the correct reference count for all data and
    // the danger is past.
    //
    // The solution to this is to disable garbage collection until the synch action
    // has completed.

    AccessPtr destinationAccess = destination->GetDatabase().CreateAccess(
        branchPath,
        cor::TimeRange(cor::Time::NegativeInf(), cor::Time::NegativeInf()),
        eWrite
        );
    //destinationAccess->Open("Gaining access to destination database branch");
    destinationAccess->DisableGarbageCollection();

    // tag may be null if the main branch is empty
    if (sourceTag)
    {
        // find head metadata table of the tag, and have it copy all
        // unique nodes from this
        MetaDataTable rootOfSync = sourceTag->GetHeadTable();

        if (syncDirection == ePush)
        {
            copied += rootOfSync->RootPushUniqueNodes(source->GetDatabaseImp(), destination->GetDatabase());
        }
        else
        {
            copied += rootOfSync->RootPullUniqueNodes(source->GetDatabase(), destination->GetDatabaseImp());
        }

        destination->MoveHead(sourceTag->GetVersion());

        source->DeleteTag(sourceTag->GetLiteral());

    }
    else
    {
        printf("Could not create temporary tag: is the source database '%s' empty?\n",
            source->GetDatabase().GetLiteral().c_str());
    }

    // copy tags on the source branch
    std::set<std::string> sourceTags;
    source->GetTags(sourceTags);
    std::set<std::string>::iterator i = sourceTags.begin();
    for (; i != sourceTags.end(); i++)
    {
        const std::string tagName = *i;

        // this access makes sure the tag is not changed while
        // the operation is underway
       /* AccessPtr accessPtr = source->GetDatabase().CreateAccess(branchPath,
            cor::TimeRange(cor::Time::NegativeInf(), cor::Time::NegativeInf()),
             eRead,
             tagName);*/

        //if (accessPtr)
        {
            TagPtr localTag = source->GetTag(tagName);
            if (!localTag)
            {
                //printf("Skipping tag %s: it no longer exists\n", tagName.c_str());
                continue;
            }

            MetaDataTable rootOfSync = localTag->GetHeadTable();

            if (syncDirection == ePush)
            {
                copied += rootOfSync->RootPushUniqueNodes(source->GetDatabaseImp(), destination->GetDatabase());
            }
            else
            {
                copied += rootOfSync->RootPullUniqueNodes(source->GetDatabase(), destination->GetDatabaseImp());
            }

            // create (or move) the destination tag here
            /*AccessPtr a = destination->GetDatabase().CreateAccess(branchPath,
                                                     cor::TimeRange(cor::Time::NegativeInf(), cor::Time::NegativeInf()),
                                                     eWrite,
                                                     tagName);
            // the access may not exist if the tag does not exist locally, but if
            // it does, this operation must wait until the local tag is available
            // for moving
            // XXX DEADLOCK THREAT WITH ABOVE ACCESS ON SOURCE DATABASE !!!
            if (a)
            {
                a->Open("Push: Waiting for branch %s tag '%s' to be available",
                            branchPath.GetLiteral().c_str(),
                            tagName.c_str());
            }*/
            destination->CreateTag(tagName, eMove, localTag->GetVersion());
        }

        // remove all destination tags that do not exist in the source
        destination->TrimTags(sourceTags);
    }

    return copied;
}

void
BranchImp::Render(Json::Value& into)
{
    DEBUG_LOCAL("BranchImp::Render -- %s\n", GetLiteral().c_str());
    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " Render");

    {
        Json::Value tags;
        TagImpTable::const_iterator i = mTagImpTable.begin();
        for (; i != mTagImpTable.end(); i++)
        {
            Json::Value v;
            // do not persist temporary tags
            if (!i->second->IsTemporary())
            {
                i->second->RenderToJson(v);
                tags[i->first] = v;
            }
        }
        if (!mTagImpTable.empty())
            into["tags"] = tags;
    }

    {
        Json::Value branches;
        BranchTable::const_iterator i = mBranchTable.begin();
        for (; i != mBranchTable.end(); i++)
        {
            Json::Value v;
            i->second->Render(v);
            branches[i->first] = v;
        }
        if (!mBranchTable.empty())
           into["branches"] = branches;
    }

}

void
BranchImp::Parse(const Json::Value& from)
{
    DEBUG_LOCAL("BranchImp::Parse()%s\n","");
    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " Parse");

    const Json::Value& tags = from.get("tags", Json::Value());
    if (!tags.isNull())
    {
        if (!tags.isObject())
            throw cor::Exception("Expected JSON Object for tags");
        Json::ValueConstIterator i = tags.begin();
        for (; i != tags.end(); i++)
        {
            std::string name = i.key().asString();
            Version v;
            v.ParseFromJson(*i);
            const bool temporary = false;
            MetaDataTable tagRoot = GetTagMetaData(name);
            // DEFENSIVE
            if (!tagRoot)
                throw cor::Exception("BranchImp::Parse() -- JSON specifies tag '%s' at version '%s' that does not exist",
                    name.c_str(),
                    v.GetLiteral().c_str());
            mTagImpTable[name] = std::shared_ptr<TagImp>(new TagImp(*this, name, v, tagRoot, temporary));
        }
    }

    const Json::Value& branches = from.get("branches", Json::Value());
    if (!branches.isNull())
    {
        if (!branches.isObject())
            throw cor::Exception("Expected JSON Object for branches");
        Json::ValueConstIterator i = branches.begin();
        for (; i != branches.end(); i++)
        {
            std::string name = i.key().asString();
            BranchImpPtr branch(new BranchImp(mDatabase, name));
            branch->Parse(*i);
            mBranchTable[name] = branch;
        }
    }

}

void
BranchImp::Initialize(const Version& headVersion)
{
    DEBUG_LOCAL("BranchImp::Initialize()%s\n","");

    {
        cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " Initialize");

        // DEFENSIVE
        if (mHeadVersion.Valid())
        {
            // this would cause unexpected version regression; this is only to be called
            // one time, when a database instance is starting
            throw cor::Exception("BranchImp::Initialize() called on already initialized branch");
        }
    }

    // should return "/opt/mag_nabu/root.BRANCH_A.*.met", etc.
    /*std::string searchPath = mDatabase.GetFileNamer().GetBranchGlob(mBranchPath);

    std::vector<std::string> roots;

    cor::File::FindFilenames(searchPath, roots);
    DEBUG_LOCAL("BranchImp::Initialize() -- Found %ld roots from search '%s'\n", roots.size(), searchPath.c_str());

    if (roots.empty())
    {
        printf("BranchImp::Initialize() -- No valid root metadata for '%s'\n",
                             mDatabase.GetFileNamer().GetRoot().c_str());
        return;
    }

    // DEFENSIVE
    if (roots.size() > 1)
        throw cor::Exception("BranchImp::Initialize() -- multiple roots found in '%s'", searchPath.c_str());

    std::string s = cor::File::FollowSymlink(roots.front()); */
    {
        cor::ObjectLocker ol(mHeadMutex, "DB " + mDatabase.GetLiteral() + " Initialize");
        mHeadVersion = headVersion;
        //printf("head of branch %s is version %s\n", mBranchPath.GetLiteral().c_str(), mHeadVersion.GetLiteral().c_str());
    }

    try {
        LoadJournals();
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("BranchImp::Initialize() -- %s", err.what());
        throw err;
    }

    DEBUG_LOCAL("BranchImp::Initialize() -- complete%s\n","");
}

MetaDataTable
BranchImp::GetHead()
{
    cor::ObjectLocker ol(mHeadMutex, "DB " + mDatabase.GetLiteral() + " GetHead");

    TableAddress ta(MetaKey(), TimePath(), mHeadVersion);
    return Cache::Get().Find(mDatabase, ta);
}

MetaDataTable
BranchImp::GetTagMetaData(const std::string& tagName)
{
    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " GetTag");

    TagImpTable::const_iterator i = mTagImpTable.find(tagName);
    if (i == mTagImpTable.end())
        return MetaDataTable();

    TableAddress ta(MetaKey(), TimePath(), i->second->GetVersion());
    return Cache::Get().Find(mDatabase, ta);
}

size_t
BranchImp::TotalSize()
{
    DEBUG_LOCAL("BranchImp::TotalSize -- %s\n", GetLiteral().c_str());

    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " TotalSize");

    // head
    size_t count = GetHead()->TotalSize(mDatabase);

    // tags
    {
        TagImpTable::const_iterator i = mTagImpTable.begin();
        for (; i != mTagImpTable.end(); i++)
        {
            count += GetTagMetaData(i->first)->TotalSize(mDatabase);
        }
    }

    // branches
    {
        BranchTable::const_iterator i = mBranchTable.begin();
        for (; i != mBranchTable.end(); i++)
        {
            count += i->second->TotalSize();
        }
    }

    return count;
}

void
BranchImp::CountMissing(AsyncStatus& stats)
{
    DEBUG_LOCAL("BranchImp::CountMissing -- %s\n", GetLiteral().c_str());

    cor::ObjectLocker rl(mLabelMutex,"DB " + mDatabase.GetLiteral() + " CountMissing");

    // check head
    MetaDataTable head = GetHead();
    head->RootCountMissing(mDatabase, stats);

    // check tags
    {
        TagImpTable::const_iterator i = mTagImpTable.begin();
        for (; i != mTagImpTable.end(); i++)
        {
            MetaDataTable tag = GetTagMetaData(i->first);

            tag->RootCountMissing(mDatabase, stats);
        }
    }

    // check branches
    {
        BranchTable::const_iterator i = mBranchTable.begin();
        for (; i != mBranchTable.end(); i++)
        {
            i->second->CountMissing(stats);
        }
    }

}

TagImpPtr
BranchImp::GetTagImp(TagPtr tag)
{
    cor::ObjectLocker rl(mLabelMutex, "DB " + mDatabase.GetLiteral() + " GetTagImp");
    TagImpTable::iterator i = mTagImpTable.find(tag->GetLiteral());

    // SANITY
    if (i == mTagImpTable.end())
        throw cor::Exception("BranchImp::GetTagImp -- tag implementation for '%s' not found", tag->GetLiteral().c_str());

    return i->second;
}

}