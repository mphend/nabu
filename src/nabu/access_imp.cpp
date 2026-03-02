//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <mcor/json_time.h>

#include "access_imp.h"
#include "database.h"
#include "database_imp.h"
#include "branch.h"
#include "io_op_imp.h"
#include "leak_detector.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {


AccessImp::AccessImp(std::shared_ptr<BranchImp> branchImp,
                     const std::string& tagName,
                     const muuid::uuid& handle,
                     const cor::TimeRange& timeRange,
                     const cor::TimeRange& snappedRange,
                     AccessType type) :
                     Access(branchImp, handle, type, timeRange, tagName),
                     mBranchImp(branchImp),
                     mSnapped(snappedRange),
                     mMutex("AccessImp")
{
    DEBUG_LOCAL("AccessImp::AccessImp() %s\n", mHandle.string().c_str());
    mBranchImp->GetAccessTable(GetTagName()).AddAccess(this);
    LeakDetector::Get().Increment("Access");
}

AccessImp::~AccessImp()
{
    DEBUG_LOCAL("~AccessImp() %s (tag %s)\n", mHandle.string().c_str(), mTagName.c_str());

    Close();

    // this does not actually enable GC unless it is the final access out
    EnableGarbageCollection();
    LeakDetector::Get().Decrement("Access");
}

void
AccessImp::AddOperation(IOOperationImp* iop)
{
    cor::ObjectLocker ol(mMutex);

    // verify time range of IOP
    if (!iop->GetTimeRange().Within(mTimeRange))
    {
        throw cor::Exception("Access::AddOperation() -- IOOperation time range (%s) extends outside that of access (%s)",
                             iop->GetTimeRange().Print().c_str(),
                             mTimeRange.Print().c_str());
    }

    // DEFENSIVE
    if (mIOOperations.find(iop->GetHandle()) != mIOOperations.end())
    {
        throw cor::Exception("AccessImp::AddOperation() -- IOP has already been added");
    }

    mIOOperations[iop->GetHandle()] = iop;
}

void
AccessImp::RemoveOperation(IOOperationImp* iop)
{
    cor::ObjectLocker ol(mMutex);

    IOOperationMap::iterator i = mIOOperations.find(iop->GetHandle());

    // DEFENSIVE
    if (i == mIOOperations.end())
    {
        printf("\n\n\n!!!!!!!!!!!!!!!!!!!!!!!\n\nAccessImp::RemoveOperation() -- IOP '%s' not found\n\n\n",
               iop->GetHandle().string().c_str());
        throw cor::Exception("AccessImp::RemoveOperation() -- IOP '%s' not found\n",
               iop->GetHandle().string().c_str());
    }

    WrittenColumnsMap wcm;
    cor::TimeRange wtr;
    //printf("AccessImp::RemoveOperation %s\n", iop->GetHandle().string().c_str());
    iop->GetWrittenColumns(wcm, wtr);
    mWrittenColumnsMap.Merge(wcm);
    if (!wtr.Valid())
    {
        wtr = mWrittenTimeRange;
    }
    else
    {
        mWrittenTimeRange = mWrittenTimeRange.SuperUnion(wtr);
    }
    /*printf("RemoveOperation() -- %s Access %s has %ld written columns\n",
           mType == eWrite ? "Write" : "Read",
           GetHandle().string().c_str(),
           mWrittenColumnsMap.size());*/

    mIOOperations.erase(i);
}

std::string
AccessImp::Print() const
{
    cor::ObjectLocker ol(mMutex);

    std::string type = mType == eRead ? "Read" : mType == eWrite ? "Write" : "Invalid";
    return mHandle.string() + " : " + type + " " + mTimeRange.Print();
}

const WrittenColumnsMap&
AccessImp::GetWrittenColumns(cor::TimeRange& extents) const
{
    cor::ObjectLocker ol(mMutex);
    extents = mWrittenTimeRange;
    return mWrittenColumnsMap;
}

cor::MMutexPtr
AccessImp::GetColumnMutex(const MetaKey& column)
{
    cor::ObjectLocker ol(mMutex);
    ColumnMutexMap::iterator i = mColumnMutexMap.find(column);
    if (i == mColumnMutexMap.end())
    {
        mColumnMutexMap[column] = cor::MMutexPtr(new cor::MMutex(column.GetLiteral()+"_column_mutex"));
    }
    return mColumnMutexMap[column];
}

IOOperationPtr
AccessImp::CreateIOOperation(const cor::TimeRange& timeRange, IterationDirection direction)
{
    DEBUG_LOCAL("AccessImp::CreateIOOperation %s\n", timeRange.Print().c_str());

    IOOperationImpPtr iop(new IOOperationImp(mBranchImp->GetDatabaseImp(),
                                             timeRange,
                                             direction,
                                             muuid::uuid::random(),
                                             this,
                                             IOOperationImp::eNormal));


    return iop;
}

bool
AccessImp::Open(int waitSeconds)
{
    cor::ObjectLocker ol(mMutex);
    const std::string tagName = GetTagName();

    bool r = GetBranchImp()->GetAccessTable(GetTagName()).Ready(this, waitSeconds);

    if (r)
    {
        try
        {
            mTag = GetBranchImp()->GetTag(tagName);
            //printf("%s Open() got tag %s\n", GetHandle().string().c_str(), tagName.c_str());
        }
        catch (const cor::Exception& ex)
        {
            printf("AccessImp::Open : can't get tag '%s' : %s\n", tagName.c_str(), ex.what());
        }
    }
    else
    {
        //printf("Access %s (tag %s) waiting on:\n", GetHandle().string().c_str(), tagName.c_str());
        GetBranchImp()->GetAccessTable(GetTagName()).Print(std::cout);
    }

    return r;
}

void
AccessImp::Close()
{
    DEBUG_LOCAL("AccessImp::Close%s\n", "");
    cor::ObjectLocker ol(mMutex);

    // DEFENSIVE
    if (!mIOOperations.empty())
    {
        printf("AccessImp::Close() : not empty\n");
        printf("   Access: %s\n", Print().c_str());
        printf("IOs:\n");
        IOOperationMap::const_iterator i = mIOOperations.begin();
        for (; i != mIOOperations.end(); i++)
        {
            printf("%s\n", i->second->Print().c_str());
        }

        throw cor::Exception("~AccessImp : not empty");
    }

    GetBranchImp()->GetAccessTable(GetTagName()).RemoveAccess(this);
}

SelectResult
AccessImp::Select(const std::set<MetaKey>& selectColumns,
                  WrittenColumnsMap& writtenColumnsMap,
                  int waitSeconds)
{
    DEBUG_LOCAL("AccessImp::Select%s\n", "");

    AccessTable& at = GetBranchImp()->GetAccessTable(GetTagName());
    SelectResult r = at.Select(this, selectColumns, writtenColumnsMap, waitSeconds);
    return r;
}

bool
AccessImp::AbortSelect()
{
    DEBUG_LOCAL("AccessImp::AbortSelect%s\n", "");

    AccessTable& at = GetBranchImp()->GetAccessTable(GetTagName());
    return at.AbortSelect(this);
}

void
AccessImp::Extents(const std::set<MetaKey>& selectColumns, WrittenColumnsMap& wcm)
{
    cor::ObjectLocker ol(mMutex);
    wcm.clear();

    // look at each column, and put a single entry into the wcm that is the
    // total extent of the data that exists. Limit this to the time range of
    // this access.
    // Since data is not actually being written, there is no meaning to the set
    // of timeranges as demarcation of write operations.

    // if selectColumns is empty, then get all columns
    if (selectColumns.empty())
    {
        std::vector<nabu::MetaKey> allColumns;
        GetBranchImp()->GetColumns(nabu::MetaKey(), allColumns);
        std::map<nabu::MetaKey, cor::TimeRange> extentsMap;
        for (size_t i = 0; i < allColumns.size(); i++)
        {
            cor::TimeRange tr = GetBranchImp()->Extents(allColumns[i], nabu::Branch::eReadable);
            wcm[allColumns[i].GetLiteral()].insert(tr);
        }

        return;
    }

    std::set<MetaKey>::const_iterator i = selectColumns.begin();
    for (; i != selectColumns.end(); i++)
    {
        wcm[*i].insert(GetBranchImp()->Extents(*i, Branch::eReadable).Intersection(mTimeRange));
    }
}

/*
TagPtr
AccessImp::CreateTag(const std::string& newTagName, TagPolicy tagPolicy)
{
    if (!IsWrite())
        throw cor::Exception("AccessImp::CreateTag() -- not open for write");

    DEBUG_LOCAL("AccessImp::CreateTag() -- %s, tag '%s'\n",
                mHandle.string().c_str(),
                newTagName.c_str());

    cor::ObjectLocker ol(mMutex);

    // do nothing is the branch is empty
    if (!GetRoot()->GetTableAddress().GetVersion().Valid())
    {
        DEBUG_LOCAL("AccessImp::CreateTag() -- ignoring tag (%s) of empty branch: database '%s'\n",
            newTagName.c_str(),
            mBranchImp->GetDatabase().GetLiteral().c_str());
        return TagPtr();
    }

    // if intent is to move the tag, then this operation should have been opened with
    // the existing tag
    if (tagPolicy == eMove)
    {
        if (newTagName != GetTagName())
        {
            throw cor::Exception("AccessImp::CreateTag() -- move policy allowed only when opened on same tag name");
        }
    }

    TagPtr r = GetBranchImp()->TagHead(newTagName, tagPolicy);
    if (!r)
    {
        throw cor::Exception("AccessImp::CreateTag() -- tag %s of head failed in database '%s'",
            newTagName.c_str(),
            mBranchImp->GetDatabase().GetLiteral().c_str());
    }

    return r;
}*/


void
AccessImp::Tick()
{}

void
AccessImp::DisableGarbageCollection()
{
    GetBranchImp()->GetDatabaseImp().GetGarbage().Lock(mHandle.string());
}

void
AccessImp::EnableGarbageCollection()
{
    GetBranchImp()->GetDatabaseImp().GetGarbage().Unlock(mHandle.string());
}

void
AccessImp::GetChildKeys(std::set<std::string>& keys)
{
    cor::ObjectLocker ol(mMutex);
    IOOperationMap::const_iterator i = mIOOperations.begin();
    for (; i != mIOOperations.end(); i++)
        keys.insert(i->first.string());
}

MetaDataTable
AccessImp::GetRoot()
{
    // if working from a tag, use that root; otherwise, use head
    if (!GetTagName().empty())
    {
        return GetBranchImp()->GetTagMetaData(GetTagName());
    }
    else
    {
        return GetBranchImp()->GetHead();
    }
}

std::ostream&
operator<<(std::ostream& os, const AccessImp& a)
{
    os << a.Print();
    return os;
}

}
