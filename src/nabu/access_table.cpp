//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include "access_imp.h"
#include "access_table.h"
#include "database_imp.h"
#include "exception.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

// these are mutually exclusive policies, but split out for readability
#define READ_WRITE_POLICY
//#define WRITE_MUTEX_POLICY

namespace nabu {

/** class AccessStateImp : the 'value' type of access tracking
 *
 */
class AccessStateImp
{
public:
    enum State { ePending, eActive, eInvalid };

    std::set<MetaKey> mSelectColumns;
    WrittenColumnsMap mWrittenColumnMap;
    const muuid::uuid mHandle;
    const AccessType mType;
    const cor::TimeRange mSnappedTimeRange;
    const cor::TimeRange mTimeRange;

    bool IsRead() const { return mType == eRead; }
    bool IsWrite() const { return mType == eWrite; }
    muuid::uuid GetHandle() const { return mHandle; }

    //AccessStateImp();
    AccessStateImp(State state,
                   const muuid::uuid& handle,
                   AccessType type,
                   const cor::TimeRange& snappedTimeRange,
                   const cor::TimeRange& timeRange);
    ~AccessStateImp();

    bool IsActive() const;
    bool IsPending() const;
    bool IsInSelect() const;

    void SetState(State);
    void SetInSelect(bool b);

    std::string Print() const;

    // negative value => wait indefinitely
    SelectResult Wait(int waitSeconds);
    void Signal();
    void Abort();

private:
    mutable cor::MMutex mMutex;
    cor::MCondition mCondition;

    State mState;
    // this is true when in a Select call; it is both set and cleared
    // only by the thread calling Select(), not by the thread that
    // signals.
    bool mInSelect;

    bool mAbort; // abort a Select

};

AccessStateImp::AccessStateImp(State state,
                               const muuid::uuid& handle,
                               AccessType type,
                               const cor::TimeRange& snappedTimeRange,
                               const cor::TimeRange& timeRange) :
        mState(state),
        mInSelect(false),
        mHandle(handle),
        mType(type),
        mSnappedTimeRange(snappedTimeRange),
        mTimeRange(timeRange),
        mMutex("AccessState", false),
        mCondition("AccessState", mMutex),
        mAbort(false)
{
    //printf("AccessStateImp::AccessStateImp '%s'\n", mHandle.string().c_str());
}

AccessStateImp::~AccessStateImp()
{
    //printf("AccessStateImp::~AccessStateImp '%s'\n", mHandle.string().c_str());
}

bool
AccessStateImp::IsActive() const { cor::ObjectLocker ol(mMutex, "IsActive"); return mState == eActive; }
bool
AccessStateImp::IsPending() const {  cor::ObjectLocker ol(mMutex, "IsPending"); return mState == ePending; }
bool
AccessStateImp::IsInSelect() const {  cor::ObjectLocker ol(mMutex, "IsInSelect"); return mInSelect; }

void
AccessStateImp::SetState(State s)
{
    cor::ObjectLocker ol(mMutex, "SetState");
    mState = s;
}

void
AccessStateImp::SetInSelect(bool b)
{
    cor::ObjectLocker ol(mMutex, "SetInSelect");
    mInSelect = b;
}

std::string
AccessStateImp::Print() const
{
    std::string type = mType == eRead ? "Read" : mType == eWrite ? "Write" : "Invalid";
    std::string s = mHandle.string() + " : " + type + " " + mSnappedTimeRange.Print();
    s += mState == ePending ? " (Pending)" : mState == eActive ? " (Active)" : " (Invalid)";
    return s;
}

SelectResult
AccessStateImp::Wait(int waitSeconds)
{
    DEBUG_LOCAL("AccessStateImp::Wait() %d seconds\n", waitSeconds);
    cor::ObjectLocker ol(mMutex, "Wait");

    // Don't try using IsActive() here; mutex is not reentrant
    if (mState == eActive)
        return eComplete;
    if (waitSeconds < 0)
    {
        mCondition.Wait();
        return eComplete;
    }
    if (mCondition.Wait(waitSeconds * 1000))
    {
        return mAbort ? eAbort : eComplete;
    }
    return eTimeout;
}

void
AccessStateImp::Signal()
{
    DEBUG_LOCAL("AccessStateImp::Signal()%s\n", "");
    mCondition.Signal();
}

void
AccessStateImp::Abort()
{
    DEBUG_LOCAL("AccessStateImp::Abort()%s\n", "");
    mAbort = true;
    mCondition.Signal();
}


AccessTable::AccessTable(const std::string& name, AccessPolicy policy) :
    mName(name),
    mAccessPolicy(policy),
    mMutex("AccessTable")
{}

AccessTable::~AccessTable()
{}

void
AccessTable::AddAccess(AccessImp* newAccess)
{
    cor::ObjectLocker ol(mMutex);

    DEBUG_LOCAL("Adding access %s to table %s\n", newAccess->Print().c_str(), mName.c_str());

    // DEFENSIVE : check for having already been added
    if (newAccess->IsRead())
    {
        if (mReads.find(newAccess->GetHandle()) != mReads.end())
            throw cor::Exception("AccessTable::Add() -- read access '%s' already exists",
                                 newAccess->GetHandle().string().c_str());
    }
    else if (newAccess->IsWrite())
    {
        if (mWrites.find(newAccess->GetHandle()) != mWrites.end())
            throw cor::Exception("AccessTable::Add() -- write access '%s' already exists",
                                 newAccess->GetHandle().string().c_str());
    }

    // if write, see if it overlaps an ongoing or pending conflicting access
    if (newAccess->IsWrite())
    {
        if (mAccessPolicy == eWriteMutex)
        {
            if (IntersectsAny(newAccess->mSnapped, mWrites))
            {
                DEBUG_LOCAL("Enqueueing new Access %s on table %s due to conflict with a write\n",
                            newAccess->Print().c_str(),
                            mName.c_str());
                AddPending(newAccess);
                return;
            }
        }
        else
        {
            if (IntersectsAny(newAccess->mSnapped, mReads))
            {
                DEBUG_LOCAL("Enqueueing new write Access %s on table %s due to conflict with a read\n",
                            newAccess->Print().c_str(),
                            mName.c_str());
                AddPending(newAccess);
                return;
            }
        }
    }

    if (mAccessPolicy == eReadWrite)
    {
        // all newAccesses must be exclusive of ongoing or pending writes
        if (IntersectsAny(newAccess->mSnapped, mWrites))
        {
            DEBUG_LOCAL("Enqueueing new Access %s on table %s due to conflict with a write\n",
                        newAccess->Print().c_str(),
                        mName.c_str());
            AddPending(newAccess);
            return;
        }
    }

    // no conflict; add this to appropriate
    DEBUG_LOCAL("Activating access %s on table %s\n", newAccess->Print().c_str(), mName.c_str());
    AddActive(newAccess);
}

void
AccessTable::RemoveAccess(AccessImp* accessImp)
{
    DEBUG_LOCAL("AccessTable::RemoveAccess%s\n", "");
    cor::ObjectLocker ol(mMutex);

    // find in one of the tables (read, write, or select)
    AccessState as = RemoveAccessFromMaps(accessImp->GetHandle());

    DEBUG_LOCAL("Removing access %s from table %s\n", as->Print().c_str(), mName.c_str());

    if (as->IsPending())
    {
        ClearPending(as);
    }
    else if (accessImp->IsWrite())
    {
        //printf("Write %s being removed\n", as->Print().c_str());
        cor::TimeRange extents;
        const WrittenColumnsMap& wcm = accessImp->GetWrittenColumns(extents);

        // Nothing might have been written (for instance, because of an Abort or
        // just closing the access after opening it), so extents can be Invalid
        if (extents.Valid())
        {
            // move any access in the select state that have intersection
            // with the writes that occurred into Pending
            ConsumeSelected(wcm, extents);
        }
    }

    ConsumePending();
}

bool
AccessTable::Ready(AccessImp* accessImp, unsigned int waitSeconds)
{
    DEBUG_LOCAL("AccessTable::Ready%s\n", "");
    AccessState state;
    {
        cor::ObjectLocker ol(mMutex);

        if (!Query(accessImp, state))
            throw cor::Exception("AccessTable::Ready() -- Could not find access '%s'", accessImp->Print().c_str());

        // DEFENSIVE -- don't allow an access already in select to be opened
        if (state->IsInSelect())
            throw cor::Exception("Access is in Select and cannot be Opened");
    }

    SelectResult r = state->Wait(waitSeconds);

    cor::ObjectLocker ol(mMutex);
    // SANITY
    if (r == eComplete && state->IsPending())
        throw FatalException(accessImp->GetBranchImp()->GetLiteral(),
                             "AccessTable::Ready() -- Wait returned true while Pending on table %s", mName.c_str());

    // this occurs if the process is being stopping or if this object is being
    // deleted, for instance if a tag is being destroyed by one thread (a writer) while
    // other (read) threads are waiting for access.
    //if (r == eAbort)
    //    throw cor::Exception("Access aborted");

    return r == eComplete;
}

SelectResult
AccessTable::Select(AccessImp* accessImp,
                    const std::set<MetaKey>& selectColumns,
                    WrittenColumnsMap& writtenColumnsMap,
                    unsigned int waitSeconds)
{
    DEBUG_LOCAL("AccessTable::Select%s\n", "");
    AccessState state;
    {
        cor::ObjectLocker ol(mMutex);
        {
            if (!Query(accessImp, state))
            {
                accessImp->Print();
                throw cor::Exception("AccessTable::Select() -- Could not find access '%s'", accessImp->Print().c_str());
            }
        }
        //printf("Select %s\n", accessImp->GetHandle().string().c_str());
        if (!state->IsInSelect()) // a new select?
        {
            if (!state->IsActive())
                throw cor::Exception("AccessTable::Select() -- access is not yet open");

            // move from Read/Write to Select
            mSelects[accessImp->GetHandle()] = state;
            if (accessImp->IsRead())
            {
                mReads.erase(accessImp->GetHandle());
            }
            else if (accessImp->IsWrite())
            {
                mWrites.erase(accessImp->GetHandle());
            }
            else
            {
                throw FatalException(accessImp->GetBranchImp()->GetLiteral(),
                                     "Access entering select state is neither read nor write");
            }

            // state is active; make it 'pending' (even though it is in select queue)
            state->SetState(AccessStateImp::ePending);
            state->mSelectColumns = selectColumns;
            state->SetInSelect(true);
            //printf("%s mInSelect is true\n", accessImp->GetHandle().string().c_str());
            // this removes the access from the read/write tables, and lets other accesses
            // that may be waiting enter the active state
            // RemoveAccess(accessImp);
            ConsumePending();
        }
    }

    //printf("Selecting ... %d sec wait\n", waitSeconds);
    // wait to see if this state becomes active
    SelectResult result = state->Wait(waitSeconds);
    if (result == eAbort)
        return result;
    //printf("Select: %s %s\n", accessImp->GetHandle().string().c_str(), SelectResultToString(result).c_str());

    cor::ObjectLocker ol(mMutex);
    if (result == eComplete)
    {
        writtenColumnsMap = state->mWrittenColumnMap;
        state->SetInSelect(false);
    }

    // SANITY
    if ((result == eComplete) && state->IsPending())
        throw FatalException(accessImp->GetBranchImp()->GetLiteral(),
                             "AccessTable::Select() -- Wait returned eComplete while Pending");

    return result;
}

bool
AccessTable::AbortSelect(AccessImp* accessImp)
{
    DEBUG_LOCAL("AccessTable::AbortSelect()  %s\n", accessImp->Print().c_str());
    AccessState state;
    {
        cor::ObjectLocker ol(mMutex);
        {
            if (!Query(accessImp, state))
            {
                accessImp->Print();
                throw cor::Exception("AccessTable::AbortSelect() -- Could not find access '%s'", accessImp->Print().c_str());
            }
        }
        if (!state->IsInSelect())
        {
            return false;
        }
    }

    state->Abort();

    return true;
}

AccessState
AccessTable::RemoveAccessFromMaps(const muuid::uuid accessHandle)
{
    DEBUG_LOCAL("AccessTable::RemoveAccessFromMaps%s\n", "");
    // mutex should already be held
    AccessStateMap::iterator i;

    AccessState r;
    i = mReads.find(accessHandle);
    if (i != mReads.end())
    {
        r = i->second;
        mReads.erase(i);
        return r;
    }

    i = mWrites.find(accessHandle);
    if (i != mWrites.end())
    {
        r = i->second;
        mWrites.erase(i);
        return r;
    }

    i = mSelects.find(accessHandle);
    if (i != mSelects.end())
    {
        r = i->second;
        mSelects.erase(i);
        return r;
    }

    throw cor::Exception("AccessTable::RemoveAccessFromMaps() -- access does not exist");
}

// attempt to advance the next pending access
void
AccessTable::ConsumePending()
{
    cor::ObjectLocker ol(mMutex);

    DEBUG_LOCAL("ConsumePending() table %s\n", mName.c_str());
    DEBUG_LOCAL("   Pending queue size %ld\n", mPendingQueue.size());

    std::list<AccessState>::iterator ali = mPendingQueue.begin();
    for (; ali != mPendingQueue.end();)
    {
        AccessState next = *ali;

        /*AccessState state;
        //printf("Looking up access %s\n", next->GetHandle().string().c_str());
        if (next->IsWrite())
        {
            state = mWrites.at(next->GetHandle());
        }
        else
        {
            state = mReads.at(next->GetHandle());
        }*/

        DEBUG_LOCAL("   top item: %s\n", next->Print().c_str());

        // if write, see if it overlaps an ongoing access
        if (next->IsWrite())
        {
            if (mAccessPolicy == eWriteMutex)
            {
                if (IntersectsActive(next->mSnappedTimeRange, mWrites))
                {
                    ali++;
                    continue;
                }
            }
            else // read-write
            {
                if (IntersectsActive(next->mSnappedTimeRange, mReads))
                {
                    ali++;
                    continue;
                }
            }
        }

        if (mAccessPolicy == eReadWrite)
        {
            // all accesses must be exclusive of ongoing writes
            if (IntersectsActive(next->mSnappedTimeRange, mWrites))
            {
                ali++;
                continue;
            }

            // test for write exclusion for all newer items than it
            if (IntersectsNewer(ali))
            {
                ali++;
                continue;
            }
        }

        DEBUG_LOCAL("   Activating %s\n", next->Print().c_str());
        DEBUG_LOCAL("   which has %ld written columns\n", next->mWrittenColumnMap.size());
        next->SetState(AccessStateImp::eActive);
        ali = mPendingQueue.erase(ali);
        next->Signal();

    }
}

void
AccessTable::ConsumeSelected(const WrittenColumnsMap& wcm, const cor::TimeRange& extents)
{
    DEBUG_LOCAL("AccessTable::ConsumeSelected%s\n", "");
    cor::ObjectLocker ol(mMutex);

    DEBUG_LOCAL("ConsumeSelected() table %s\n", mName.c_str());
    DEBUG_LOCAL("   Number of accesses in Select state: %ld\n", mSelects.size());
    DEBUG_LOCAL("   %ld time ranges written\n", wcm.size());
    DEBUG_LOCAL("   extents: %s\n", extents.Print().c_str());

    // iterate all accesses in select queue
    AccessStateMap::iterator ami = mSelects.begin();
    for (; ami != mSelects.end();)
    {
        AccessState item = ami->second;

        // this is an optimization to avoid more lengthy checks if the
        // Access that is Selecting() does not even overlap in time with
        // anything in the WrittenColumnsMap
        if (!item->mTimeRange.Intersects(extents))
        {
            //printf("no intersection between %s and %s\n", item->mTimeRange.Print().c_str(), extents.Print().c_str());
            ami++;
            continue;
        }

        // iterate columns written
        WrittenColumnsMap::const_iterator j = wcm.begin();
        item->mWrittenColumnMap.clear();
        for (; j != wcm.end(); j++)
        {
            if (item->mSelectColumns.empty() ||
               (item->mSelectColumns.find(j->first) != item->mSelectColumns.end()))
            {
                cor::TimeRange ctr = cor::TimeRange::SuperUnion(j->second);
                if (item->mTimeRange.Intersects(ctr))
                {
                    item->mWrittenColumnMap[j->first] = j->second;
                }
            }
        }

        if (!item->mWrittenColumnMap.empty())
        {
            DEBUG_LOCAL("   Selecting %s\n", item->Print().c_str());
            // careful with the order here...make sure new reference is made
            // before removing the old one, so that smart pointer doesn't
            // delete the underlying object
            mPendingQueue.push_back(item);

            // transfer from Select collection back to Read or Write
            if (ami->second->IsWrite())
                mWrites[ami->first] = ami->second;
            else
                mReads[ami->first] = ami->second;

            AccessStateMap::iterator amix = ami;
            ami++;
            mSelects.erase(amix);
        }
        else
        {
            //printf("   Not selecting %s\n", item->Print().c_str());
            ami++;
        }
    }

    // update all pending selects
    std::list<AccessState>::const_iterator pli = mPendingQueue.begin();
    for (; pli != mPendingQueue.end(); pli++)
    {
        (*pli)->mWrittenColumnMap.Merge(wcm, (*pli)->mTimeRange, (*pli)->mSelectColumns);
    }
}

void
AccessTable::Print(std::ostream& os)
{
    cor::ObjectLocker ol(mMutex);

    os << "Access table " << mName << std::endl;

    os << "Writes:" << std::endl;
    if (mWrites.empty())
        os << "   <none>" << std::endl;
    AccessStateMap::const_iterator amci = mWrites.begin();
    for (; amci != mWrites.end(); amci++)
    {
        os << amci->second->Print() << std::endl;
    }

    os << "Reads:" << std::endl;
    if (mReads.empty())
        os << "   <none>" << std::endl;
    amci = mReads.begin();
    for (; amci != mReads.end(); amci++)
    {
        os << amci->second->Print() << std::endl;
    }

    os << "Pending:" << std::endl;
    if (mPendingQueue.empty())
        os << "   <none>" << std::endl;
    std::list<AccessState>::const_iterator alci = mPendingQueue.begin();
    for (; alci != mPendingQueue.end(); alci++)
    {
        os << "   " << (*alci)->Print() << std::endl;
    }
}

bool
AccessTable::Query(AccessImp* access, AccessState& state)
{
    DEBUG_LOCAL("AccessTable::Query%s\n", "");
    // mutex should already be held

    // check select first, as one of IsRead() or IsWrite() will return true
    bool r = Find(mSelects, access->GetHandle(), state);
    if (r)
    {
        return r;
    }
    else if (access->IsRead())
    {
        return Find(mReads, access->GetHandle(), state);
    }
    else if (access->IsWrite())
    {
        return Find(mWrites, access->GetHandle(), state);
    }
    else
    {
        // DEFENSIVE
        throw cor::Exception("AccessTable::Query() -- Unspecified mode in access");
    }
}

bool
AccessTable::IntersectsActive(const cor::TimeRange& tr, const AccessStateMap& set)
{
    AccessStateMap::const_iterator amci = set.begin();
    for (; amci != set.end(); amci++)
    {
        if (amci->second->IsActive())
        {
            if (amci->second->mSnappedTimeRange.Intersects(tr))
                return true;
        }
    }
    return false;
}

bool
AccessTable::IntersectsAny(const cor::TimeRange& tr, const AccessStateMap& set)
{
    AccessStateMap::const_iterator amci = set.begin();
    for (; amci != set.end(); amci++)
    {
        if (amci->second->mSnappedTimeRange.Intersects(tr))
            return true;
    }
    return false;
}

bool
AccessTable::IntersectsNewer(std::list<AccessState>::iterator& iter)
{
    std::list<AccessState>::const_iterator cli = mPendingQueue.begin();
    for (; cli != iter; cli++)
    {
        if ((*iter)->IsWrite() || (*cli)->IsWrite())
        {
            if ((*cli)->mSnappedTimeRange.Intersects((*iter)->mSnappedTimeRange))
            {
                return true;
            }
        }
    }
    return false;
}

void
AccessTable::AddPending(AccessImp* access)
{
    AccessState newState(new AccessStateImp(AccessStateImp::ePending,
                                            access->GetHandle(),
                                            access->mType,
                                            access->mSnapped,
                                            access->mTimeRange));
    if (access->IsRead())
    {
        mReads[access->GetHandle()] = newState;
    }
    else
    {
        mWrites[access->GetHandle()] = newState;
    }
    mPendingQueue.push_back(newState);
}

void
AccessTable::ClearPending(AccessState as)
{
    std::list<AccessState>::iterator i = mPendingQueue.begin();
    for (;i != mPendingQueue.end(); i++)
    {
        if (as == *i)
        {
            mPendingQueue.erase(i);
            break;
        }
    }
}

void
AccessTable::AddActive(AccessImp* access)
{
    AccessState newState(new AccessStateImp(AccessStateImp::eActive,
                                            access->GetHandle(),
                                            access->mType,
                                            access->mSnapped,
                                            access->mTimeRange));
    if (access->IsRead())
    {
        mReads[access->GetHandle()] = newState;
    }
    else
    {
        mWrites[access->GetHandle()] = newState;
    }
}

bool
AccessTable::Find(AccessStateMap& set, const muuid::uuid& handle, AccessState& state)
{
    AccessStateMap::iterator i = set.find(handle);
    if (i != set.end())
    {
        state = i->second;
        return true;
    }
    return false;
}

void
AccessTable::Validate() const
{
    cor::ObjectLocker ol(mMutex);

    AccessStateMap::const_iterator i = mWrites.begin();
    for (; i != mWrites.end(); i++)
    {
        if (!i->second->IsActive())
            continue;

        AccessStateMap::const_iterator j = i;
        j++;
        for (; j != mWrites.end(); j++)
        {
            if (!j->second->IsActive())
                continue;

            if (j->second->mSnappedTimeRange.Intersects(i->second->mSnappedTimeRange))
                throw cor::Exception("Write %s and %s intersect",
                                     i->second->Print().c_str(),
                                     j->second->Print().c_str());
        }

        j = mReads.begin();
        for (; j != mReads.end(); j++)
        {
            if (!j->second->IsActive())
                continue;

            if (j->second->mSnappedTimeRange.Intersects(i->second->mSnappedTimeRange))
                throw cor::Exception("Write %s and read %s intersect",
                                     i->second->Print().c_str(),
                                     j->second->Print().c_str());
        }
    }
}


}
