//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_ACCESS_TABLE__
#define __PKG_NABU_ACCESS_TABLE__


#include <map>
#include <memory>
#include <string>
#include <list>

#include <mcor/mmutex.h>
#include <mcor/timerange.h>
#include <mcor/muuid.h>

#include "access_imp.h"
#include "time_scheme.h"

namespace nabu
{

class DatabaseImp;
class IOOperationImp;

class AccessStateImp;
typedef std::shared_ptr<AccessStateImp> AccessState;
typedef std::map<muuid::uuid, AccessState> AccessStateMap;


/** class AccessTable : manages accesses, creating a read/write policy
 *     by enqueing accesses when they are in conflict with an already
 *     active Access, then activating them when the conflict is
 *     removed.
 *
 */
class AccessTable
{
public:
    AccessTable(const std::string& name, AccessPolicy policy = eReadWrite);
    ~AccessTable();

    const std::string& GetName() const { return mName; }

    // add the given access
    void AddAccess(AccessImp* access);

    // remove the given access (because, for instance, it is done and
    // no longer needed). This is invoked only by Access objects.
    void RemoveAccess(AccessImp* access);

    // waits up to waitSeconds seconds for access to be ready, and returns
    // true if it is ready or false if it timed out; if waitSeconds is negative,
    // waits indefinitely
    bool Ready(AccessImp* access, unsigned int waitSeconds);

    // waits up to waitSeconds seconds for access to be selected, and returns
    // whether it is ready, timed out, or aborted; if msecWait is negative,
    // waits indefinitely
    // If eComplete is returned, the access has been obtained and subsequent
    // operations (Reads and Writes, etc.) can be performed;
    // Select must be called until eComplete, or the
    // Access abandoned.
    SelectResult Select(AccessImp* access,
                const std::set<MetaKey>& selectColumns,
                WrittenColumnsMap& writtenColumnsMap,
                unsigned int waitSeconds);
    // aborts any Select; returns true if a Select was aborted, false otherwise
    bool AbortSelect(AccessImp* access);

    void Print(std::ostream& os);

    void Validate() const;

protected:
    const std::string mName;
    const AccessPolicy mAccessPolicy;

    mutable cor::MMutex mMutex;
    AccessStateMap mReads;
    AccessStateMap mWrites;
    AccessStateMap mSelects;

    // removes access from read, write, or select container and returns it;
    // throws if not found
    // mMutex should be held during this call
    AccessState RemoveAccessFromMaps(const muuid::uuid accessHandle);

    // this container is non-owning and keeps track of order only
    std::list<AccessState> mPendingQueue;

    // Advance all pending accesses possible
    void ConsumePending();

    // Put all selected accesses that have intersection with the given
    // timeranges into pending. The extents of the wcm are provided so
    // that non-intersecting accesses can void the check.
    void ConsumeSelected(const WrittenColumnsMap& wcm, const cor::TimeRange& extents);

    // query the access; returns true if access was found and state is
    // valid, false otherwise
    bool Query(AccessImp* access, AccessState& state);

    static bool IntersectsActive(const cor::TimeRange& tr, const AccessStateMap& set);
    static bool IntersectsAny(const cor::TimeRange& tr, const AccessStateMap& set);
    bool IntersectsNewer(std::list<AccessState>::iterator& item);

    void AddPending(AccessImp* access);
    // warning: linear time operation to remove pending but aborted IO
    void ClearPending(AccessState access);
    void AddActive(AccessImp* access);

    // finds access and sets state if found.
    // Returns true if found, false otherwise.
    static bool Find(AccessStateMap& set, const muuid::uuid& handle, AccessState& state);

private:
    // prohibit
    AccessTable();
    AccessTable(AccessTable& other);
};




}

#endif
