//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_UNCACHER__
#define __PKG_NABU_UNCACHER__

#include <list>
#include <map>
#include <memory>
#include <string>
#include <set>

#include <mcor/mcondition.h>
#include <mcor/timerange.h>

#include "table_address.h"
#include "metadata_table.h"

namespace nabu
{

class MetaDataTableImp;

/** class Cache : keeps a cache of most-recently used MetaData tables
 *
 *  This has nothing to do with Data. The reason this exists is to limit
 *  the number of metadata tables in memory, which would otherwise grow
 *  unbounded with operation time.
 *
 */

class Cache
{
public:
    static Cache& Get(); // get the one-and-only instance

    // sets the size of the cache -- the number of metadata tables allowed
    // to exist at one time (normally).
    // Not threadsafe, set before using this object.
    void SetLimit(size_t limit);
    size_t GetLimit() const;

    // find the MetaDataTable at the given address; if it does not exist, the
    // shared pointer will be NULL
    std::shared_ptr<MetaDataTableImp> At(const DatabaseImp& database, const TableAddress& ga);
    // find the MetaDataTable at the given address; if it does not exist, a
    // new empty table will be returned
    std::shared_ptr<MetaDataTableImp> Find(const Database& database, const TableAddress& ga);

    // update the given table to be most recently used
    void Put(std::shared_ptr<MetaDataTableImp> table);

    // remove the table from the cache
    // this does nothing to persistence of the table, it just flushes it from here
    void Remove(std::shared_ptr<MetaDataTableImp> table);

    // sets the table at global address as locked
    void Lock(const TableAddress& TableAddress);
    void Unlock(const TableAddress& TableAddress);

    // disable the culling of the oldest entries from the cache
    void Disable();
    void Enable();
    bool Enabled() const;

    // print the cache contents
    void Print() const;

    // return the cache size
    size_t Size() const;

    // print just the locked addresses
    void PrintLock() const;

private:
    Cache();
    ~Cache();

    void CullOldest();

    std::shared_ptr<MetaDataTableImp> FindImp(const Database& database, const TableAddress& ga, bool enableCreate);
    
    size_t mLimit;

    mutable cor::MMutex mMutex;
    unsigned int mDisable;

    typedef std::list<std::shared_ptr<MetaDataTableImp>> List;
    List mList;
    typedef std::map<TableAddress, List::iterator > Map;
    Map mMap;

    std::set<TableAddress> mLocked;
};


/** class CacheLocker
 *
 */

class CacheLocker
{
public:
    CacheLocker(const MetaDataTableImp* who) : mWho(who)
    {
        Cache::Get().Lock(mWho->GetTableAddress());
    }

    virtual ~CacheLocker()
    {
        Cache::Get().Unlock(mWho->GetTableAddress());
    }
private:
    const MetaDataTableImp* mWho;
};


/** class CacheDisabler
 *
 */

class CacheDisabler
{
public:
    CacheDisabler()
    {
        Cache::Get().Disable();
    }

    virtual ~CacheDisabler()
    {
        Cache::Get().Enable();
    }
private:
    MetaDataTableImp* mWho;
};
}

#endif
