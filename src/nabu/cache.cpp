//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include "cache.h"
#include "database_imp.h"
#include "exception.h"
#include "metadata_table.h"
#include "profiler.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {

Cache::Cache() :
    mLimit(1000),
    mMutex("Cache"),
    mDisable(0)
{}

Cache::~Cache()
{}

Cache&
Cache::Get()
{
    static Cache* instance = 0;
    if (instance == 0)
    {
        instance = new Cache;
    }
    return *instance;
}

void
Cache::SetLimit(size_t limit)
{
    mLimit = limit;
}

size_t
Cache::GetLimit() const
{
    return mLimit;
}


void
Cache::Lock(const TableAddress& TableAddress)
{
    cor::ObjectLocker ol(mMutex, "Cache::Lock");
    //printf("Locking %s\n", TableAddress.GetLiteral().c_str());
    mLocked.insert(TableAddress);
}

void
Cache::Unlock(const TableAddress& TableAddress)
{
    cor::ObjectLocker ol(mMutex, "Cache::Unlock");
    //printf("Unlocking %s\n", TableAddress.GetLiteral().c_str());
    mLocked.erase(TableAddress);
}

void
Cache::Disable()
{
    cor::ObjectLocker ol(mMutex, "Cache::Disable");
    mDisable++;
}

void
Cache::Enable()
{
    cor::ObjectLocker ol(mMutex, "Cache::Enable");
    if (mDisable == 0) // DEFENSIVE
        throw cor::Exception("Cache::Enable() -- Disable count is 0 already\n");
    mDisable--;

    // if transitioning to enabled, go ahead and cull the cache
    if (mDisable == 0)
        CullOldest();
}

bool
Cache::Enabled() const
{
    cor::ObjectLocker ol(mMutex, "Cache::Enabled");
    return mDisable == 0;
}

MetaDataTable
Cache::Find(const Database& database, const TableAddress& ta)
{
    return FindImp(database, ta, true);
}

MetaDataTable
Cache::At(const DatabaseImp& database, const TableAddress& ta)
{
    return FindImp(database, ta, false);
}

void
Cache::Print() const
{
    cor::ObjectLocker ol(mMutex, "Cache::Print");

    printf("Table (size %ld):\n", mMap.size());
    Map::const_iterator i = mMap.begin();
    for (; i != mMap.end(); i++)
    {
        printf("%s\n", i->first.GetLiteral().c_str());
        List::iterator li = i->second;
        printf("   %s\n", (*li)->GetLiteral().c_str());
    }
    printf("List (size %ld):\n", mList.size());
    List::const_iterator li = mList.begin();
    for (; li != mList.end(); li++)
    {
        printf("%s\n", (*li)->GetLiteral().c_str());
    }
}

size_t
Cache::Size() const
{
    cor::ObjectLocker ol(mMutex, "Cache::Size");

    if (mMap.size() != mList.size())
        throw FatalException(cor::Url(), // no specific database
                             "Cache::Size() -- sanity check failed, mMap.size = %ld, mList.size == %ld\n", mMap.size(), mList.size());

    return mMap.size();
}

void
Cache::PrintLock() const
{
    cor::ObjectLocker ol(mMutex, "Cache::PrintLock");

    std::cout << "Locked tables:" << std::endl;
    std::set<TableAddress>::const_iterator i = mLocked.begin();
    for (; i != mLocked.end(); i++)
    {
        std::cout << i->GetLiteral() << std::endl;
    }
}

void
Cache::Put(MetaDataTable table)
{
    DEBUG_LOCAL("Cache::Put (%s)\n", table->GetTableAddress().GetLiteral().c_str());
    cor::ObjectLocker ol(mMutex, "Cache::Put");

    Map::iterator mi = mMap.find(table->GetTableAddress());
    if (mi == mMap.end())
    {
        CullOldest();
    }
    else
    {
        // already in table; reorder to most recent
        List::iterator li = mMap[table->GetTableAddress()];
        mList.erase(li);
    }

    mList.push_back(table);
    List::iterator i = mList.end();
    mMap[table->GetTableAddress()] = --i;

    // SANITY
    if ((*mMap[table->GetTableAddress()])->GetTableAddress() != table->GetTableAddress())
        throw FatalException(cor::Url(), // no specific database
                             "Sanity failure in cache Put()");
}

void
Cache::Remove(std::shared_ptr<MetaDataTableImp> table)
{
    DEBUG_LOCAL("Cache::Remove (%s)\n", table->GetTableAddress().GetLiteral().c_str());
    cor::ObjectLocker ol(mMutex, "Cache::Remove");

    Map::iterator mi = mMap.find(table->GetTableAddress());
    if (mi == mMap.end())
    {
        // miss
        return;
    }
    else
    {
        List::iterator li = mi->second;
        mMap.erase(mi);
        mList.erase(li);
    }
}

void
Cache::CullOldest()
{
    while (Enabled() && mMap.size() >= mLimit)
    {
        // find oldest from List
        List::iterator oI = mList.begin();
        TableAddress ta = (*oI)->GetTableAddress();
        while (mLocked.find(ta) != mLocked.end())
        {
            oI++; // item is locked, skip it

            // DEFENSIVE
            if (oI == mList.end())
                throw FatalException(cor::Url(), // no specific database
                                     "All items locked in cache but limit is exceeded (need a bigger cache)");

            ta = (*oI)->GetTableAddress();
        }

        MetaDataTable oldest = *oI;
        // SANITY -- exception will be thrown below, but Print() would fault, so do it here
        if (mMap.find(oldest->GetTableAddress()) == mMap.end())
        {
            printf("\n##### Cache coherency error ####\n\n");
            printf("Cache dump:\n");
            Print();
        }
        mList.erase(oI);

        GetProfiler().Count("Unload Table");
        DEBUG_LOCAL("Unloaded %s\n", oldest->GetTableAddress().GetLiteral().c_str());

        // DEFENSIVE
        if (mMap.erase(oldest->GetTableAddress()) != 1)
        {
            throw FatalException(cor::Url(), // no specific database
                                 "Cache::Put() -- erase of oldest element '%s' from List not found in Map",
                                 oldest->GetTableAddress().GetLiteral().c_str());
        }
    }
}

MetaDataTable
Cache::FindImp(const Database& database, const TableAddress& ta, bool enableCreate)
{
    DEBUG_LOCAL("Cache::Find (%s)\n", ta.GetLiteral().c_str());
    cor::ObjectLocker ol(mMutex, "Cache::FindImp");

    Map::iterator oi = mMap.find(ta);
    if (oi == mMap.end())
    {
        DEBUG_LOCAL("Cache miss%s\n","");

        //return MetaDataTable(NULL);
        UtcTimeScheme ts = database.GetPeriods();
        size_t layer = ts.LayerOf(ta);
        MetaDataTable table =
                MetaDataTable(new MetaDataTableImp(ts, ta, layer));
        //printf("Cache miss for %s\n", ta.GetLiteral().c_str());
        if (!table->Load(database) && !enableCreate)
        {
            return MetaDataTable();
        }
        //printf("Loaded %s\n", sub->GetLiteral().c_str());
        //sub->PrintAll(std::cout);
        Put(table);
        return table;
    }
    else
    {
        DEBUG_LOCAL("Cache hit for %s: %s\n",
                    ta.GetLiteral().c_str(),
                    oi->first.GetLiteral().c_str());
    }

    return *(oi->second);
}
    
} // end namespace



