//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include "mru_cache.h"



namespace cor {

template <class Key, class Value>
MRUCache<Key, Value>::MRUCache() :
    mLimit(1000),
    mMutex("MRUCache"),
    mDisable(0)
{}

template <class Key, class Value>
MRUCache<Key, Value>::~MRUCache<Key, Value>()
{}


template <class Key, class Value>
void
MRUCache<Key, Value>::SetLimit(size_t limit)
{
    mLimit = limit;
}


template <class Key, class Value>
void
MRUCache<Key, Value>::Lock(const Key& key)
{
    cor::ObjectLocker ol(mMutex);
    //mLocked.insert(key);
    typename Map::iterator oi = mMap.find(key);
    if (oi != mMap.end())
    {
        oi->second->mLocked = true;
    }
}

template <class Key, class Value>
void
MRUCache<Key, Value>::Unlock(const Key& key)
{
    cor::ObjectLocker ol(mMutex);
    typename Map::iterator oi = mMap.find(key);
    if (oi != mMap.end())
    {
        oi->first->mLocked = false;
    }
}

template <class Key, class Value>
void
MRUCache<Key, Value>::Disable()
{
    cor::ObjectLocker ol(mMutex);
    mDisable++;
}

template <class Key, class Value>
void
MRUCache<Key, Value>::Enable()
{
    cor::ObjectLocker ol(mMutex);
    if (mDisable == 0) // DEFENSIVE
        throw cor::Exception("Cache::Enable() -- Disable count is 0 already\n");
    mDisable--;

    // if transitioning to enabled, go ahead and cull the cache
    if (mDisable == 0)
        CullOldest();
}

template <class Key, class Value>
bool
MRUCache<Key, Value>::Enabled() const
{
    cor::ObjectLocker ol(mMutex);
    return mDisable == 0;
}

template <class Key, class Value>
bool
MRUCache<Key, Value>::Find(const Key& key, Value& value)
{
    cor::ObjectLocker ol(mMutex);

    typename Map::iterator oi = mMap.find(key);
    if (oi == mMap.end())
    {
        return false;
    }

    typename MRUCache<Key, Value>::ListEntry listEntry = *(oi->second);
    value = listEntry.mValue;
    return true;
}

template <class Key, class Value>
size_t
MRUCache<Key, Value>::Size() const
{
    cor::ObjectLocker ol(mMutex);

    if (mMap.size() != mList.size())
        throw cor::Exception("Cache::Size() -- sanity check failed, mMap.size = %ld, mList.size == %ld\n", mMap.size(), mList.size());

    return mMap.size();
}

template <class Key, class Value>
void
MRUCache<Key, Value>::Put(const Key& key, const Value& value)
{
    //printf("==== PUT\n");
    cor::ObjectLocker ol(mMutex);

    typename Map::iterator mi = mMap.find(key);
    if (mi == mMap.end())
    {
        //printf("   CullOldest\n");
        // miss; removes oldest item if at maximum size, otherwise
        // does nothing
        CullOldest();
    }
    else
    {
        //printf("   Already in table, re-ordering\n");
        // already in table; reorder to most recent
        typename List::iterator li = mMap[key];
        mList.erase(li);
    }

    //printf("   Inserting\n");
    mList.push_back(ListEntry(key, value));
    typename List::iterator i = mList.end();
    mMap[key] = --i;
}

template <class Key, class Value>
void
MRUCache<Key, Value>::CullOldest()
{
    while (Enabled() && mMap.size() >= mLimit)
    {
        // find oldest from List
        typename List::iterator oI = mList.begin();
        //TableAddress ga = (*oI)->GetTableAddress();
        //Key key = (*oI)->GetKey();
        while (oI->mLocked)
        {
            oI++; // item is locked, skip it

            // DEFENSIVE
            if (oI == mList.end())
                throw cor::Exception("All items locked in cache but limit is exceeded (need a bigger cache)");

            //key = (*oI)->GetKey();
        }


        Key oldestKey = oI->mKey;
        mList.erase(oI);

        // DEFENSIVE
        if (mMap.erase(oldestKey) != 1)
        {
            throw cor::Exception("Cache::Put() -- erase of oldest element from List not found in Map");
        }
    }
}
    
} // end namespace



