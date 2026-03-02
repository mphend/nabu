//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef PKG_MCOR_MRU_CACHE_INCLUDED
#define PKG_MCOR_MRU_CACHE_INCLUDED

#include <list>
#include <map>
#include <set>

#include <mcor/mcondition.h>

namespace cor
{

/** template MRUCache : keeps a cache of most-recently used objects
 *
 */

template <class Key, class Value>
class MRUCache
{
public:
    MRUCache();
    virtual ~MRUCache();

     // sets the size of the cache -- the number of entries allowed
    // to exist at one time (normally)
    void SetLimit(size_t limit);

    // find the value at the given address; return if value was found
    virtual bool Find(const Key& k, Value& v);

    // update the value at key 'k' to 'v' and become most recently used
    virtual void Put(const Key& k, const Value& v);

    // sets the entry at key as locked
    void Lock(const Key& key);
    void Unlock(const Key& key);

    // disable the cache
    void Disable();
    void Enable();
    bool Enabled() const;

    // return the cache size
    size_t Size() const;

    struct ListEntry
    {
        ListEntry(const Key& key, Value v) : mKey(key), mValue(v), mLocked(false)
        {}
        Key mKey; // this is needed for reverse look up when an item ages out
        Value mValue;
        bool mLocked;
    };

    typedef typename std::list<ListEntry>::const_iterator ConstIterator;

    ConstIterator Begin() const { return mList.begin(); }
    ConstIterator End() const { return mList.end(); }

private:


    virtual void CullOldest();
    
    size_t mLimit;

    mutable cor::MMutex mMutex;
    unsigned int mDisable;

    // list that owns object and keeps track of age; oldest is at front,
    // newest at back
    typedef std::list<ListEntry> List;
    List mList;

    // map for quick lookup of item
    typedef std::map<Key, typename List::iterator > Map;
    Map mMap;
};

}

// template implementation
#include "mru_cache.cpp"

namespace cor {

/** class CacheLocker
 *
 */

template <class Key, class Value>
class CacheLocker
{
public:
    CacheLocker(MRUCache<Key,Value>& cache, Key* who) : mCache(cache), mWho(who)
    {
        mCache.Lock(who);
    }

    virtual ~CacheLocker()
    {
        mCache.Unlock(mWho);
    }
private:
    MRUCache<Key,Value>& mCache;
    Key mWho;
};


/** class CacheDisabler
 *
 */
template <class Key, class Value>
class CacheDisabler
{
public:
    CacheDisabler(MRUCache<Key,Value>& cache) : mCache(cache)
    {
        mCache.Disable();
    }

    virtual ~CacheDisabler()
    {
        mCache.Enable();
    }
private:
    MRUCache<Key,Value>& mCache;
};


}

#endif
