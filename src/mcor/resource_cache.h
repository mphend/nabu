//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#ifndef GICM_RESOURCE_CACHE_H
#define GICM_RESOURCE_CACHE_H

#include <list>
#include <map>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include <mcor/mcondition.h>
#include <mcor/mmutex.h>
#include <mcor/mtimer.h>
#include <mcor/mthread.h>


namespace cor {

// this is the item held by the cache to own and keep track of the resource
template <class Resource>
class ItemImp;


/** ResourceAccessor : accessor to the underlying resource, held in the cache
 *
 */
template <class Resource>
class ResourceAccessor
{
public:
    ResourceAccessor() {}
    ResourceAccessor(const ResourceAccessor& other);
    ResourceAccessor(std::shared_ptr<ItemImp<Resource>> value);
    ~ResourceAccessor();

    void operator=(const ResourceAccessor& other);
    operator bool() const { return (bool)mValue; }

    // pointer-like access the resource
    Resource operator*() { return (*mValue).mResource; }
    const Resource operator*() const { return (*mValue).mResource; }
    Resource operator->() { return (*mValue).mResource; }
    const Resource operator->() const { return (*mValue).mResource; }

private:
    std::shared_ptr<ItemImp<Resource>> mValue;
};


/** class CacheInterface : an interface to a subset of cache actions
 *
 */
class CacheInterface
{
public:
    // resets the age of the item; throws if it does not exist
    virtual void Reset(const std::string& key) = 0;

    // remove multiple items, ignoring those that do not exist
    virtual void Remove(const std::set<std::string>& keys) = 0;
};


/** class ResourceCache : a cache of items with a thread that deletes any
 *  that have not been accessed for a specified number of seconds.
 *
 *  Items can have a parent item in a different cache; access of the child
 *  causes a reset of the parent (as though it were accessed). Access of
 *  children can thereby keep parents alive.
 *
 */
template <class Resource>
class ResourceCache : public cor::Thread, public CacheInterface
{
public:
    ResourceCache(const std::string& name, unsigned int maxAgeSeconds);
    ~ResourceCache();

    void SetMaxAgeSeconds(unsigned int seconds);

    // return a resource already in the cache; will throw if it does not exist
    // this action resets the age of the item
    ResourceAccessor<Resource> Get(const std::string& key);

    // insert a new item into the cache; will throw if the item already exists
    void Put(const std::string& key,
             Resource value,
             const std::string& parentKey = "",
             CacheInterface* parentCache = NULL,
             CacheInterface* childCache = NULL);

    // gets all the items
    void GetAll(std::vector<ResourceAccessor<Resource>>& resources);

    // resets the age of the item; throws if it does not exist
    void Reset(const std::string& key) override;

    // remove an item; throws if it does not exist
    void Remove(const std::string& key);

    // remove multiple items, ignoring those that do not exist
    void Remove(const std::set<std::string>& keys) override;

    // print cache to standard out, for debugging
    void Print();

protected:
    const std::string mName;
    // if there is a child and parent cache (as with Nabu's IOOperation and Access,
    // respectively), then the lock order policy is parent first, child second.
    mutable cor::MMutex mMutex;
    unsigned int mMaxAgeSeconds;

    // The list is the owner, and keeps the objects in age order
    // A map allows quick access to the item in the list
    //
    // The "too old" policy is executed based on a time kept in each
    // item, but the age ordering of the list allows the culling
    // of aged-out items to be efficient (the entire list does not
    // need to be searched each time through, only the items that
    // are oldest and will be removed)

    // back of list is most recent, front is oldest
    typedef std::list<std::shared_ptr<ItemImp<Resource>>> List;
    List mList;
    typedef std::map<std::string, typename List::iterator > Map;
    Map mMap;

    void PutItem(const std::string& key, std::shared_ptr<ItemImp<Resource>> value);

    void ThreadFunction();

};



// ============================================================================
// template implementation

template<class Resource>
class ItemImp
{
public:
    ItemImp(const std::string& key,
            Resource r,
            const std::string& parentKey,
            CacheInterface* parentCache,
            CacheInterface* childCache) :
            mMutex("ItemImp"),
            mBusyCount(0),
            mTimer(1), // this value is never used, but must be positive
            mKey(key),
            mResource(r),
            mParentKey(parentKey),
            mParentCache(parentCache),
            mChildCache(childCache)
    {}

    ~ItemImp()
    {
        if (mChildCache != NULL)
        {
            //printf("Deleting child items from child cache\n");
            std::set<std::string> childKeys;
            mResource->GetChildKeys(childKeys);

            /*printf("%ld children\n", childKeys.size());
            std::set<std::string>::const_iterator i = childKeys.begin();
            for (; i != childKeys.end(); i++)
                printf("   %s\n", i->c_str());*/

            mChildCache->Remove(childKeys);
        }
    }


    // this mutex synchronizes access the following data members which
    // can be accessed by:
    // 1) the thread that owns this; the URL handler
    // 2) the ResourceCache thread, which deletes stalled IO accesses
    //
    // This object exists as a shared pointer, with references in the ResourceCache and
    // the URL handler. Membership in the ResourceCache is synchronized by its mutex.
    //
    // The ResourceCache will not drop its reference while the URL handler
    // is running (mBusyCount is nonzero). This is so that when the handler
    // is done, it will not run into a case where it tries to invoke
    // an action on the ResourceCache for an IOP that has already been forgotten.
    //
    mutable cor::MMutex mMutex;
    int mBusyCount;
    cor::Timer mTimer;

    std::string mKey;
    Resource mResource;

    std::string mParentKey;
    CacheInterface* mParentCache;

    CacheInterface* mChildCache;
};


template <class Resource>
ResourceAccessor<Resource>::ResourceAccessor(const ResourceAccessor<Resource>& other)
{
    mValue = other.mValue;

    if (!mValue)
        return;

    cor::ObjectLocker ol(mValue->mMutex);
    mValue->mBusyCount++;
}

template <class Resource>
ResourceAccessor<Resource>::ResourceAccessor(std::shared_ptr<ItemImp<Resource>> value) : mValue(value)
{
    if (!mValue)
        return;

    cor::ObjectLocker ol(mValue->mMutex);
    mValue->mBusyCount++;
}

template <class Resource>
ResourceAccessor<Resource>::~ResourceAccessor()
{
    if (!mValue)
        return;

    cor::ObjectLocker ol(mValue->mMutex);
    mValue->mBusyCount--;

    // SANITY
    if (mValue->mBusyCount < 0)
        printf("~ResourceAccessor -- negative mBusyCount!\n");
}

template <class Resource>
void
ResourceAccessor<Resource>::operator=(const ResourceAccessor<Resource>& other)
{
    if (mValue) // let go of old
    {
        cor::ObjectLocker ol(mValue->mMutex);
        mValue->mBusyCount--;
    }

    mValue = other.mValue;

    if (!mValue)
        return;

    cor::ObjectLocker ol(mValue->mMutex);
    mValue->mBusyCount++;
}

template <class Resource>
ResourceCache<Resource>::ResourceCache(const std::string& name, unsigned int maxAgeSeconds) :
        cor::Thread(name + " ResourceCache"),
        mName(name),
        mMutex("ResourceCache"),
        mMaxAgeSeconds(maxAgeSeconds)
{
}

template <class Resource>
ResourceCache<Resource>::~ResourceCache()
{}

template <class Resource>
void
ResourceCache<Resource>::SetMaxAgeSeconds(unsigned int seconds)
{
    cor::ObjectLocker ol(mMutex);
    mMaxAgeSeconds = seconds;
}

template <class Resource>
ResourceAccessor<Resource>
ResourceCache<Resource>::Get(const std::string& key)
{
    ResourceAccessor<Resource> r;
    std::shared_ptr<ItemImp<Resource>> item;
    {
        cor::ObjectLocker ol(mMutex);

        typename Map::iterator oi = mMap.find(key);
        if (oi == mMap.end())
        {
            throw cor::Exception("ResourceCache() %s: No item '%s' in cache",
                                 mName.c_str(),
                                 key.c_str());
        }
        Reset(key);

        r = ResourceAccessor<Resource>(*(oi->second));
        item = *(oi->second);
    }
    // must release mMutex here in case the parent cache's mutex is about
    // to be obtained; otherwise, this could violate the lock ordering
    // policy (parent first, child second)

    // if this item is linked to a parent resource, reset the time on that
    // as well
    if (item->mParentCache != NULL)
    {
        item->mParentCache->Reset(item->mParentKey);
    }

    return r;
}

template <class Resource>
void
ResourceCache<Resource>::PutItem(const std::string& key, std::shared_ptr<ItemImp<Resource>> value)
{
    cor::ObjectLocker ol(mMutex);

    typename Map::iterator mi = mMap.find(key);
    if (mi != mMap.end())
    {
        throw cor::Exception("ResourceCache::Put() %s : item %s already in cache",
                             mName.c_str(),
                             key.c_str());
        //printf("Added %s to %s ResourceCache\n", key.c_str(), mName.c_str());
    }

    value->mTimer.Reset(mMaxAgeSeconds);
    mList.push_back(value);
    typename List::iterator i = mList.end();
    i--;
    mMap[key] = i;
}

template <class Resource>
void
ResourceCache<Resource>::Put(const std::string& key,
                             Resource value,
                             const std::string& parentKey,
                             CacheInterface* parentCache,
                             CacheInterface* childCache)
{
    //printf("Put -- new %s resource %s\n", mName.c_str(), key.c_str());
    std::shared_ptr<ItemImp<Resource>> item(new ItemImp<Resource>(key, value, parentKey, parentCache, childCache));
    PutItem(key, item);
}

template <class Resource>
void
ResourceCache<Resource>::GetAll(std::vector<ResourceAccessor<Resource>>& resources)
{
    resources.clear();

    cor::ObjectLocker ol(mMutex);
    resources.resize(mMap.size());

    typename Map::iterator mi = mMap.begin();
    for (size_t i = 0; mi != mMap.end(); mi++, i++)
        resources[i] = *(mi->second);
}

// resets the age of the item; throws if it does not exist
template <class Resource>
void
ResourceCache<Resource>::Reset(const std::string& key)
{
    cor::ObjectLocker ol(mMutex);

    typename Map::iterator mi = mMap.find(key);
    if (mi == mMap.end())
    {
        throw cor::Exception("ResourceCache::Reset() %s : item %s not in cache",
                             mName.c_str(),
                             key.c_str());
    }

    // already in table; reorder to most recent
    typename List::iterator li = mMap[key];
    std::shared_ptr<ItemImp<Resource>> value = *li;
    mList.erase(li);
    mList.push_back(value);

    value->mTimer.Reset(mMaxAgeSeconds);

    // update map to point to new list entry
    typename List::iterator i = mList.end();
    i--;
    mMap[key] = i;
}

template <class Resource>
void
ResourceCache<Resource>::Remove(const std::string& key)
{
    //printf("Remove -- removing %s resource %s\n", mName.c_str(), key.c_str());
    cor::ObjectLocker ol(mMutex);

    typename Map::iterator mi = mMap.find(key);

    if (mi == mMap.end())
    {
        throw cor::Exception("ResourceCache::Remove() %s : Item %s not in cache",
                             mName.c_str(),
                             key.c_str());
    }

    // the list owns the instance (and the map only has an iterator to the list)
    typename List::iterator li = mi->second;
    mList.erase(li); // object is probably deleted here
    mMap.erase(key);
}

template <class Resource>
void
ResourceCache<Resource>::Remove(const std::set<std::string>& keys)
{
    cor::ObjectLocker ol(mMutex);

    std::set<std::string>::const_iterator i = keys.begin();
    for (; i != keys.end(); i++)
    {
        const std::string& key = *i;
        typename Map::iterator mi = mMap.find(key);

        if (mi != mMap.end())
        {
            // the list owns the instance (and the map only has an iterator to the list)
            typename List::iterator li = mi->second;
            mList.erase(li); // object is probably deleted here
            mMap.erase(key);
        }
    }
}

template <class Resource>
void
ResourceCache<Resource>::ThreadFunction()
{
    while (!StopNow())
    {
        try {
            cor::ObjectLocker ol(mMutex);

            while (!mList.empty())
            {
                //printf("%ld outstanding %s resources\n", mList.size(), mName.c_str());
                // delete from LRU end of list until age is no longer exceeding
                typename List::iterator i = mList.begin(); // points to first (oldest) element
                std::shared_ptr<ItemImp<Resource>> item = *i;

                // hold while accessing item's mBusyCount and mTimer; cannot let
                // object go from !busy -> busy while we are deleting it.
                cor::ObjectLocker ool(item->mMutex);
                if (item->mTimer.IsExpired())
                {
                    if (item->mBusyCount > 0)
                        continue; // in use; skip this

                    printf("%s %s aged %d seconds (>%d); deleting it\n",
                           mName.c_str(),
                           (*i)->mKey.c_str(),
                           (int)item->mTimer.ElapsedSeconds(),
                           mMaxAgeSeconds);

                    // the list owns the instance (and the map only has an iterator to the list);
                    // so note the map key to delete prior to removing item from the list, because
                    // the object will cease to exist due to reference count decrement.

                    std::string key = (*i)->mKey;
                    mList.erase(i);
                    mMap.erase(key);
                }
                else
                    break;
            }

            //printf("No resources in %s list\n", mName.c_str());


        } catch (const cor::Exception& err)
        {
            printf("Error in %s ResourceCache thread: %s\n", mName.c_str(), err.what());
        }

        sleep(1);
    }
}

template <class Resource>
void
ResourceCache<Resource>::Print()
{
    cor::ObjectLocker ol(mMutex);

    printf("%s Cache\n", mName.c_str());
    printf("List:\n");
    {
        typename List::const_iterator i = mList.begin();
        for (; i != mList.end(); i++)
        {
            std::shared_ptr<ItemImp<Resource>> item = *i;
            printf("   Item %s: time remaining %d\n", item->mKey.c_str(), (int)(item->mTimer.RemainingSeconds()));
        }
    }

    printf("Map:\n");
    {
        typename Map::const_iterator i = mMap.begin();
        for (; i != mMap.end(); i++)
        {
            printf("   %s\n", i->first.c_str());
        }
    }
}


}

#endif
