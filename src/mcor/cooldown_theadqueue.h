//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#ifndef GICM_COOLDOWN_THREADQUEUE_H
#define GICM_COOLDOWN_THREADQUEUE_H

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


template <class T>
class ItemImp;

/** class CooldownThreadQueue : a queue that contains an object for a certain
 *     amount of time (a "cooldown" from video gaming usage), then calls a
 *     handler and removes it.
 *
 */
template <class T>
class CooldownThreadQueue : public cor::Thread
{
public:
    CooldownThreadQueue(const std::string& name, const cor::Time& cooldown);
    virtual ~CooldownThreadQueue();

    void SetCooldown(const cor::Time& cooldown);
    cor::Time GetCooldown() const;

    // insert a new item into the cache; will throw if the item already exists
    void Insert(const T& item);

    // remove an item; throws if it does not exist
    void Remove(const T& key);

    // return the size of the queue
    size_t Size() const;
    
    // Function called when item(s) have completed their cooldown period
    // and have been removed from the collection
    virtual void Completed(std::list<T>& items) {};

protected:
    mutable cor::MMutex mMutex;
    cor::Time mCooldown;

    // The list is the owner, and keeps the objects in age order
    // A map allows quick access to the item in the list
    //
    // The "too old" policy is executed based on a time kept in each
    // item, but the age ordering of the list allows the culling
    // of aged-out items to be efficient (the entire list does not
    // need to be searched each time through, only the items that
    // are oldest and will be removed)

    // back of list is most recent, front is oldest
    typedef std::list<ItemImp<T>> List;
    List mList;
    typedef std::map<T, typename List::iterator > Map;
    Map mMap;

    void ThreadFunction();

};



// ============================================================================
// template implementation

template<class T>
class ItemImp
{
public:
    ItemImp(const T& r, const cor::Time& wakeupTime) :
            mTime(wakeupTime),
            mImp(r)
    {}

    ~ItemImp()
    {
    }

    void SetCondition(cor::MCondition* c) { mCondition = c; }

    cor::Time mTime;
    T mImp;

    cor::MCondition* mCondition;
};


template <class T>
CooldownThreadQueue<T>::CooldownThreadQueue(const std::string& name, const cor::Time& cooldown) :
        cor::Thread(name + " CooldownThreadQueue"),
        mMutex("CooldownThreadQueue " + name, false), // false => non-recursive, for use with Condition
        mCooldown(cooldown)
{
}

template <class T>
CooldownThreadQueue<T>::~CooldownThreadQueue()
{
    Stop();
}

template <class T>
void
CooldownThreadQueue<T>::SetCooldown(const cor::Time& cooldown)
{
    cor::ObjectLocker ol(mMutex);
    mCooldown = cooldown;
}

template <class T>
cor::Time
CooldownThreadQueue<T>::GetCooldown() const
{
    cor::ObjectLocker ol(mMutex);
    return mCooldown;
}

template <class T>
void
CooldownThreadQueue<T>::Insert(const T& item)
{
    cor::ObjectLocker ol(mMutex);

    typename Map::iterator mi = mMap.find(item);
    if (mi != mMap.end())
    {
        throw cor::Exception("CooldownThreadQueue::Put() : item already in queue");
    }

    mList.push_back(ItemImp<T>(item, cor::Time() + mCooldown));
    typename List::iterator i = mList.end();
    i--;
    mMap[item] = i;
}


template <class T>
void
CooldownThreadQueue<T>::Remove(const T& key)
{
    //printf("Remove -- removing %s resource %s\n", mName.c_str(), key.c_str());
    cor::ObjectLocker ol(mMutex);

    typename Map::iterator mi = mMap.find(key);

    if (mi == mMap.end())
    {
        // not an error
        return;
        //throw cor::Exception("CooldownThreadQueue::Remove() : Item is not in cache");
    }

    // the list owns the instance (and the map only has an iterator to the list)
    typename List::iterator li = mi->second;
    mList.erase(li);
    mMap.erase(key);
}

template <class T>
size_t
CooldownThreadQueue<T>::Size() const
{
    cor::ObjectLocker ol(mMutex);
    if (mList.size() != mMap.size())
        printf("Unexpected size delta in CooldownThreadQueue::Size()!!!!!\n");
    return mList.size();
}

template <class T>
void
CooldownThreadQueue<T>::ThreadFunction()
{
    while (!StopNow())
    {
        try {
            cor::ObjectLocker ol(mMutex);

            while (!mList.empty())
            {
                //printf("%ld outstanding items\n", mList.size());
                // delete from LRU end of list until age is no longer exceeding
                typename List::iterator i = mList.begin(); // points to first (oldest) element
                const ItemImp<T>& item = *i;

                cor::MCondition waiter("CooldownThreadQueue::Waiter", mMutex);
                if (!waiter.Wait(item.mTime))
                {
                    //printf("Timeout for item at %s\n", item.mTime.Print().c_str());
                    // condition timed out, which is the expected case; remove it and
                    // invoke Completed
                    cor::Time now;

                    // get all items that are past time; note that the item whose time
                    // it is to be popped off the queue might have been removed
                    // asynchronously, so it is not an error that no items are removed
                    // due to time below.
                    std::list<T> completedItems;
                    while (i != mList.end() && (i->mTime < now))
                    {
                        //printf("Erasing item at time %s\n", i->mTime.Print().c_str());
                        completedItems.push_back(i->mImp);
                        i = mList.erase(i);
                        mMap.erase(completedItems.back());
                    }
                    Completed(completedItems);
                }
                else
                {
                    printf("Impossible\n");
                    // should be unreachable
                }
            }

            //printf("No resources in %s list\n", mName.c_str());

        } catch (const cor::Exception& err)
        {
            printf("Error in CooldownThreadQueue thread: %s\n", err.what());
        }

        sleep(1);
    }
}


}

#endif
