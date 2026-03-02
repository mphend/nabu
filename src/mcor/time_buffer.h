//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_TIME_BUFFER_H
#define GICM_TIME_BUFFER_H

#include <map>
#include <vector>

#include <mcor/timerange.h>

namespace cor {


/** template TimeBuffer : size-constrained time-indexed buffer
 * 
 */
template <class T>
class TimeBuffer
{
public:
    TimeBuffer(size_t maxSize);
    virtual ~TimeBuffer();

    size_t size() const { return mImp.size(); }
    bool empty() const { return mImp.empty(); }
    size_t capacity() const { return mMaxSize; }

    // add t to container, removing the oldest element if necessary to obey
    // size constraint
    void Insert(const cor::Time& whence, const T& t);

    // appends all data from 'since' and newer into 'out' without
    // modifying the buffer contents
    void CopySince(const cor::Time& since, std::vector<T>& out) const;

    // remove all entries of age t and older; returns number removed
    // if this is nonzero, timeRange will hold the span of item removed
    size_t DeleteOlder(const cor::Time& t, cor::TimeRange& timeRange);

    // returns newest data, throws if empty
    T& Newest();
    // returns oldest data, throws if empty
    T& Oldest();

    void Print() const;

private:
    TimeBuffer();
    TimeBuffer(const TimeBuffer& other);

    typedef std::map<cor::Time, T> Imp;
    Imp mImp;

    size_t mMaxSize;
};


template<class T>
TimeBuffer<T>::TimeBuffer(size_t size) :
    mMaxSize(size)
{
}

template<class T>
TimeBuffer<T>::~TimeBuffer()
{
}

template<class T>
void
TimeBuffer<T>::Insert(const cor::Time& whence, const T& t)
{
    mImp[whence] = t;
    if (mImp.size() > mMaxSize)
        mImp.erase(mImp.begin());
}

template<class T>
void
TimeBuffer<T>::CopySince(const cor::Time& since, std::vector<T>& out) const
{
    typename Imp::const_iterator i = mImp.lower_bound(since);

    for (size_t j = 0; i != mImp.end(); i++, j++)
    {
        out.push_back(i->second);
    }
}

// remove all entries of age t and older; returns number removed
// if this is nonzero, timeRange will hold the span of item removed
template<class T>
size_t
TimeBuffer<T>::DeleteOlder(const cor::Time& t, cor::TimeRange& timeRange)
{
    if (empty())
        return 0;

    typename Imp::iterator i = mImp.upper_bound(t);
    // i points to first element newer than t (or possibly the end)
    i--;
    if (i->first > t)
        return 0; // nothing in container is as old as t

    size_t n = 0;
    if (i != mImp.end())
    {
        size_t before = mImp.size();
        timeRange.First() = mImp.begin()->first;
        timeRange.Final() = i->first;
        mImp.erase(mImp.begin(), ++i);
        n = before - mImp.size();
    }
    return n;
}

template<class T>
T&
TimeBuffer<T>::Newest()
{
    if (empty())
        throw cor::Exception("TimeBuffer: Newest() called on empty container");

    return mImp.rbegin()->second;
}

template<class T>
T&
TimeBuffer<T>::Oldest()
{
    if (empty())
        throw cor::Exception("TimeBuffer: Oldest() called on empty container");

    return mImp.begin()->second;
}

template<class T>
void
TimeBuffer<T>::Print() const
{
    typename Imp::const_iterator i = mImp.begin();

    for (size_t j = 0; i != mImp.end(); i++, j++)
    {
        printf("%s\n", i->first.Print(true).c_str());
    }
}


} // end namespace
#endif
