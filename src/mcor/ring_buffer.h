//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_RING_BUFFER_H
#define GICM_RING_BUFFER_H

#include <algorithm>
#include <vector>

#include "mcondition.h"
#include "mmutex.h"


namespace cor {


/** template RingBuffer : very simple circular buffer
 *
 *  Threadsafe.
 */
template <class T>
class RingBuffer
{
public:
    RingBuffer(size_t size);
    virtual ~RingBuffer();

    // empty the buffer
    void Reset();

    // returns amount of data in buffer
    size_t Length() const;
    bool Empty() const { return Length() == 0; }
    // returns amount of data that can be written before an overrun
    size_t CapacityLeft() const {return mImp.size() - Length();}
    // returns total capacity
    size_t Capacity() const { return mImp.size(); }

    // returns number of data copied to out; does not remove data
    // from buffer
    size_t Copy(std::vector<T>& out, size_t n, int msecWait) const;

    // returns newest data, throws if empty
    T& Newest();
    // returns oldest data, throws if empty
    T& Oldest();

    // returns false if underruns (not enough data to read n); otherwise
    // fills out with n data and removes it from buffer.
    bool Read(std::vector<T>& out, size_t n, int msecWait);
    bool Read(T& out, int msecWait); // single read
    
    // returns false if overruns (not enough capacity to write in): the
    // write will succeed, but some of the oldest data will be clobbered
    bool Write(const std::vector<T>& in);
    bool Write(const T& in); // single write

private:
    RingBuffer();
    RingBuffer(const RingBuffer& other);

    // returns amount of data in buffer
    size_t LengthPrivate() const;
    bool EmptyPrivate() const { return LengthPrivate() == 0; }
    size_t CopyPrivate(std::vector<T>& out, size_t n, int msecWait) const;

    mutable cor::MMutex mMutex;
    mutable MCondition mCondition;
    std::vector<T> mImp;
    size_t mRead;
    size_t mWrite;
};


template<class T>
RingBuffer<T>::RingBuffer(size_t size) :
    mMutex("RingBuffer", false),
    mCondition("RingBuffer", mMutex),
    mRead(0),
    mWrite(0)
{
        mImp.resize(size);
}

template<class T>
RingBuffer<T>::~RingBuffer()
{
}

template<class T>
void
RingBuffer<T>::Reset()
{
    mRead = mWrite = 0;
}

template<class T>
size_t
RingBuffer<T>::Length() const
{
    cor::ObjectLocker ol(mMutex);
    return LengthPrivate();
}

template<class T>
size_t
RingBuffer<T>::LengthPrivate() const
{
    if (mWrite >= mRead)
        return mWrite - mRead;
    return mWrite + mImp.size() - mRead;
}

template<class T>
size_t
RingBuffer<T>::Copy(std::vector<T>& out, size_t n, int msecWait) const
{
    cor::ObjectLocker ol(mMutex);
    return CopyPrivate(out, n, msecWait);
}

template<class T>
size_t
RingBuffer<T>::CopyPrivate(std::vector<T>& out, size_t n, int msecWait) const
{
    size_t l = LengthPrivate();
    if (l == 0)
    {
        mCondition.Wait(msecWait);
    }
    l = LengthPrivate();
    if (l == 0)
        return 0;

    if (n > l)
    {
        n = l;
    }

    out.resize(n);

    if (mWrite < mRead)
    {
        size_t top = mImp.size() - mRead;
        if (top > n)
        {
            std::copy_n(mImp.begin() + mRead, n, out.begin());
        }
        else
        {
            std::copy_n(mImp.begin() + mRead, top, out.begin());
            std::copy_n(mImp.begin(), n - top, out.begin() + top);
        }
    }
    else
    {
        std::copy_n(mImp.begin() + mRead, n, out.begin());
    }

    return n;
}

template<class T>
T&
RingBuffer<T>::Newest()
{
    cor::ObjectLocker ol(mMutex);
    if (EmptyPrivate())
        throw cor::Exception("ring buffer: Newest() called on empty container");

    // return data just behind the write pointer
    if (mWrite == 0)
        return mImp.back();
    return mImp[mWrite - 1];
}

template<class T>
T&
RingBuffer<T>::Oldest()
{
    cor::ObjectLocker ol(mMutex);
    if (EmptyPrivate())
        throw cor::Exception("ring buffer: Oldest() called on empty container");

    // return data at read pointer
    return mImp[mRead];
}

template<class T>
bool
RingBuffer<T>::Read(std::vector<T>& out, size_t n, int msecWait)
{
    cor::ObjectLocker ol(mMutex);
    n = CopyPrivate(out, n, msecWait);

    // advance read
    if (n != 0)
    {
        mRead += n;
        mRead = mRead % mImp.size();
    }

    return n != 0;
}

template<class T>
bool
RingBuffer<T>::Read(T& out, int msecWait)
{
    std::vector<T> t;
    bool r = Read(t, 1, msecWait);
    if (r) {
        out = t.front();
    }
    return r;
}

template<class T>
bool
RingBuffer<T>::Write(const std::vector<T>& in)
{
    cor::ObjectLocker ol(mMutex);
    size_t l = LengthPrivate();

    if (in.size() >= mImp.size())
        throw cor::Exception("ring buffer: single write (%d) exceeded size (%d)", in.size(), mImp.size());

    if ((mWrite + in.size()) > mImp.size())
    {
        size_t top = mImp.size() - mWrite;
        std::copy_n(in.begin(), top, mImp.begin() + mWrite);
        std::copy_n(in.begin() + top, in.size() - top, mImp.begin());
    }
    else
    {
        std::copy_n(in.begin(), in.size(), mImp.begin() + mWrite);
    }

    // advance write
    mWrite += in.size();
    mWrite = mWrite % mImp.size();

    // advance read, if necessary
    int ra = in.size() - (mImp.size() - l) + 1;

    if (ra > 0)
    {

        mRead += ra;
        mRead = mRead % mImp.size();
    }

    mCondition.Signal();

    return (ra <= 0);
}


template<class T>
bool
RingBuffer<T>::Write(const T& in)
{
    std::vector<T> t;
    t.push_back(in);
    return Write(t);
}


} // end namespace
#endif
