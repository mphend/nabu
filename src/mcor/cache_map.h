//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __kriger__cache_map__
#define __kriger__cache_map__

#include <mcor/mexception.h>

#include <vector>

namespace cor {


/** Class CacheMap : a 2D array of <x, y>
 *
 *   This is different from a plain 2D container in its inclusion of a validity
 *   concept.  At construction, this object is an empty cache.  As missing data
 *   is accessed, 'miss' operations fill the container and set validity flags
 *   for <x, y>.
 *
 */

template <class T>
class CacheMap
{
public:
    CacheMap(size_t dimX, size_t dimY);
    virtual ~CacheMap() {}

    // return the data; may cause Miss() to run if that data is invalid
    const T& Get(size_t x, size_t y);

    // set validity signature; defaults to 1
    void SetSignature(unsigned long signature) { mSignature = signature; }

protected:
    // The outer dimension is x*y
    typedef std::vector< T > Imp;

    Imp mImp; // the data
    std::vector<unsigned long> mValidity; // validity
    const size_t mDimX;
    const size_t mDimY;
    const size_t mFrameSize; // mDimX * mDimY
    unsigned long mSignature;

    virtual void Miss(size_t x, size_t y, T& data) = 0;
};


// ===================================

template <class T>
CacheMap<T>::CacheMap(size_t dimX, size_t dimY) :
    mDimX(dimX),
    mDimY(dimY),
    mFrameSize(dimX * dimY),
    mSignature(1)
{
    mImp.resize(mFrameSize);

    mValidity.resize(mFrameSize);
    for (size_t i = 0; i < mValidity.size(); i++)
        mValidity[i] = ~mSignature;
}

template<class T>
const T&
CacheMap<T>::Get(size_t x, size_t y)
{
    if ((x * mDimY + y) >= mFrameSize)
        throw Exception("Out-of-range (%d,%d)", x, y);

    if (mValidity[x * mDimY + y] != mSignature)
    {
        Miss(x, y, mImp[x * mDimY + y]);
        mValidity[x * mDimY + y] = mSignature;
    }
    
    return mImp[x * mDimY + y];
}


} // end namespace

#endif
