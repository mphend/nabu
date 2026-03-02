//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <limits>
#include <sstream>

#include <openssl/md5.h>

#include <mcor/binary.h>
#include <mcor/hash_md5.h>
#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include "address.h"



namespace nabu {

Coordinate::Coordinate(const BigTime& bt) :
    std::vector<BigTime>(1, bt)
{
}

Coordinate::Coordinate(const BigTime& bt1, const BigTime& bt2) :
    std::vector<BigTime>(2)
{
    (*this)[0] = bt1;
    (*this)[1] = bt2;
}

Coordinate::Coordinate(size_t size) :
    std::vector<BigTime>(size)
{
}

std::string
Coordinate::Print() const
{
    if (empty())
        return "";
    if (size() == 1)
        return front().PrintFractional();

    std::string s = "< ";
    for (size_t i = 0; i < size() - 1; i++)
        s += operator[](i).PrintFractional() + ", ";
    s += back().Print() + " >";

    return s;
}

std::string
Coordinate::PrintHex() const
{
    if (empty())
        return "";
    if (size() == 1)
        return front().PrintFractionalHex();

    std::string s = "< ";
    for (size_t i = 0; i < size() - 1; i++)
        s += operator[](i).PrintFractionalHex() + ", ";
    s += back().Print() + " >";

    return s;
}

PhysicalAddress::PhysicalAddress()
{
}

PhysicalAddress::PhysicalAddress(const ColumnName& column, const Coordinate& coordinate) :
    mColumn(column),
    mCoordinate(coordinate)
{
    mColumnHash = column.Hash();
}

std::string
PhysicalAddress::Print() const
{
    std::string s = mColumn.GetLiteral();
    s += " (" + mColumnHash.PrintHex() + ") ";
    s += mCoordinate.Print();
    return s;
}

AddressResolver::AddressResolver()
{
}

void
AddressResolver::PushRadix(const Coordinate& radix)
{
    //printf("Radix being pushed: 0x%lx 0x%lx\n", radix.front().mSeconds, radix.front().mAttoSeconds);

    // valid radix:
    // * must have same number of dimensions
    // * each radix must not overlap any other in its dimension; 0x0f and 0xf0,
    //   not 0x0f and 0x03, for example
    // * radix must be a single contiguous set of bits, like 0x00fff or 0x3;
    //   not 0x5A, etc.

    if (!mRadixes.empty() && mRadixes.front().size() != radix.size())
    {
        throw cor::Exception("AddressResolver::PushRadix() -- new radix is of dimension %ld, not %ld",
            radix.size(),
            mRadixes.front().size());
    }

    // construct the current bitmask. It has already been checked by previous
    // calls of PushRadix, so it is known to conform to the expectations above.
    Coordinate totalMask(radix.size());
    for (size_t i = 0; i < mRadixes.size(); i++)
    {
        for (size_t j = 0; j < mRadixes[i].size(); j++)
        {
            totalMask[j].mAttoSeconds |= mRadixes[i][j].mAttoSeconds;
            totalMask[j].mSeconds |= mRadixes[i][j].mSeconds;
        }
    }
    //printf("totalMask existing = %s\n", totalMask.PrintHex().c_str());

    // check for contiguous bits
    for (size_t i = 0; i < radix.size(); i++)
    {
        bool set = false;
        int count = 0;

        for (size_t f = 0; f < 2; f++)
        {
            const int64_t* field = f == 0 ? &(radix[i].mSeconds) : &(radix[i].mAttoSeconds);
            uint64_t bit = 0x8000000000000000ll;
            for (size_t j = 0; j < 64; j++)
            {
                if (bit & *field)
                {
                    //printf("0x%lx & 0x%lx is set\n", bit, *field);
                    if (!set)
                    {
                        set = true;
                        count++;
                    }
                }
                else
                {
                    //printf("0x%lx & 0x%lx is clear\n", bit, *field);
                    if (set)
                        set = false;
                }
                bit = bit >> 1;
            }
        }
        //printf("count = %ld\n", count);
        if (count != 1)
            throw cor::Exception("AddressResolver::PushRadix() -- radix %s is not contiguous bits", radix.PrintHex().c_str());
    }

    // check for continuity against existing bits
    for (size_t i = 0; i < radix.size(); i++)
    {
        bool set = false;
        int count = 0;

        for (size_t f = 0; f < 2; f++)
        {
            int64_t x;
            if (f == 0)
            {
                x = totalMask[i].mSeconds;
                if (x & radix[i].mSeconds)
                {
                    throw cor::Exception("AddressResolver::PushRadix() -- radix[%ld] 0x%lx overlaps existing bits (0x%lx)",
                        i,
                        radix[i].mSeconds,
                        totalMask[i].mSeconds);
                }
                x = x | radix[i].mSeconds;
            }
            else
            {
                x = totalMask[i].mAttoSeconds;
                if (x & radix[i].mAttoSeconds)
                {
                    throw cor::Exception("AddressResolver::PushRadix() -- radix[%ld] 0x%lx overlaps existing bits (0x%lx)",
                                         i,
                                         radix[i].mAttoSeconds,
                                         totalMask[i].mAttoSeconds);
                }
                x = x | radix[i].mAttoSeconds;
            }
            //printf("%ld: x = 0x%lx\n", f, x);

            uint64_t bit = 0x8000000000000000ll;
            for (size_t j = 0; j < 64; j++)
            {
                if (bit & x)
                {
                    //printf("0x%lx & 0x%lx is set\n", bit, x);
                    if (!set)
                    {
                        set = true;
                        count++;
                    }
                }
                else
                {
                    //printf("0x%lx & 0x%lx is clear\n", bit, x);
                    if (set)
                        set = false;
                }
                bit = bit >> 1;
            }
            //printf("%ld: set = %d, count = %d\n", f, set, count);
        }
        //printf("count = %ld\n", count);
        if (count != 1)
            throw cor::Exception("AddressResolver::PushRadix() -- radix %s is not contiguous with previous bits",
                                 radix.PrintHex().c_str());
    }

    mRadixes.push_back(radix);
}

Coordinate
AddressResolver::Resolve(size_t layer,
                   const PhysicalAddress& physicalAddress,
                   Coordinate& remainder)
{
    Coordinate r;

    if (layer >= mRadixes.size())
        throw cor::Exception("AddressResolver::Resolve() -- do not have enough radix depth for layer %ld\n", layer);

    return r;
}

std::string
AddressResolver::Print() const
{
    std::string s;
    for (size_t i = 0; i < mRadixes.size(); i++)
    {
        s += mRadixes[i].PrintHex();
        if (i != mRadixes.size() - 1)
            s += "\n";
    }
    return s;
}

} // end namespace
