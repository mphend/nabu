//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef PKG_NABU_ADDRESS_INCLUDED
#define PKG_NABU_ADDRESS_INCLUDED

#include <list>
#include <map>
#include <string>
#include <vector>

#include <json/json.h>
#include <mcor/bigtime.h>

#include "column_name.h"

namespace nabu
{

/* Layers:
 *
 * Layer 0 is the root, which is the first of two layers that hold the
 * column name hash
 *
 * Layer 2 is the first layer of the physical address.
 *
 */

/** class Coordinate : a multi-dimensional number
 *
 */
struct Coordinate : public std::vector<BigTime>
{
    Coordinate() {};
    Coordinate(const BigTime& bt);
    Coordinate(const BigTime& bt1, const BigTime& bt2);
    Coordinate(size_t size);

    std::string Print() const;
    std::string PrintHex() const;
};


/** class PhysicalAddress : address in physical terms
 *
 */
struct PhysicalAddress
{
    PhysicalAddress();
    PhysicalAddress(const ColumnName& column, const Coordinate& coordinate);

    ColumnName mColumn;
    Coordinate mCoordinate;

    Coordinate mColumnHash;

    std::string Print() const;
};


/** class AddressResolver
 *
 */
class AddressResolver
{
public:
    AddressResolver();

    void PushRadix(const Coordinate& radix);

    // translate a physical address into a hierarchical address
    Coordinate Resolve(size_t layer,
                       const PhysicalAddress& physicalAddress,
                       Coordinate& remainder);

    std::string Print() const;

private:
    std::vector<Coordinate> mRadixes;
};

}

#endif
