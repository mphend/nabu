//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef PKG_MCOR_EXTREMA_INCLUDED
#define PKG_MCOR_EXTREMA_INCLUDED


#include <vector>

#include <mcor/mexception.h>

namespace cor
{

/** class Extrema : minimum and maximum accumulator
 *
 */
struct Extrema
{
    Extrema() : mMax(0), mMin(0), mInit(false) {}

    // absorb d into the extrema
    void operator&=(double d);

    double mMax;
    double mMin;
    bool mInit;

    // return the ScaleMax(mMax) and ScaleMin(mMin)
    Extrema Scale() const;
};

// compute the next higher integer on the correct order of magnitude to
// represent the number d
// 1.1  -> 2
// 2.0  -> 2
// 11.0 -> 20
// etc.
double ScaleMax(double d);

// compute the next lower integer on the correct order of magnitude to
// represent the number d
// 1.1  -> 1
// 2.0  -> 2
// 11.0 -> 10
// etc.
double ScaleMin(double d);

}

#endif
