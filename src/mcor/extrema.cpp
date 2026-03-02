//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <math.h>

#include "extrema.h"


namespace cor {

void
Extrema::operator&=(double d)
{
    if (!mInit)
    {
        mMin = mMax = d;
        mInit = true;
        return;
    }

    if (d < mMin)
        mMin = d;
    else if (d > mMax)
        mMax = d;
}

Extrema
Extrema::Scale() const
{
    Extrema r;
    r.mMax = ScaleMax(mMax);
    r.mMin = ScaleMin(mMin);
    return r;
}

double
ScaleMax(double d)
{
    if (d == 0)
        return 0;

    // determine the order of magnitude
    double absnum = fabs(d);
    double order = log10(absnum);
    order = pow(10, floor(order));

    // scale is next integer higher
    // 71   -> 80  ceil( 71 / 10 )  * 10

    double r = ceil(d / order);
    r *= order;
    //printf("ScaleMax: %f -> %f\n", d, r);

    return r;
}

double
ScaleMin(double d)
{
    if (d == 0)
        return 0;

    // determine the order of magnitude
    double absnum = fabs(d);
    double order = log10(absnum);
    order = pow(10, floor(order));

    // scale is next integer lower
    // 71   -> 70  floor( 71 / 10 )  * 10

    double r = floor(d / order);
    r *= order;
    //printf("ScaleMax: %f -> %f\n", d, r);

    return r;
}

}