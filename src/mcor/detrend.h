//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __mcor_detrend_included__
#define __mcor_detrend_included__


#include <vector>

#include <mcor/mexception.h>

namespace cor
{

// detrends the data; beginning and end of data will be zero
// out cannot be the same object as in; this will throw
void Detrend(const std::vector<double>& in, std::vector<double>& out);

// in-place detrending
void Detrend(std::vector<double>& data);
}

#endif
