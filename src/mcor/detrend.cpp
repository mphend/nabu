//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include "detrend.h"


namespace cor {

void
Detrend(const std::vector<double>& data, std::vector<double>& out)
{
    if (data.empty()) // DEFENSIVE
        return;

    if (&out == &data) // DEFENSIVE
        throw cor::Exception("Cannot detrend in place (out cannot be same object as in)");

    double slope = (data.back() - data.front()) / (data.size() - 1);
    double yIntercept = data.front();

    out.clear();
    out.resize(data.size());
    for (size_t i = 0; i < data.size(); i++)
    {
        out[i] = (data[i] - (slope * i + yIntercept));
    }
}

void
Detrend(std::vector<double>& data)
{
    std::vector<double> tempVec;

    Detrend(data, tempVec);

    data = tempVec;
}

}