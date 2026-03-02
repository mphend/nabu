//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_TIME_SCHEME_H
#define GICM_NABU_TIME_SCHEME_H

#include <set>

#include <mcor/mtime.h>
#include <mcor/timerange.h>

#include "table_address.h"
#include "time_path.h"

namespace nabu
{


/** Class UtcTimeScheme : Timescheme based on UTC seconds
 */
class UtcTimeScheme
{
public:
    // finest-grained period is first element, each element after is count of
    // the previous layer:
    // 10 -- 10 second cumulative
    // 3 -- 30 seconds cumulative
    // 2 -- 60 seconds cumulative
    // etc.
    UtcTimeScheme() {}
    UtcTimeScheme(int count, ...);
    UtcTimeScheme(const std::vector<unsigned int>& periods);
    virtual ~UtcTimeScheme();

    void SetPeriods(const std::vector<unsigned int>& periods);

    size_t Size() const { return mPeriods.size(); }

    // the earliest time expressible
    nabu::TimePath Origin() const;
    // the last time expressible
    nabu::TimePath Termination() const;

    nabu::TimePath GetPath(const cor::Time& t) const;
    cor::TimeRange GetTime(const nabu::TimePath& timePath) const;

    // get all paths contained in the time range
    void GetPaths(std::vector<TimePath>& paths, const cor::TimeRange& timeRange) const;

    // return the time range snapped to the grid, i.e., spanning the
    // first moment in the first bin to the last moment in the final bin
    cor::TimeRange Snap(const cor::TimeRange& in) const;
    cor::Time SnapFirst(const cor::Time& in) const;
    cor::Time SnapFinal(const cor::Time& in) const;

    // return the time range snapped to the grid and rounded down, i.e.,
    // spanning the first moment in the first bin and the last moment
    // of the final bin that was exceeded by 'in'
    cor::TimeRange SnapDown(const cor::TimeRange& in) const;

    // returns metadata layer of roots
    size_t RootLayer() const;
    // returns metadata layer that is addressed by tableAddress
    size_t LayerOf(const TableAddress& tableAddress) const;

    std::vector<cor::TimeRange> GetTimeRange(const cor::Time& t) const;

    const std::vector<unsigned int>& GetPeriods() const { return mPeriods; }

    // fill timePath, starting at layer, with the final chunk for each layer
    // e.g.
    // tp = [4][5][2][3]
    // SetToLat(tp, 2)
    // tp = [4][5][9][9]
    void SetToLast(TimePath& tp, size_t layer) const;

    // returns if times are within the same leaf
    bool InSameLeaf(const cor::Time& t1, const cor::Time& t2) const;

    void FromJson(const Json::Value& v);
    void ToJson(Json::Value& v) const;

    std::string Print() const;

protected:
    std::vector<unsigned int> mPeriods;
};


} // end namespace

#endif
