//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef __mcor_timerange__
#define __mcor_timerange__


#include <set>
#include <string>
#include <vector>

#include <mcor/mexception.h>
#include <mcor/mtime.h>


namespace cor
{


class TimeRangeSet;


/** A range of time
 *
 */
class TimeRange
{
public:
    TimeRange(); // is 'invalid'
    TimeRange(const Time& first, const Time& final);

    virtual ~TimeRange();

    const Time& First() const;
    Time& First();
    const Time& Final() const;
    Time& Final();

    // throws if this is invalid object or of infinite length
    Time Length() const;
    // returns number of integer seconds spanned by the range
    // e.g., span of 1.5 seconds = 1
    size_t IntegerSecondCount() const;
    // returns number of UTC seconds spanned by the range
    // e.g., span of 1.5 seconds starting at UTC .75 = 2, starting at UTC .25 = 1
    size_t UtcSecondCount() const;

    // tests if start == end
    bool IsDegenerate() const;

    // test for infinite
    bool IsInfinite() const;
    bool GoesToPositiveInfinity() const;
    bool GoesToNegativeInfinity() const;

    // returns if t is within range
    bool In(const Time& t) const;

    // returns if this is within other's range
    bool Within(const TimeRange& other) const;

    // returns if overlapping
    bool Intersects(const TimeRange& other) const;

    // returns intersection or an invalid Timerange if there is no overlap
    TimeRange Intersection(const TimeRange& other) const;
    // returns union or invalid Timerange if there is no overlap
    TimeRange Union(const TimeRange& other) const;
    // returns total extent of the range of this and other
    TimeRange SuperUnion(const TimeRange& other) const;

    // returns all members of 'other' trimmed to be within 'this', and
    // performs Union on them
    void Intersection(const TimeRangeSet& other, TimeRangeSet& intersection) const;

    // returns true if this follows other
    bool Follows(const TimeRange& other) const;
    // returns true if this precedes other
    bool Precedes(const TimeRange& other) const;
    // returns true if this adjoins other without intersection
    bool IsAdjacent(const TimeRange& other) const;

    static TimeRange SuperUnion(const TimeRangeSet& in);
    static TimeRange SuperUnion(const std::vector<cor::TimeRange>& in);

    // produce the minimal set of disjoint TimeRange objects
    static void Union(const std::vector<TimeRange>& in, std::vector<TimeRange>& out);
    static void Union(const TimeRangeSet& in, std::vector<TimeRange>& out);
    static void Union(const TimeRangeSet& in, TimeRangeSet& out);

    // adjusts all members of in to be included in limit; removes any members
    // which are not valid
    static void Trim(TimeRangeSet& in, const cor::TimeRange& limit);

    // produce the set of time ranges that remain after removing all intersections with 'in'
    void Remove(const std::vector<TimeRange>& in, std::vector<TimeRange>& out) const;

    bool Valid() const;

    operator bool() { return Valid(); }

    TimeRange& operator +=(const Time& t);
    TimeRange&  operator -=(const Time& t);

    virtual std::string Print(bool decimal = false) const;
    virtual std::string PrintRaw() const;
    void Print(std::ostream&, bool decimal = false) const;

    // return the midpoint
    Time Midpoint() const;

    // return the UTC day that contains t
    static TimeRange UtcDayOf(const cor::Time& t);

protected:
    Time mFirst;
    Time mFinal;
};

class TimeRangeSet : public std::set<TimeRange>
{
public:
    // insert all elements of other into this
    void Merge(const TimeRangeSet& other);

    // make this set a set of step-sized time ranges that starts at 'start'
    // and ends at stop
    void Span(const Time& start,
              const Time& stop,
              const Time& step);
};

// sort is done by comparison in the following precedence:
//    first time compared to first time
//    second time compared to second time
bool operator >(const TimeRange& t1, const TimeRange& t2);
bool operator >=(const TimeRange& t1, const TimeRange& t2);
bool operator <(const TimeRange& t1, const TimeRange& t2);
bool operator <=(const TimeRange& t1, const TimeRange& t2);
bool operator ==(const TimeRange& t1, const TimeRange& t2);
bool operator !=(const TimeRange& t1, const TimeRange& t2);

bool operator ==(const TimeRange& tr, const Time& t);
bool operator ==(const Time& t, const TimeRange& tr);

std::ostream& operator<<(std::ostream& os, const TimeRange& t);

// produce a vector of timeranges from 'source' onto 'snap'; the total
// amount of time contained in source that overlaps snap will be returned in
// a set of timeranges that are each entirely interior to those in 'snap'
// optionally, perform a Union on each snapped set to merge adjacent/overlapping
// TimeRange within the same snap bucket (but not between them)
//
// if overlap occurs within 'source' or 'snap', then results are undefined
//
// For instancce, if 'snap' are 24-hour day periods, then 'projection' will be
// the time within those days, never crossing 24-hour boundaries, of the
// time in 'source' that are within those day periods

void Projection(const TimeRangeSet& source,
                const TimeRangeSet& snap,
                TimeRangeSet& projection,
                bool unify = true);

std::string Print(const TimeRangeSet& trs);
    
}

#endif
