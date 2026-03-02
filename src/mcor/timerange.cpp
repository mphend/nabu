//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <stdio.h>
#include <set>
#include <sstream>

#include "timerange.h"


namespace cor {

TimeRange::TimeRange() :
    mFirst(Time::eInvalid),
    mFinal(Time::eInvalid)
{}

TimeRange::TimeRange(const Time& first, const Time& final) :
    mFirst(first),
    mFinal(final)
{}

TimeRange::~TimeRange()
{}

const Time&
TimeRange::First() const
{
    return mFirst;
}

Time&
TimeRange::First()
{
    return mFirst;
}

const Time&
TimeRange::Final() const
{
    return mFinal;
}

Time&
TimeRange::Final()
{
    return mFinal;
}

Time
TimeRange::Length() const
{
    if (!Valid())
        throw cor::Exception("TimeRange::Length() -- invalid timerange");
    if (IsInfinite())
        throw cor::Exception("TimeRange::Length() -- infinite timerange");
    return (mFinal - mFirst);
}

size_t
TimeRange::IntegerSecondCount() const
{
    double d = Length().ToDouble();
    size_t r = d;
    if (d == r)
    {
        // XXX inclusive
        // if both ends are inclusive, add one
        d++;
        // if both ends are exclusive, subtract one
    }
    return d;
}

size_t
TimeRange::UtcSecondCount() const
{
    double d = Length().ToDouble();
    size_t r = d;
    // if exactly integer number of seconds, the endpoints must be considered
    if (d == r)
    {
        if (mFirst.AttoSecondPortion() == 0) // i.e., on a second exactly
        {
            // mFinal will by necessity have zero attosec portion as well
            r--;

            // XXX inclusive
            // if mFirst is inclusive
            r++;
            // else
            // r--;

            // XXX inclusive
            // if mFinal is inclusive
            r++;
            // else
            // r--;
        }
    }
    return r;

}

bool
TimeRange::IsDegenerate() const
{
    if (!Valid())
        throw cor::Exception("TimeRange::IsDegenerate() -- invalid timerange");
    if (IsInfinite())
        return false;
    return (mFirst == mFinal);
}

bool
TimeRange::IsInfinite() const
{
    if (!Valid())
        throw cor::Exception("TimeRange::IsInfinite() -- invalid timerange");
    return mFirst.IsInfinite() || mFinal.IsInfinite();
}

bool
TimeRange::GoesToPositiveInfinity() const
{
    if (!Valid())
        throw cor::Exception("TimeRange::GoesToPositiveInfinity() const() -- invalid timerange");
    return mFinal.IsPositiveInfinite();
}

bool
TimeRange::GoesToNegativeInfinity() const
{
    if (!Valid())
        throw cor::Exception("TimeRange::GoesToNegativeInfinity() const() -- invalid timerange");
    return mFirst.IsNegativeInfinite();
}

// returns if t is within range
bool
TimeRange::In(const Time& t) const
{
    return (t >= mFirst) && (t <= mFinal);
}

bool
TimeRange::Within(const TimeRange& other) const
{
    return (Intersection(other) == *this);
}

bool
TimeRange::Intersects(const TimeRange& other) const
{
    return Intersection(other);
}

TimeRange
TimeRange::Intersection(const TimeRange& other) const
{
    if (!Valid() || !other.Valid())
        return TimeRange(); // invalid

    // use latest mFirst and earliest mFinal
    Time first = mFirst;
    if (other.mFirst > first)
        first = other.mFirst;

    Time final = mFinal;
    if (other.mFinal < final)
        final = other.mFinal;

    if (final >= first)
        return TimeRange(first, final);
    return TimeRange();
}

TimeRange
TimeRange::Union(const TimeRange& other) const
{
    if (!Intersection(other))
        return TimeRange();

    return SuperUnion(other);
}

TimeRange
TimeRange::SuperUnion(const TimeRange& other) const
{
    // superunion of any range with an invalid range returns the valid one
    if (!Valid() && other.Valid())
        return other;
    if (!other.Valid() && Valid())
        return *this;

    // if neither are valid, return invalid
    if (!other.Valid() && ~Valid())
        return TimeRange();

    // use earliest mFirst and latest mFinal
    Time first = mFirst;
    if (other.mFirst < first)
        first = other.mFirst;

    Time final = mFinal;
    if (other.mFinal > final)
        final = other.mFinal;

    return TimeRange(first, final);
}

void
TimeRange::Intersection(const TimeRangeSet& other,
                        TimeRangeSet& intersection) const
{
    intersection.clear();

    TimeRangeSet::const_iterator i = other.begin();
    for (; i != other.end(); i++)
    {
         cor::TimeRange tr = i->Intersection(*this);
         if (tr.Valid())
             intersection.insert(tr);
    }
}

bool
TimeRange::Follows(const TimeRange& other) const
{
    if (!Valid() || !other.Valid())
        throw cor::Exception("TimeRange::Follows() -- invalid timerange");
    return First() == other.Final();
}

bool
TimeRange::Precedes(const TimeRange& other) const
{
    if (!Valid() || !other.Valid())
        throw cor::Exception("TimeRange::Precedes() -- invalid timerange");
    return other.Follows(*this);
}

bool
TimeRange::IsAdjacent(const TimeRange& other) const
{
    if (!Valid() || !other.Valid())
        throw cor::Exception("TimeRange::IsAdjacent() -- invalid timerange");
    return other.Follows(*this) || this->Follows(other);
}

TimeRange
TimeRange::SuperUnion(const TimeRangeSet& in)
{
    TimeRange r;
    TimeRangeSet::const_iterator i = in.begin();
    if (i != in.end())
        r = *i++;
    for (; i != in.end(); i++)
        r = r.SuperUnion(*i);
    return r;
}

TimeRange
TimeRange::SuperUnion(const std::vector<cor::TimeRange>& in)
{
    TimeRange r;
    std::vector<cor::TimeRange>::const_iterator i = in.begin();
    if (i != in.end())
        r = *i++;
    for (; i != in.end(); i++)
        r = r.SuperUnion(*i);
    return r;
}

void
TimeRange::Union(const std::vector<TimeRange>& in, std::vector<TimeRange>& out)
{
    TimeRangeSet inS;
    for (size_t i = 0; i < in.size(); i++)
        inS.insert(in[i]);
    Union(inS, out);
}

void
TimeRange::Union(const TimeRangeSet& in, std::vector<TimeRange>& out)
{
    out.clear();

    // walk trs and join
    if (!in.empty())
    {
        TimeRangeSet::iterator i = in.begin();
        out.push_back(*i);
        i++;
        for (; i != in.end(); i++)
        {
            if (i->Intersects(out.back()))
            {
                out.back().mFinal = i->mFinal;
            }
            else
            {
                out.push_back(*i);
            }
        }
    }
}

void
TimeRange::Union(const TimeRangeSet& in, TimeRangeSet& out)
{
    std::vector<TimeRange> ov;
    Union(in, ov);

    // wait until here to clear in case in == out
    out.clear();

    // copy vector to set
    for (size_t i = 0; i < ov.size(); i++)
        out.insert(ov[i]);
}

void
TimeRange::Trim(TimeRangeSet& in, const cor::TimeRange& limit)
{
    std::vector<TimeRange> out;
    TimeRangeSet::iterator i = in.begin();
    for (; i != in.end(); i++)
    {
        TimeRange tr = limit.Intersection(*i);
        if (tr.Valid())
            out.push_back(tr);
    }

    in.clear();
    for (size_t i = 0; i < out.size(); i++)
        in.insert(out[i]);

}

void
TimeRange::Remove(const std::vector<TimeRange>& in, std::vector<TimeRange>& out) const
{
    TimeRangeSet trs;

    // fill set, which sorts in ascending start time and removes duplicates
    for (size_t i = 0; i < in.size(); i++)
    {
        trs.insert(in[i]);
    }

    // wait until now in case in == out
    out.clear();

    Time edge = First();

    // walk trs and join
    if (!trs.empty())
    {
        TimeRangeSet::iterator i = trs.begin();
        //out.push_back(*i);
        //i++;
        for (; i != trs.end(); i++)
        {
            TimeRange t = Intersection(*i);
            if (t.Valid())
            {
                // XXX exclusive
                if (t.First() <= edge)
                    edge = t.Final();
                else
                {
                    out.push_back(cor::TimeRange(edge, t.First()));
                    edge = t.Final();
                }
            }
        }
    }
    if (edge < Final())
        out.push_back(cor::TimeRange(edge, Final()));
}

bool
TimeRange::Valid() const
{
    return mFirst.Valid() && mFinal.Valid() && (mFinal >= mFirst);
}

TimeRange&
TimeRange::operator +=(const Time& t)
{
    if (!Valid())
        throw cor::Exception("TimeRange::+=() -- invalid TimeRange");
    if (!t.IsOrdinary())
        throw cor::Exception("TimeRange::+=() -- invalid Time offset");

    if (mFinal.IsOrdinary())
        mFinal += t;
    if (mFirst.IsOrdinary())
        mFirst += t;

    return *this;
}

TimeRange&
TimeRange::operator -=(const Time& t)
{
    if (!Valid())
        throw cor::Exception("TimeRange::-=() -- invalid TimeRange");
    if (!t.IsOrdinary())
        throw cor::Exception("TimeRange::-=() -- invalid Time offset");

    if (mFinal.IsOrdinary())
        mFinal -= t;
    if (mFirst.IsOrdinary())
        mFirst -= t;

    return *this;
}

std::string
TimeRange::Print(bool decimal) const
{
    if (!Valid())
        return "invalid";

    std::string s = mFirst.Print(decimal) + " - " + mFinal.Print(decimal);
    return s;
}

std::string
TimeRange::PrintRaw() const
{
    std::string r;
    if (!Valid())
        r = "invalid ";

    r += mFirst.Print() + " - " + mFinal.Print();
    return r;
}

void
TimeRange::Print(std::ostream& os, bool decimal) const
{
    os << Print(decimal);
}

// return the midpoint
Time
TimeRange::Midpoint() const
{
    return mFirst.Average(mFinal);
}

TimeRange
TimeRange::UtcDayOf(const cor::Time& t)
{
    struct tm bdt = t.ToTm();
    bdt.tm_sec = bdt.tm_min = bdt.tm_hour = 0;
    cor::Time snap(bdt);
    cor::TimeRange tr(snap, snap + cor::Time(86400 - 1));
    return tr;
}

void
TimeRangeSet::Merge(const TimeRangeSet& other)
{
    TimeRangeSet::const_iterator i = other.begin();
    for (; i != other.end(); i++)
    {
        insert(*i);
    }
}

void
TimeRangeSet::Span(const cor::Time &start, const cor::Time &stop, const cor::Time &step)
{
    clear();

    cor::Time t = start;
    for (; t < stop; t += step)
    {
        // XXX inclusive
        insert(cor::TimeRange(t, t + step - cor::Time::OneSecond()));
    }
}

bool
operator >(const TimeRange& t1, const TimeRange& t2)
{
    if (t1.First() == t2.First())
    {
        if (t1.Final() == t2.Final())
        {
            return false; // equal
        }

        return t1.Final() > t2.Final();
    }

    return t1.First() > t2.First();
}

bool
operator >=(const TimeRange& t1, const TimeRange& t2)
{
    if (t1.First() == t2.First())
    {
        return t1.Final() >= t2.Final();
    }

    return t1.First() >= t2.First();
}

bool
operator <(const TimeRange& t1, const TimeRange& t2)
{
    if (t1.First() == t2.First())
    {
        if (t1.Final() == t2.Final())
        {
            return false; // equal
        }

        return t1.Final() < t2.Final();
    }

    return t1.First() < t2.First();
}

bool
operator <=(const TimeRange& t1, const TimeRange& t2)
{
    if (t1.First() == t2.First())
    {
        return t1.Final() <= t2.Final();
    }

    return t1.First() <= t2.First();
}

bool
operator ==(const TimeRange& t1, const TimeRange& t2)
{
    return (t1.First() == t2.First()) && (t1.Final() == t2.Final());
}


bool
operator !=(const TimeRange& t1, const TimeRange& t2)
{
    return !(t1 == t2);
}

bool
operator ==(const TimeRange& tr, const Time& t)
{
    return tr.In(t);
}

bool
operator ==(const Time& t, const TimeRange& tr)
{
    return tr == t;
}

std::ostream&
operator<<(std::ostream& os, const TimeRange& t)
{
    t.Print(os);
    return os;
}

void
Projection(const TimeRangeSet& source,
           const TimeRangeSet& snap,
           TimeRangeSet& projection,
           bool unify)
{
    projection.clear();

    TimeRangeSet::const_iterator i = snap.begin();
    // brute force
    for (; i != snap.end(); i++)
    {
        TimeRangeSet v;
        i->Intersection(source, v);
        TimeRangeSet o;
        if (unify)
            TimeRange::Union(v, o);
        TimeRangeSet::const_iterator k = unify ? o.begin() : v.begin();
        TimeRangeSet::const_iterator end = unify ? o.end() : v.end();
        for (; k != end; k++)
            projection.insert(*k);
    }
}

std::string
Print(const TimeRangeSet& trv)
{
    std::ostringstream oss;
    TimeRangeSet::const_iterator i = trv.begin();
    for (; i != trv.end(); i++)
        oss << i->Print() << std::endl;
    return oss.str();
}

    
}
