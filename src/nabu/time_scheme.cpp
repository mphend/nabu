//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <cstdarg>
#include <stdio.h>
#include <sstream>

#include "time_scheme.h"

#define MAX_ATTOSECONDS_PER_SECOND  (1000000000000000000ll - 1)

namespace nabu
{
UtcTimeScheme::UtcTimeScheme(int count, ...)
{
    va_list args;
    va_start(args, count);

    std::vector<unsigned int> periods(count);
    for (size_t i = 0; i < count; i++)
    {
        periods[i] = va_arg(args, unsigned int);
    }
    SetPeriods(periods);

    va_end(args);
}

UtcTimeScheme::UtcTimeScheme(const std::vector<unsigned int>& periods)
{
    SetPeriods(periods);
}

UtcTimeScheme::~UtcTimeScheme()
{}

void
UtcTimeScheme::SetPeriods(const std::vector<unsigned int> &periods)
{
    // DEFENSIVE
    if (periods.empty())
        throw cor::Exception("UtcTimeScheme -- empty");

    // DEFENSIVE
    for (size_t i = 0; i < periods.size(); i++)
    {
        if (periods[i] == 0)
            throw cor::Exception("UtcTimeScheme -- period of 0");
    }

    mPeriods = periods;
}

nabu::TimePath
UtcTimeScheme::Origin() const
{
    nabu::TimePath p(mPeriods.size());
    for (size_t i = 0; i < mPeriods.size(); i++)
        p[i] = 0;
    return p;
}

nabu::TimePath
UtcTimeScheme::Termination() const
{
    nabu::TimePath p(mPeriods.size());
    for (size_t i = 0; i < mPeriods.size(); i++)
        p[i] = mPeriods[i] - 1;
    p.back() = ~uint32_t();

    return p;
}

nabu::TimePath
UtcTimeScheme::GetPath(const cor::Time& t) const
{
    if (t.IsNegativeInfinite())
    {
        return Origin();
    }
    else if (t.IsPositiveInfinite())
    {
        return Termination();
    }

    // DEFENSIVE
    if (t.IsInfinite())
        throw cor::Exception("UtcTimeScheme::GetPath() -- arg is infinite");

    nabu::TimePath p(mPeriods.size());

    time_t seconds = t.SecondPortion();

    int period = 1;
    for (size_t i = 0; i < mPeriods.size(); i++)
        period *= mPeriods[i];

    for (int i = mPeriods.size() - 1; i >= 0; i--)
    {
        int chunk = seconds / period;
        seconds -= chunk * period;
        period /= mPeriods[i];
        p[i] = chunk;
    }
    p.mRemainder = seconds;

    return p;
}

cor::TimeRange
UtcTimeScheme::GetTime(const nabu::TimePath& timePath) const
{
    if (!timePath.Valid())
        throw cor::Exception("UtcTimeScheme::GetTime() -- invalid timePath");

    int period = 1;
    for (size_t i = 0; i < timePath.size(); i++)
        period *= mPeriods[i];

    // period is the size of the current digit
    time_t seconds = 0;
    int i = timePath.size() - 1;
    while (i >= 0)
    {
        seconds += timePath[i] * period;
        period /= mPeriods[i];
        i--;
    }

    return cor::TimeRange(cor::Time(seconds), cor::Time(seconds + mPeriods[0] - 1, MAX_ATTOSECONDS_PER_SECOND));
}

void
UtcTimeScheme::GetPaths(std::vector<TimePath>& paths, const cor::TimeRange& timeRange) const
{
    if (!timeRange.Valid() || timeRange.IsInfinite() || timeRange.Length() == cor::Time(0))
        return;

    TimePath i = GetPath(timeRange.First());
    TimePath end = GetPath(timeRange.Final());

    paths.push_back(i);

    while (i != end)
    {
        size_t k = 0;
        while (k < mPeriods.size())
        {
            i[k]++;

            // rollover
            // 'periods' describes grouping of data, starting with the number
            // of records in a data file (mPeriods[0]). Since this is iterating
            // paths, not data, this lowest period is ignored. In other words,
            // this iterates file names, not records in files.
            if ((k != mPeriods.size() - 1) && (i[k] == mPeriods[k + 1]))
            {
                i[k] = 0;
                k++;
            }
            else
                break;
        }

        //printf("%s\n", i.GetLiteral().c_str());
        paths.push_back(i);
    }

}

cor::TimeRange
UtcTimeScheme::Snap(const cor::TimeRange& in) const
{
    return cor::TimeRange(SnapFirst(in.First()), SnapFinal(in.Final()));
}

cor::Time
UtcTimeScheme::SnapFirst(const cor::Time& in) const
{
    if (in.IsInfinite())
        return in;

    nabu::TimePath p1 = GetPath(in);
    return in.Seconds() - cor::Time(p1.mRemainder);
}

cor::Time
UtcTimeScheme::SnapFinal(const cor::Time& in) const
{
    if (in.IsInfinite())
        return in;

    nabu::TimePath p2 = GetPath(in);
    // XXX exclusive
    return in.Seconds() + cor::Time(mPeriods[0] - p2.mRemainder - 1, MAX_ATTOSECONDS_PER_SECOND);
}

cor::TimeRange
UtcTimeScheme::SnapDown(const cor::TimeRange& in) const
{
    if (in.IsInfinite())
        return in;

    cor::Time t1 = SnapFirst(in.First());
    cor::Time t2 = GetTime(GetPath(in.Final())).First();
    // because t1 is inclusive and t2 is exclusive, they can't be the
    // same moment
    if (t1 == t2)
        return cor::TimeRange();
    return cor::TimeRange(t1, t2);
}

size_t
UtcTimeScheme::RootLayer() const
{
    return LayerOf(TableAddress());
}

size_t
UtcTimeScheme::LayerOf(const TableAddress& tableAddress) const
{
    size_t r = mPeriods.size() - tableAddress.GetTimePath().size();
    // add one for root layer (above columns)
    if (tableAddress.GetColumn().IsUnknown())
    {
        r += 1;
    }
    return r;
}

std::vector<cor::TimeRange>
UtcTimeScheme::GetTimeRange(const cor::Time& t) const
{
    // DEFENSIVE
    if (t.IsInfinite())
        throw cor::Exception("UtcTimeScheme::GetTimeRange() -- arg is infinite");

    time_t seconds = t.SecondPortion();

    std::vector<cor::TimeRange> r(mPeriods.size());

    int period = 1;
    for (size_t i = 0; i < mPeriods.size(); i++)
        period *= mPeriods[i];

    //for (size_t i = 0; i < mPeriods.size(); i++)
    for (int i = mPeriods.size() - 1; i >= 0; i--)
    {
        int chunk = seconds / period;
        cor::Time tFirst = cor::Time(chunk * period);
        // XXX exclusive
        cor::Time tFinal = tFirst + cor::Time(period - 1, MAX_ATTOSECONDS_PER_SECOND);
        r[i] = cor::TimeRange(tFirst, tFinal);

        seconds -= tFirst.SecondPortion();
        period /= mPeriods[i];
    }

    return r;
}

void
UtcTimeScheme::SetToLast(TimePath& tp, size_t layer) const
{
    if (layer == mPeriods.size())
        return; // do nothing

    //printf("SetToLast: tp = %s, layer = %ld\n", tp.GetLiteral().c_str(), layer);
    if (layer >= mPeriods.size())
        throw cor::Exception("UtcTimeScheme::SetToLast() -- layer %ld >= size %ld\n", layer, mPeriods.size());

    // XXX exclusive
    for (size_t i = 0; i <= layer; i++)
        tp[i] = mPeriods[i] - 1;
    //printf("SetToLast -- tp = %s\n", tp.GetLiteral().c_str());
}

bool
UtcTimeScheme::InSameLeaf(const cor::Time& t1, const cor::Time& t2) const
{
    return GetPath(t1) == GetPath(t2);
}

void
UtcTimeScheme::FromJson(const Json::Value& v)
{
    if (!v.isArray())
        throw cor::Exception("UtcTimeScheme::FromJson() -- not an Array");
    std::vector<unsigned int> periods;
    Json::ValueConstIterator i = v.begin();
    for(; i != v.end(); i++)
    {
        if (!(*i).isInt())
        {
            throw cor::Exception("UtcTimeScheme::FromJson() -- array element is not an unsigned integer");
        }
        periods.push_back((*i).asInt());
    }
    SetPeriods(periods);
}

void
UtcTimeScheme::ToJson(Json::Value& v) const
{
    for (int i = 0; i < mPeriods.size(); i++)
    {
        v[i] = mPeriods[i];
    }
}

std::string
UtcTimeScheme::Print() const
{
    std::ostringstream oss;
    for (size_t i = 0; i < mPeriods.size(); i++)
    {
        oss << "[" << mPeriods[i] << "]";
    }
    return oss.str();
}


} //  end namespace
