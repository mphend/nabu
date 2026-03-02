//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <string.h>
#include <stdio.h>

#include <openssl/md5.h>

#include <mcor/binary.h>
#include <json/json.h>
#include <json/writer.h>

#include "exception.h"
#include "leak_detector.h"
#include "record.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {

const uint16_t currentVersion = 2;

Record::Record() :
    mTimeRange(cor::Time::eInvalid, cor::Time::eInvalid)
{
    LeakDetector::Get().Increment("Record");
    mData.reset(new std::vector<unsigned char>);
}

Record::Record(const Record& other) :
    mTimeRange(other.mTimeRange),
    mDataObject(other.mDataObject),
    mData(other.mData)
{
    LeakDetector::Get().Increment("Record");
}

Record::Record(const cor::Time &time, size_t size) :\
    mTimeRange(time, time)
{
    LeakDetector::Get().Increment("Record");
    mData.reset(new std::vector<unsigned char>);
    mData->resize(size);
}

Record::Record(const cor::Time &time, const std::vector<unsigned char> &data) :
    mTimeRange(time, time)
{
    LeakDetector::Get().Increment("Record");
    mData.reset(new std::vector<unsigned char>);
    mData->resize(data.size());
    for (size_t i = 0; i < mData->size(); i++)
        (*mData)[i] = data[i];
}

Record::Record(const cor::Time &time, unsigned char *data, size_t size) :
    mTimeRange(time, time)
{
    LeakDetector::Get().Increment("Record");
    mData.reset(new std::vector<unsigned char>(size));
    for (size_t i = 0; i < size; i++)
        (*mData)[i] = data[i];
}

Record::Record(const cor::TimeRange &timeRange, size_t size) :
    mTimeRange(timeRange)
{
    LeakDetector::Get().Increment("Record");
    mData.reset(new std::vector<unsigned char>);
    mData->resize(size);
}

Record::Record(const cor::TimeRange &timeRange, const std::vector<unsigned char> &data) :
    mTimeRange(timeRange)
{
    LeakDetector::Get().Increment("Record");
    mData.reset(new std::vector<unsigned char>);
    mData->resize(data.size());
    for (size_t i = 0; i < mData->size(); i++)
        (*mData)[i] = data[i];
}

Record::Record(const cor::TimeRange &timeRange, unsigned char *data, size_t size) :
    mTimeRange(timeRange)
{
    LeakDetector::Get().Increment("Record");
    mData.reset(new std::vector<unsigned char>(size));
    for (size_t i = 0; i < size; i++)
        (*mData)[i] = data[i];
}

Record::~Record()
{
    LeakDetector::Get().Decrement("Record");
}

void
Record::SetTime(const cor::Time& t)
{
    mTimeRange.First() = t;
    mTimeRange.Final() = t;
}

void
Record::SetTimeRange(const cor::TimeRange& timeRange)
{
    mTimeRange = timeRange;
}

std::string
Record::PrintTime(bool decimal) const
{
    if (mTimeRange.IsDegenerate())
        return mTimeRange.First().Print(decimal);
    else
        return mTimeRange.Print(decimal);
}

bool
Record::HasIdenticalPayload(const Record& other) const
{
    if (mDataObject != other.mDataObject)
        return false;
    return (*mData == *other.mData);
}

std::string
Record::ToString() const
{
    std::string s;
    for (size_t i = 0; i < mData->size(); i++)
        s.push_back((*mData)[i]);
    return s;
}

void
Record::FromString(const std::string &s)
{
    mData->resize(s.size());
    for (size_t i = 0; i < mData->size(); i++)
        (*mData)[i] = s[i];
}

// convert payload to JSON
void
Record::ToJson(Json::Value &v) const
{
    Json::Reader reader;
    if (!reader.parse(reinterpret_cast<const char *>(mData->data()),
                      reinterpret_cast<const char *>(mData->data() + mData->size()),
                      v))
    {
        throw cor::Exception("Payload of record could not be interpreted as JSON: %s\n",
                             reader.getFormattedErrorMessages().c_str());
    }
}

// create payload from JSON
void
Record::FromJson(const Json::Value &v)
{
    Json::FastWriter writer;
    FromString(writer.write(v));
}

bool
operator==(const Record& r1, const Record& r2)
{
    if (r1.mTimeRange != r2.mTimeRange)
        return false;
    return r1.HasIdenticalPayload(r2);
}

bool
operator!=(const Record& r1, const Record& r2)
{
    return !(r1 == r2);
}


/** RecordVector implementation
 *
 */

RecordVector::RecordVector() : std::vector<Record>()
{
    LeakDetector::Get().Increment("RecordVector");
}

RecordVector::RecordVector(const RecordVector& other) :
    std::vector<Record>(other)
{
    LeakDetector::Get().Increment("RecordVector");
}

RecordVector::RecordVector(size_t size) : std::vector<Record>(size)
{
    LeakDetector::Get().Increment("RecordVector");
}

RecordVector::~RecordVector()
{
    LeakDetector::Get().Decrement("RecordVector");
}

void
RecordVector::Append(const nabu::RecordVector &other)
{
    size_t oSize = size();
    resize(oSize + other.size());

    for (size_t i = oSize, j = 0; i < oSize + other.size(); i++, j++)
    {
        (*this)[i] = other[j];
    }
}

std::string
RecordVector::Print(bool asStringPayload) const
{
    std::ostringstream oss;
    std::string r;
    for (size_t i = 0; i < size(); i++)
    {
        // time
        if ((*this)[i].GetTime().Valid())
            oss << (*this)[i].PrintTime(true) << std::endl;
        else
            oss << "Record " << i << " has invalid time!" << std::endl;

        // searchable JSON
        if (!(*this)[i].mDataObject.isNull())
        {
            Json::StyledStreamWriter writer;
            writer.write(oss, (*this)[i].mDataObject);
        }

        // payload
        if (asStringPayload)
        {
            if ((*this)[i].GetTime().Valid())
                oss << (*this)[i].PrintTime(true) << ": " << (*this)[i].ToString() << std::endl;
            else
                oss << "Record " << i << " has invalid time!" << std::endl;
        }
    }

    return oss.str();
}

size_t
RecordVector::TotalSize() const
{
    size_t total = 0;
    for (size_t i = 0; i < size(); i++)
        total += (*this)[i].mData->size();
    return total;
}

void
RecordVector::TestNonOverlap() const
{
    size_t last;
    cor::TimeRange tr; // invalid
    for (size_t i = 0; i < size(); i++)
    {
        const Record& r = (*this)[i];
        if (!tr.Valid())
        {
            tr = r.mTimeRange;
            last = i;
        }
        else
        {
            if (tr.Final() >= r.mTimeRange.First()) // overlap
            {
                throw cor::Exception("record %ld (%s) overlaps prior record %ld (%s)",
                                     i, r.mTimeRange.Print().c_str(),
                                     last, tr.Print().c_str());
            }
            tr = r.mTimeRange;
            last = i;
        }
    }
}

void RecordVector::TestOutOfOrder(const std::string& where) const
{
    cor::Time guard = cor::Time::eInvalid;
    for (size_t i = 0; i < size(); i++)
    {
        if (guard.Valid())
        {
            if ((*this)[i].GetTime() <= guard)
                throw cor::Exception("At %s: record %ld (%s) <= %s",
                    where.c_str(),
                    i,
                    (*this)[i].GetTime().Print(true).c_str(),
                    guard.Print(true).c_str());
        }
        guard = (*this)[i].GetTime();
    }
}

void
RecordVector::GetTimeRanges(cor::TimeRangeSet& timeRangeSet) const
{
    for (size_t i = 0; i < size(); i++)
    {
        timeRangeSet.insert((*this)[i].mTimeRange);
    }
}

void
RecordVector::FromTimeRanges(const cor::TimeRangeSet& timeRangeSet)
{
    resize(timeRangeSet.size());
    cor::TimeRangeSet::const_iterator i = timeRangeSet.begin();
    for (size_t j = 0; i != timeRangeSet.end(); i++, j++)
        (*this)[j] = Record(*i, 0);
}

RecordMap::RecordMap()
{
    LeakDetector::Get().Increment("RecordMap");
}

RecordMap::RecordMap(const RecordMap& other) :
    std::map<MetaKey, RecordVector>(other)
{
    LeakDetector::Get().Increment("RecordMap");
}

RecordMap::~RecordMap()
{
    LeakDetector::Get().Decrement("RecordMap");
}

cor::TimeRange
RecordMap::Extents() const
{
    cor::TimeRange tr;
    const_iterator i = begin();
    for (; i != end(); i++)
    {
        if (!i->second.empty())
        {
            cor::TimeRange ctr = cor::TimeRange(i->second.front().GetTime(), i->second.back().GetTime());
            if (!tr.Valid())
                tr = ctr;
            else
                tr = tr.Union(ctr);
        }
    }
    return tr;
}

void
RecordMap::Print() const
{
    const_iterator i = begin();
    for (; i != end(); i++)
    {
        printf("%s\n", i->first.GetLiteral().c_str());
        i->second.Print();
    }
}

void
RecordMap::PrintSummary() const
{
    const_iterator i = begin();
    for (; i != end(); i++)
    {
        printf("%s ", i->first.GetLiteral().c_str());
        if (i->second.empty())
            printf("(empty)\n");
        else
        {
            cor::TimeRange tr(i->second.front().mTimeRange.First(),
                              i->second.back().mTimeRange.Final());
            printf(" [%ld] - %s\n", i->second.size(), tr.Print().c_str());
        }
    }
}

bool
RecordMap::ContentsEmpty() const
{
    const_iterator i = begin();
    for (; i != end(); i++)
        if (!i->second.empty())
            return false;

    return true;
}

}
