//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef __PKG_NABU_RECORD__
#define __PKG_NABU_RECORD__

#include <memory>
#include <vector>

#include <json/json.h>
#include <mcor/mfile.h>
#include <mcor/timerange.h>

#include <mcor/muuid.h>

#include "metakey.h"


namespace nabu
{

/** class Record : a time-stamped block of opaque binary data
 *
 */

struct Record
{
    Record();
    Record(const Record& other);
    Record(const cor::Time& time, size_t size);
    Record(const cor::Time& time, const std::vector<unsigned char>& data);
    Record(const cor::Time& time, unsigned char* data, size_t size);
    Record(const cor::TimeRange& timeRange, size_t size);
    Record(const cor::TimeRange& timeRange, const std::vector<unsigned char>& data);
    Record(const cor::TimeRange& timeRange, unsigned char* data, size_t size);

    ~Record();

    // Right now, Record has data members for all record types: point-in-time,
    // or time range, searchable or non-searchable. The specific record type
    // is used to render to and from disk, so disk space is minimized, but
    // to avoid C++ type proliferation the union-of-all approach to member data
    // is used. Some day, if this causes undesired memory waste, some dynamic
    // cast scheme could be used to slim things down.
    cor::TimeRange mTimeRange; // first == final for point-in-time records
    Json::Value mDataObject;   // null object for non-searchable records
    std::shared_ptr<std::vector<unsigned char>> mData;

    void SetTime(const cor::Time& t);
    void SetTimeRange(const cor::TimeRange& timeRange);

    const cor::Time& GetTime() const { return mTimeRange.First(); }

    std::string PrintTime(bool decimal = false) const;

    // returns true if everything except time is identical, false otherwise
    bool HasIdenticalPayload(const Record& other) const;

    bool Valid() const { return mTimeRange.Valid(); }

    // ==== convenience methods for common, generic payload types

    // convert payload to string
    std::string ToString() const;
    // create payload from string
    void FromString(const std::string& s);

    // convert payload to JSON
    void ToJson(Json::Value& v) const;
    // create payload from JSON
    void FromJson(const Json::Value& v);
};

bool operator==(const Record& r1, const Record& r2);
bool operator!=(const Record& r1, const Record& r2);


/** class RecordVector : a vector of Records
 *
 */
class RecordVector : public std::vector<Record>
{
public:
    RecordVector();
    RecordVector(const RecordVector& other);
    RecordVector(size_t size);
    virtual ~RecordVector();

    void Append(const RecordVector& other);

    // print the vector, optionally including the (assumed) string-type payload
    std::string Print(bool asStringPayload = false) const;

    // the total size, in bytes, of the data of all records
    size_t TotalSize() const;

    // tests for overlap of records, throws if found
    void TestNonOverlap() const;

    // tests for out-of-order records, throws if found
    void TestOutOfOrder(const std::string& where) const;

    // extract just the time ranges for the records
    void GetTimeRanges(cor::TimeRangeSet& timeRangeSet) const;

    // create dataless entries from time ranges
    void FromTimeRanges(const cor::TimeRangeSet& timeRangeSet);
};


/** class RecordMap : a map of RecordVectors keyed on MetaKey
 *
 */
class RecordMap : public std::map<MetaKey, RecordVector>
{
public:
    RecordMap();
    RecordMap(const RecordMap& other);
    virtual ~RecordMap();

    // compute the total time range extents of all data in the map
    cor::TimeRange Extents() const;

    // print the entire contents
    void Print() const;
    // print the size and time range of each entry
    void PrintSummary() const;

    // returns true if this contains any records (that at least one
    // key exists that holds a non-empty RecordVector)
    bool ContentsEmpty() const;

};

typedef std::map<nabu::MetaKey, cor::TimeRange> RangeMap;

}

#endif
