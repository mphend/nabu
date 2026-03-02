//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <mcor/json_time.h>

#include "leak_detector.h"
#include "wcm.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {


/** class WrittenColumnsMap implementation
 *
 */

WrittenColumnsMap::WrittenColumnsMap()
{
    LeakDetector::Get().Increment("WrittenColumnsMap");
}

WrittenColumnsMap::~WrittenColumnsMap()
{
    LeakDetector::Get().Decrement("WrittenColumnsMap");
}

std::string
WrittenColumnsMap::Print() const
{
    std::stringstream ss;
    const_iterator i = begin();
    for (; i != end(); i++)
    {
        cor::TimeRangeSet::const_iterator j = i->second.begin();
        std::cout << i->first.GetLiteral() << " :" << std::endl;
        for (; j != i->second.end(); j++)
        {
            ss << "    " << j->Print() << std::endl;
        }
    }

    return ss.str();
}

void
WrittenColumnsMap::Merge()
{
    const_iterator i = begin();
    for (; i != end(); i++)
    {
        // this might add a new, empty member
        cor::TimeRangeSet &value = (*this)[i->first];
        // merge them into a minimal set of spanning timeranges
        cor::TimeRange::Union(value, value);
    }
}

void
WrittenColumnsMap::Merge(const WrittenColumnsMap &other)
{
    const_iterator i = other.begin();
    for (; i != other.end(); i++)
    {
        // this might add a new, empty member
        cor::TimeRangeSet &value = (*this)[i->first];

        // copy all timeranges in other's column into this
        value.insert(i->second.begin(), i->second.end());

        // merge them into a minimal set of spanning timeranges
        cor::TimeRange::Union(value, value);
    }
}

void
WrittenColumnsMap::Merge(const WrittenColumnsMap &other,
                         const cor::TimeRange &limit,
                         const std::set<MetaKey> &columnSet)
{
    const_iterator i = other.begin();
    for (; i != other.end(); i++)
    {
        if (!columnSet.empty() &&
            (columnSet.find(i->first) == columnSet.end()))
            continue; // not a column of interest, ignore it

        // this might add a new, empty member
        cor::TimeRangeSet &value = (*this)[i->first];

        // copy all timeranges in other's column into this
        value.insert(i->second.begin(), i->second.end());

        // merge them into a minimal set of spanning timeranges
        cor::TimeRange::Union(value, value);
        cor::TimeRange::Trim(value, limit);
    }
    //printf("Merged %ld from other (%ld) into this, limited by %s\n", size(), other.size(), limit.Print().c_str());
}

void
WrittenColumnsMap::Intersect(const std::set<MetaKey>& columnSet)
{
    iterator i = begin();
    for (; i != end(); i++)
    {
        if (columnSet.find(i->first) == columnSet.end())
        {
            // avoid requiring C++11, etc.
            iterator ic = i++;
            erase(ic);
        }
    }
}

void
WrittenColumnsMap::Set(const std::set<MetaKey> &columnSet, const cor::TimeRange &tr)
{
    clear();
    std::set<MetaKey>::const_iterator i = columnSet.begin();
    for (; i != columnSet.end(); i++)
    {
        (*this)[*i].insert(tr);
    }
}

cor::TimeRange
WrittenColumnsMap::Extents() const
{
    cor::TimeRange t;

    const_iterator i = begin();
    if (i != end())
        t = cor::TimeRange::SuperUnion(i++->second);

    for (; i != end(); i++)
        t = t.SuperUnion(cor::TimeRange::SuperUnion(i->second));

    return t;
}

void
WrittenColumnsMap::GetKeys(std::vector<nabu::MetaKey>& v) const
{
    v.clear();
    v.resize(size());
    const_iterator i = begin();
    for (size_t j = 0; i != end(); i++, j++)
        v[j] = i->first;
}

void
WrittenColumnsMap::Parse(const Json::Value& v)
{
    // DEFENSIVE; check type
    if (!v.isObject())
        throw cor::Exception("WrittenColumnsMap::Parse() -- JSON is not an Object");

    clear();
    Json::ValueConstIterator i = v.begin();
    for (; i != v.end(); i++)
    {
        // DEFENSIVE; check types
        if (!i.key().isString())
            throw cor::Exception("WrittenColumnsMap::Parse() -- key '%s' in JSON is not a string",
                                 i.key().asString().c_str());
        if (!(*i).isArray())
            throw cor::Exception("WrittenColumnsMap::Parse() -- value for '%s' in JSON is not an array",
                                 i.key().asString().c_str());

        MetaKey key = i.key().asString();
        Json::ValueConstIterator j = (*i).begin();
        for (; j != (*i).end(); j++)
        {
            cor::TimeRange tr = Json::TimeRangeFromJson(*j);
            (*this)[key].insert(tr);
        }
    }
}

void
WrittenColumnsMap::Render(Json::Value& v) const
{
    // iterate all columns and insert into JSON object
    const_iterator i = begin();
    for (; i != end(); i++)
    {
        // iterate all timeranges in set and insert each into a JSON array
        cor::TimeRangeSet::const_iterator j = i->second.begin();
        int n = 0;
        for (; j != i->second.end(); j++, n++)
        {
            Json::TimeRangeToJson(*j, v[i->first.GetLiteral()][n]);
        }
    }
}
    
}
