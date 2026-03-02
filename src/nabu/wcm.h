//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_WCM__
#define __PKG_NABU_WCM__


#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <mcor/timerange.h>
#include <mcor/muuid.h>

#include <nabu/interface_types.h>
#include <nabu/metakey.h>


namespace nabu
{

class IOOperation;


/** class WrittenColumnsMap : publishes the time ranges that have been
 *  written to each column
 */
class WrittenColumnsMap : public std::map<MetaKey, cor::TimeRangeSet>
{
public:
    WrittenColumnsMap();
    ~WrittenColumnsMap();

    // integrate time ranges
    void Merge();
    // add columns and integrate time ranges
    void Merge(const WrittenColumnsMap& other);
    // add columns and integrate time ranges, and limit the contents of this
    // object to not extend outside of limit or include keys not in columnSet
    // If columnSet is empty, no filtering based on column name is done.
    void Merge(const WrittenColumnsMap& other,
               const cor::TimeRange& limit,
               const std::set<MetaKey>& columnSet);

    // Remove columns in this that are not in columnSet
    void Intersect(const std::set<MetaKey>& columnSet);

    // set the map to the set of columns all with the given time range
    void Set(const std::set<MetaKey>& columnSet, const cor::TimeRange& tr);

    std::string Print() const;

    // return the total time extents
    cor::TimeRange Extents() const;

    // return the keys of the map
    void GetKeys(std::vector<nabu::MetaKey>&) const;

    // construct from JSON
    void Parse(const Json::Value& v);
    // render to JSON
    void Render(Json::Value& v) const;
};


}

#endif
