//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_TIME_PATH_H
#define GICM_NABU_TIME_PATH_H

#include <json/json.h>

#include "metakey.h"

namespace nabu
{


/** Class TimePath : a path into metadata tree
 *
 *  begin() is leaf  (layer 0)
 *  end() is root-1  (layer N)
 *
 */
class TimePath : public std::vector<Chunk>
{
public:
    TimePath() : mRemainder(0) {}
    TimePath(size_t size) : std::vector<Chunk>(size), mRemainder(0) {}
    virtual ~TimePath() {}

    bool Valid() const { return !empty(); }
    operator bool() const { return Valid(); }

    void RenderToJson(Json::Value& v) const;
    void ParseFromJson(const Json::Value& v);

    uint32_t mRemainder;

    std::string GetLiteral(char delimiter = '_') const;
    void Parse(const std::string& s, char delimiter = '_');

    TimePath operator+(unsigned int) const;

    // return path down from root to given layer
    // So, for path of <root>:3:2:1:0
    //   DownTo(2) => 3:2
    //   DownTo(1) => 2:3:1
    TimePath RootDownTo(size_t layer) const;

    // zero given layer and all past it
    // ex: 1 2 3
    //     ZeroOut(1)
    //     1 0 0
    //
    // returns the timepath up to that layer, e.g.,
    //     ZeroOut(0)
    //     1 2
    TimePath ZeroOut(size_t layer);

    void Reverse();
};

bool operator >(const TimePath& t1, const TimePath& t2);
bool operator >=(const TimePath& t1, const TimePath& t2);
bool operator <(const TimePath& t1, const TimePath& t2);
bool operator <=(const TimePath& t1, const TimePath& t2);
bool operator ==(const TimePath& t1, const TimePath& t2);
bool operator !=(const TimePath& t1, const TimePath& t2);


TimePath operator+(const MetaKey& key, const TimePath& path);

std::ostream& operator <<(std::ostream& os, const TimePath& p);


} // end namespace

#endif
