//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_VERSION_H
#define GICM_NABU_VERSION_H

#include <json/json.h>
#include <mcor/muuid.h>

#include "metakey.h"

// use for debugging to get incrementing version numbers
//#define USE_INCREMENTING_NUMBERS

class MD5state_st;

namespace nabu
{

struct Version
{
    Version() {}
    Version(muuid::uuid v) : mNumber(v) {}

    ~Version();

    muuid::uuid mNumber;

    uint64_t ToNumber() const;
    void AsNumber(uint64_t n);

    void RenderToJson(Json::Value& v) const;
    void ParseFromJson(const Json::Value& v);

    void MD5_Update(struct MD5state_st& ctx) const;

    bool Valid() const { return !mNumber.is_nil(); }
    bool Invalid() const { return !Valid(); }

    // return the next version
    Version NextNumber() const;

    // create a new version with same branch
    Version New() const;

    // convert to string
    std::string GetLiteral() const;
    void Parse(const std::string& s);

    bool gt(const Version& other) const;
    bool ge(const Version& other) const;
    bool lt(const Version& other) const;
    bool le(const Version& other) const;
    bool eq(const Version& other) const;
    bool ne(const Version& other) const;
};

bool operator>(const Version& v1, const Version& v2);
bool operator>=(const Version& v1, const Version& v2);
bool operator<(const Version& v1, const Version& v2);
bool operator<=(const Version& v1, const Version& v2);
bool operator==(const Version& v1, const Version& v2);
bool operator!=(const Version& v1, const Version& v2);

std::ostream& operator<<(std::ostream& os, const Version& v);

} // end namespace

#endif
