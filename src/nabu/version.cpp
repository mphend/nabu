//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <stdio.h>
#include <string.h>
#include <sstream>

#include <openssl/md5.h>

#include <mcor/binary.h>
#include <mcor/mexception.h>
#include <mcor/strutil.h>
#include <mcor/muuid.h>

#include "version.h"


namespace nabu
{

Version::~Version()
{}

uint64_t
Version::ToNumber() const
{
    // XXX uuid's are 128 bits (16 bytes), not 64
    uint64_t r = cor::Unpack64(mNumber.data().data());
    return r;
}

void
Version::AsNumber(uint64_t n)
{
    uuid_t raw;
    memset(raw, 0, 16);
    // XXX 128 bits
    cor::Pack64(raw, n);

    mNumber.set(raw);
}

void
Version::RenderToJson(Json::Value& v) const
{
    v["number"] = mNumber.string();
}

void
Version::ParseFromJson(const Json::Value& v)
{
    mNumber.set(v["number"].asString());
}

void
Version::MD5_Update(MD5_CTX& ctx) const
{
    ::MD5_Update(&ctx, mNumber.data().data(), mNumber.data().size());
}

Version
Version::NextNumber() const
{
    Version v = *this;
    v.AsNumber(v.ToNumber()+1);
    return v;
}

Version
Version::New() const
{
    Version v = *this;
    v.mNumber = muuid::uuid::random();
    return v;
}

std::string
Version::GetLiteral() const
{
    std::ostringstream oss;
    oss << mNumber.string();
    return oss.str();
}

void
Version::Parse(const std::string& s)
{
    mNumber.set(s);
}


bool
Version::gt(const Version& other) const
{
    return mNumber > other.mNumber;
}

bool
Version::ge(const Version& other) const
{
    return mNumber >= other.mNumber;
}

bool
Version::lt(const Version& other) const
{
    return mNumber < other.mNumber;
}

bool
Version::le(const Version& other) const
{
    return mNumber <= other.mNumber;
}

bool
Version::eq(const Version& other) const
{
    return mNumber == other.mNumber;
    return false;
}

bool
Version::ne(const Version& other) const
{
    return !eq(other);
}

bool operator>(const Version& v1, const Version& v2)  { return v1.gt(v2); }
bool operator>=(const Version& v1, const Version& v2) { return v1.ge(v2); }
bool operator<(const Version& v1, const Version& v2)  { return v1.lt(v2); }
bool operator<=(const Version& v1, const Version& v2) { return v1.le(v2); }
bool operator==(const Version& v1, const Version& v2) { return v1.eq(v2); }
bool operator!=(const Version& v1, const Version& v2) { return v1.ne(v2); }

std::ostream& operator<<(std::ostream& os, const Version& v) { return os << v.GetLiteral(); }

} //  end namespace
