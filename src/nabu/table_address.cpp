//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <xxhash.h>

#include "exception.h"
#include "table_address.h"


namespace nabu {

TableAddress::TableAddress() : mHash(0)
{
}

TableAddress::TableAddress(const MetaKey& column, const TimePath& path, const Version& version) :
    mColumn(column),
    mTimePath(path),
    mVersion(version),
    mHash(0)
{
}

TableAddress::TableAddress(const TableAddress& other) :
    mColumn(other.mColumn),
    mTimePath(other.mTimePath),
    mVersion(other.mVersion),
    mHash(other.mHash)
{
}

TableAddress::~TableAddress()
{}

std::string
TableAddress::GetLiteral() const
{
    return mColumn.GetLiteral() + ":" + mTimePath.GetLiteral() + ", ver " + mVersion.GetLiteral();
}

bool
TableAddress::IsRoot() const
{
    if (mTimePath.Valid())
        return false;

    if (mColumn.IsUnknown() || (mColumn.IsAddress() && mColumn.Empty()))
        return true;

    return false;
}

void
TableAddress::RenderToJson(Json::Value& v) const
{
    v["column"] = mColumn.GetLiteral();
    mTimePath.RenderToJson(v["timepath"]);
    mVersion.RenderToJson(v["version"]);
}

void
TableAddress::ParseFromJson(const Json::Value& v)
{
    mColumn.Parse(v["column"].asString(), '/');
    mTimePath.ParseFromJson(v["timepath"]);
    mVersion.ParseFromJson(v["version"]);
    Rehash();
}

/*bool
TableAddress::IsBrandNewRoot() const
{
    return ((mVersion.mBranch == "main") && (mVersion.mNumber.is_nil()));
}*/

uint64_t
TableAddress::GetHash() const
{
    if (mHash != 0)
        return mHash;

    std::string s = mColumn.GetLiteral()  + mTimePath.GetLiteral() + mVersion.GetLiteral();
    XXH64_hash_t h = XXH64(s.data(), s.length(), 0);
    mHash = h;
    return mHash;
}

void
TableAddress::SetColumn(const MetaKey& m)
{
    mColumn = m;
    Rehash();
}

void
TableAddress::SetTimePath(const TimePath& tp)
{
    mTimePath = tp;
    Rehash();
}

void
TableAddress::SetVersion(const Version& v)
{
    mVersion = v;
    Rehash();
}

void
TableAddress::NewVersion()
{
    mVersion = mVersion.New();
    Rehash();
}

void
TableAddress::Rehash() const
{
    mHash = 0;
}

#define USE_HASH

#ifdef USE_HASH
bool
operator >(const TableAddress& t1, const TableAddress& t2)
{
    return t1.GetHash() > t2.GetHash();
}

bool
operator >=(const TableAddress& t1, const TableAddress& t2)
{
    return t1.GetHash() >= t2.GetHash();
}

bool
operator <(const TableAddress& t1, const TableAddress& t2)
{
    return t1.GetHash() < t2.GetHash();
}

bool
operator <=(const TableAddress& t1, const TableAddress& t2)
{
    return t1.GetHash() <= t2.GetHash();
}

bool
operator ==(const TableAddress& t1, const TableAddress& t2)
{
    return t1.GetHash() == t2.GetHash();
}
#else

bool
operator >(const TableAddress& t1, const TableAddress& t2)
{
    if (t1.mColumn == t2.mColumn)
    {
        if (t1.mTimePath == t2.mTimePath)
        {
            return false; // equal
        }

        return t1.mTimePath > t2.mTimePath;
    }

    return t1.mColumn > t2.mColumn;
}

bool
operator >=(const TableAddress& t1, const TableAddress& t2)
{
    if (t1.mColumn == t2.mColumn)
    {
        return t1.mTimePath >= t2.mTimePath;
    }

    return t1.mColumn >= t2.mColumn;
}

bool
operator <(const TableAddress& t1, const TableAddress& t2)
{
    if (t1.mColumn == t2.mColumn)
    {
        if (t1.mTimePath == t2.mTimePath)
        {
            return false; // equal
        }

        return t1.mTimePath < t2.mTimePath;
    }

    return t1.mColumn < t2.mColumn;
}

bool
operator <=(const TableAddress& t1, const TableAddress& t2)
{
    if (t1.mColumn == t2.mColumn)
    {
        return t1.mTimePath <= t2.mTimePath;
    }

    return t1.mColumn <= t2.mColumn;
}

bool
operator ==(const TableAddress& t1, const TableAddress& t2)
{
    return (t1.mColumn == t2.mColumn) && (t1.mTimePath == t2.mTimePath);
}
#endif
bool
operator !=(const TableAddress& t1, const TableAddress& t2)
{
    return !(t1 == t2);
}

std::ostream&
operator<<(std::ostream& os, const TableAddress& g)
{
    return os << g.GetLiteral();
}
    
}
