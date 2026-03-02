//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_TABLE_ADDRESS__
#define __PKG_NABU_TABLE_ADDRESS__


#include "metakey.h"
#include "time_path.h"
#include "version.h"

namespace nabu
{

class TableAddress
{
public:
    TableAddress();
    TableAddress(const MetaKey& column, const TimePath& path, const Version& version);
    TableAddress(const TableAddress& other);
    ~TableAddress();

    std::string GetLiteral() const;
    bool IsRoot() const;

    void RenderToJson(Json::Value& v) const;
    void ParseFromJson(const Json::Value& v);

    // returns whether this address is the root of a brand-new database
    //bool IsBrandNewRoot() const;

    // component access
    const MetaKey& GetColumn() const { return mColumn; }
    const TimePath& GetTimePath() const { return mTimePath; }
    const Version& GetVersion() const { return mVersion; }
    uint64_t GetHash() const;

    void SetColumn(const MetaKey& m);
    void SetTimePath(const TimePath& tp);
    void SetVersion(const Version& v);
    // create a new unique version
    void NewVersion();
    void Zero() { mVersion = Version(); Rehash(); }

    friend bool operator >(const TableAddress& t1, const TableAddress& t2);
    friend bool operator >=(const TableAddress& t1, const TableAddress& t2);
    friend bool operator <(const TableAddress& t1, const TableAddress& t2);
    friend bool operator <=(const TableAddress& t1, const TableAddress& t2);
    friend bool operator ==(const TableAddress& t1, const TableAddress& t2);
    friend bool operator !=(const TableAddress& t1, const TableAddress& t2);

private:
    MetaKey mColumn;
    TimePath mTimePath;
    Version mVersion;
    mutable uint64_t mHash;

    void Rehash() const;
};

bool operator >(const TableAddress& t1, const TableAddress& t2);
bool operator >=(const TableAddress& t1, const TableAddress& t2);
bool operator <(const TableAddress& t1, const TableAddress& t2);
bool operator <=(const TableAddress& t1, const TableAddress& t2);
bool operator ==(const TableAddress& t1, const TableAddress& t2);
bool operator !=(const TableAddress& t1, const TableAddress& t2);
std::ostream& operator<<(std::ostream& os, const TableAddress& g);



}

#endif
