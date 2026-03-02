//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_METAKEY__
#define __PKG_NABU_METAKEY__

#include <stdint.h>
#include <string>
#include <vector>

#include <json/json.h>

class MD5state_st;

namespace nabu
{

// for legibility
typedef uint32_t Chunk;


/** class MetaKey: key of metadata tables, either:
 *      vector of strings
 *      32-bit integer: index into time bin ("chunk")
 *
 */
class MetaKey
{
public:
    MetaKey();
    MetaKey(Chunk chunk);
    // top-to-bottom address delimited by slashes or some other character
    MetaKey(const std::string& string, char delimiter = '/');
    MetaKey(const char* string, char delimiter = '/');
    // index 0 is root, index 1 is next layer down, etc.
    // e.g.
    //    path[0] = "dasi";
    //    path[1] = "boulder";
    //    path[2] = "raw";
    MetaKey(const std::vector<std::string>& path);

    static MetaKey MainBranch();
    // an empty, but address-typed value used for comparison with other
    // addresses
    static MetaKey EmptyAddress();

    MetaKey(const MetaKey& other);
    void operator=(const MetaKey& other);

    virtual ~MetaKey();

    enum Type { eNone, eAddress, eChunk};
    std::string TypeName() const;

    bool IsAddress() const { return mType == eAddress; }
    bool IsChunk() const { return mType == eChunk; }
    bool IsUnknown() const { return mType == eNone; }

    size_t Size() const;
    bool Empty() const;
    const std::string& Front() const;
    const std::string& Back() const;
    void PopFront();
    void PopBack();

    // Set "wildcard" property used for less_than comparisons of Address
    // type objects, and nothing else.
    // See implementation for details; this is used to get ranges of
    // addresses from a map with MetaKey as its key value
    void SetWildcard(bool b);
    bool GetWildcard() const { return mWildcard; }

    // returns if the given component is found in an Address metakey
    bool Find(const std::string& component) const;

    bool SameType(const MetaKey& other) const;

    Chunk GetChunk() const;

    void MD5Update(struct MD5state_st& ctx) const;

    std::string& At(size_t index);
    std::string At(size_t index) const;
    std::string operator[](size_t i) const { return At(i); }
    std::string& operator[](size_t i) { return At(i); }
    void Replace(size_t at, const MetaKey& other);
    std::string GetLiteral(const char delimeter = '/') const;

    void Parse(std::istream& is, Type expected);
    void Parse(const std::string& s, char delimiter);
    void Parse(const std::vector<std::string>& path);

    // mPath comparisons
    bool gt(const MetaKey& other) const;
    bool ge(const MetaKey& other) const;
    bool lt(const MetaKey& other) const;
    bool le(const MetaKey& other) const;
    bool eq(const MetaKey& other) const;
    bool ne(const MetaKey& other) const;

    void Append(const MetaKey& other);
    void operator+=(const MetaKey& mk) { Append(mk); }

    // returns portion of path down to given layer, which must be less
    // than Size(); an exception is thrown if this is not an Address
    MetaKey PathDownTo(size_t layer) const;

private:
    Type mType;
    std::vector<std::string> mPath;

    bool mWildcard;

    Chunk mChunk;
};

bool operator >(const MetaKey& a1, const MetaKey& a2);
bool operator >=(const MetaKey& a1, const MetaKey& a2);
bool operator <(const MetaKey& a1, const MetaKey& a2);
bool operator <=(const MetaKey& a1, const MetaKey& a2);
bool operator ==(const MetaKey& a1, const MetaKey& a2);
bool operator !=(const MetaKey& a1, const MetaKey& a2);

// appends s to a1; exception if a1 is not Address type
MetaKey operator+(const MetaKey& a1, const std::string& );
MetaKey operator+(const MetaKey& a1, const MetaKey& a2);

std::ostream& operator<<(std::ostream& os, const MetaKey& a);



}

#endif
