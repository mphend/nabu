//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_COLUMN_NAME__
#define __PKG_NABU_COLUMN_NAME__

#include <list>
#include <map>
#include <string>
#include <vector>

#include <json/json.h>
#include <mcor/bigtime.h>

class MD5state_st;

namespace nabu
{


/** class ColumnName: a vector of string components, like a directory path
 *  index 0 is root, index 1 is next layer down, etc.
    e.g.
        path[0] = "dasi";
        path[1] = "boulder";
        path[2] = "raw";
 *
 */
class ColumnName
{
public:
    ColumnName();
    // top-to-bottom address delimited by slashes or some other character
    ColumnName(const std::string& string, char delimiter = '/');
    ColumnName(const char* string, char delimiter = '/');
    ColumnName(const std::vector<std::string>& path);

    ColumnName(const ColumnName& other);
    void operator=(const ColumnName& other);

    virtual ~ColumnName();


    size_t Size() const;
    bool Empty() const;
    const std::string& Front() const;
    const std::string& Back() const;
    void PopFront();
    void PopBack();

    // Set "wildcard" property used for less_than comparisons of Address
    // type objects, and nothing else.
    // See implementation for details; this is used to get ranges of
    // addresses from a map with ColumnName as its key value
    void SetWildcard(bool b);
    bool GetWildcard() const { return mWildcard; }

    // returns if the given component is found in an Address metakey
    bool Find(const std::string& component) const;

    // XXX BigTime should be 'BigNumber' someday
    BigTime Hash() const;

    std::string& At(size_t index);
    std::string At(size_t index) const;
    std::string operator[](size_t i) const { return At(i); }
    std::string& operator[](size_t i) { return At(i); }
    void Replace(size_t at, const ColumnName& other);
    std::string GetLiteral(const char delimeter = '/') const;

    void Parse(std::istream& is);
    void Parse(const std::string& s, char delimiter);
    void Parse(const std::vector<std::string>& path);

    // mPath comparisons
    bool gt(const ColumnName& other) const;
    bool ge(const ColumnName& other) const;
    bool lt(const ColumnName& other) const;
    bool le(const ColumnName& other) const;
    bool eq(const ColumnName& other) const;
    bool ne(const ColumnName& other) const;

    void Append(const ColumnName& other);
    void operator+=(const ColumnName& mk) { Append(mk); }

    // returns portion of path down to given layer, which must be less
    // than Size(); an exception is thrown if this is not an Address
    ColumnName PathDownTo(size_t layer) const;

private:
    std::vector<std::string> mPath;

    bool mWildcard;
};

bool operator >(const ColumnName& a1, const ColumnName& a2);
bool operator >=(const ColumnName& a1, const ColumnName& a2);
bool operator <(const ColumnName& a1, const ColumnName& a2);
bool operator <=(const ColumnName& a1, const ColumnName& a2);
bool operator ==(const ColumnName& a1, const ColumnName& a2);
bool operator !=(const ColumnName& a1, const ColumnName& a2);

// appends s to a1; exception if a1 is not Address type
ColumnName operator+(const ColumnName& a1, const std::string& );
ColumnName operator+(const ColumnName& a1, const ColumnName& a2);

std::ostream& operator<<(std::ostream& os, const ColumnName& a);



}

#endif
