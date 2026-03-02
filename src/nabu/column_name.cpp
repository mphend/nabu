//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <limits>
#include <sstream>

#include <openssl/md5.h>

#include <mcor/binary.h>
#include <mcor/hash_md5.h>
#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include "column_name.h"



namespace nabu {

ColumnName::ColumnName() : mWildcard(false)
{}


ColumnName::ColumnName(const std::string& string, char delimiter) :
    mWildcard(false)
{
    Parse(string, delimiter);
}

ColumnName::ColumnName(const char* str, char delimiter) :
    mWildcard(false)
{
    Parse(str, delimiter);
}

ColumnName::ColumnName(const std::vector<std::string>& pathIn) :
    mWildcard(false)
{
    Parse(pathIn);
}

ColumnName::ColumnName(const ColumnName& other)
{
    mPath = other.mPath;
    mWildcard = other.mWildcard;
}

void
ColumnName::operator=(const ColumnName& other)
{
    mPath = other.mPath;
    mWildcard = other.mWildcard;
}

ColumnName::~ColumnName()
{
}


size_t
ColumnName::Size() const
{
    return mPath.size();
}

bool
ColumnName::Empty() const
{
    return mPath.empty();
}

const std::string&
ColumnName::Front() const
{
    if (Empty())
        throw cor::Exception("ColumnName::Front() -- object is empty");
    return mPath[0];
}

const std::string&
ColumnName::Back() const
{
    if (Empty())
        throw cor::Exception("ColumnName::Back() -- object is empty");
    return mPath.back();
}

void
ColumnName::PopFront()
{
    if (Empty())
        throw cor::Exception("ColumnName::PopFront() -- object is empty");

    // there is no pop_front() on vector; must synthesize it
    mPath.erase(mPath.begin());
}

void
ColumnName::PopBack()
{
    if (Empty())
        throw cor::Exception("ColumnName::PopBack() -- object is empty");

    mPath.pop_back();
}

void
ColumnName::SetWildcard(bool b)
{
    mWildcard = b;
}

bool
ColumnName::Find(const std::string& component) const
{
    for (size_t i = 0; i < Size(); i++)
    {
        if (component == mPath[i])
            return true;
    }
    return false;
}

BigTime
ColumnName::Hash() const
{
    cor::Hash hash;
    for (size_t i = 0; i < Size(); i++)
        hash.Add(mPath[i]);

    BigTime r;

    r.mSeconds = cor::Unpack64(hash.Get().mValue);
    r.mAttoSeconds = cor::Unpack64(hash.Get().mValue + 8);

    return r;
}

std::string&
ColumnName::At(size_t index)
{
    std::string s = "";

    if (index >= Size())
        throw cor::Exception("ColumnName::At() -- index %ld out-of-range (length %ld)", index, Size());
    return mPath[index];
}

std::string
ColumnName::At(size_t index) const
{
    return const_cast<ColumnName*>(this)->At(index);
}

void
ColumnName::Replace(size_t at, const ColumnName& other)
{
    if (at >= Size())
        throw cor::Exception("ColumnName::Replace() -- index %ld out-of-range (%ld)", at, Size());

    mPath.erase(mPath.begin() + at);
    mPath.insert(mPath.begin() + at,
                 other.mPath.begin(),
                 other.mPath.end());
}

std::string
ColumnName::GetLiteral(char delimeter) const
{
    std::string s;
    for (size_t i = 0; i < Size();)
    {
        s += mPath[i];
        i++;
        if (i != Size())
            s += delimeter;
    }
    
    return s;
}

// When a ColumnName is a wildcard (*), it is greater than any other ColumnName
// that has the same members, even if it is shorter than it.
// so normally "foo" is less than "foo/bar", but "foo*" is greater than
// "foo/bar", but of course still less than "goo".
//
// This allows lower_bound(foo) and upper_bound(foo*) to return
// all keys that match foo/*

bool
ColumnName::gt(const ColumnName& other) const
{
    const size_t n = std::min(Size(), other.Size());
    for (size_t i = 0; i < n; i++)
    {
        if (mPath[i] > other.mPath[i])
            return true;
        else if (mPath[i] < other.mPath[i])
            return false;
    }
    // if a wildcard has a longer path, it is 'greater'
    if (mWildcard && (Size() > other.Size()))
        return true;
    if (other.mWildcard && (other.Size() > Size()))
        return false;
    if (!mWildcard && !other.mWildcard)
    {
        if (Size() != other.Size())
            return Size() > other.Size();
    }

    // equal
    return false;
}

bool
ColumnName::ge(const ColumnName& other) const
{
    const size_t n = std::min(Size(), other.Size());
    for (size_t i = 0; i < n; i++)
    {
        if (mPath[i] > other.mPath[i])
            return true;
        else if (mPath[i] < other.mPath[i])
            return false;
    }
    // if a wildcard has a longer path, it is 'greater'
    if (mWildcard && (Size() > other.Size()))
        return true;
    if (other.mWildcard && (other.Size() > Size()))
        return false;
    if (!mWildcard && !other.mWildcard)
    {
        if (Size() != other.Size())
            return Size() > other.Size();
    }
    // equal
    return true;
}

bool
ColumnName::lt(const ColumnName& other) const
{
    const size_t n = std::min(Size(), other.Size());
    for (size_t i = 0; i < n; i++)
    {
        if (mPath[i] < other.mPath[i])
            return true;
        else if (mPath[i] > other.mPath[i])
            return false;
    }
    // if a wildcard has a longer path, it is 'greater'
    if (mWildcard && (Size() > other.Size()))
        return false;
    if (other.mWildcard && (other.Size() > Size()))
        return true;
    if (!mWildcard && !other.mWildcard)
    {
        if (Size() != other.Size())
            return Size() < other.Size();
    }
    // equal
    return false;
}

bool
ColumnName::le(const ColumnName& other) const
{
    const size_t n = std::min(Size(), other.Size());
    for (size_t i = 0; i < n; i++)
    {
        if (mPath[i] < other.mPath[i])
            return true;
        else if (mPath[i] > other.mPath[i])
            return false;
    }
    // if a wildcard has a longer path, it is 'greater'
    if (mWildcard && (Size() > other.Size()))
        return false;
    if (other.mWildcard && (other.Size() > Size()))
        return true;
    if (!mWildcard && !other.mWildcard)
    {
        if (Size() != other.Size())
            return Size() < other.Size();
    }
    // equal
    return true;
}

bool
ColumnName::eq(const ColumnName& other) const
{
    const size_t n = std::min(Size(), other.Size());
    if (Size() != other.Size())
        return false;

    for (size_t i = 0; i < n; i++)
    {
        if (mPath[i] != other.mPath[i])
            return false;
    }

    // equal
    return true;
}

bool
ColumnName::ne(const ColumnName& other) const
{
    return !eq(other);
}

void
ColumnName::Append(const ColumnName& other)
{
    mPath.insert(mPath.end(), other.mPath.begin(), other.mPath.end());
}

ColumnName
ColumnName::PathDownTo(size_t layer) const
{
    if (layer > Size())
        throw cor::Exception("ColumnName::PathDownTo() -- layer %ld out-of-range", layer);

    ColumnName r;
    for (size_t i = 0; i < layer; i++)
    {
        r.mPath.push_back(mPath[i]);
    }

    return r;
}

bool
operator >(const ColumnName& t1, const ColumnName& t2)
{
    return t1.gt(t2);
}

bool
operator >=(const ColumnName& t1, const ColumnName& t2)
{
    return t1.ge(t2);
}

bool
operator <(const ColumnName& t1, const ColumnName& t2)
{
    return t1.lt(t2);
}

bool
operator <=(const ColumnName& t1, const ColumnName& t2)
{
    return t1.le(t2);
}

bool
operator ==(const ColumnName& t1, const ColumnName& t2)
{
    return t1.eq(t2);
}


bool
operator !=(const ColumnName& t1, const ColumnName& t2)
{
    return !(t1 == t2);
}

ColumnName
operator+(const ColumnName& a1, const std::string& s)
{
    ColumnName r = a1;
    r.Append(s);
    return r;
}

ColumnName
operator+(const ColumnName& a1, const ColumnName& a2)
{
    ColumnName r = a1;
    r += a2;
    return r;
}

std::ostream&
operator<<(std::ostream& os, const ColumnName& a)
{
    os << "\"" << a.GetLiteral(' ') << "\"";

    return os;
}

void
ColumnName::Parse(std::istream& is)
{
    std::string s;
    // find first quote,
    is.ignore(std::numeric_limits<std::streamsize>::max(), '"');
    std::getline(is, s, '"');

    std::vector<std::string> v;
    cor::Split(s, " ", v);

    Parse(v);
}

void
ColumnName::Parse(const std::string &string, char delimiter)
{
    std::vector<std::string> path;
    std::string del(1, delimiter);
    cor::Split(string, del, path);

    Parse(path);
}

void
ColumnName::Parse(const std::vector<std::string> &pathIn)
{
    std::vector<std::string> path = pathIn;

    // DEFENSIVE: remove any empty strings or 'null' from path
    for (size_t i = 0; i < path.size(); i++)
    {
        if ((path[i].empty()) || (path[i] == "null"))
        {
            path.erase(path.begin() + i);
        }
    }

    mPath.clear();

    if (!path.empty())
    {
        mPath.resize(path.size());
        for (size_t i = 0; i < path.size(); i++)
            mPath[i] = path[i];
    }
}


}
