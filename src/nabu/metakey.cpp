//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <limits>
#include <sstream>

#include <openssl/md5.h>

#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include "metakey.h"


namespace nabu {

MetaKey::MetaKey() : mType(eNone), mChunk(~0), mWildcard(false)
{}

MetaKey::MetaKey(Chunk chunk) : mType(eChunk), mChunk(chunk), mWildcard(false)
{
}

MetaKey::MetaKey(const std::string& string, char delimiter) :
    mType(eAddress),
    mChunk(~0),
    mWildcard(false)
{
    Parse(string, delimiter);
}

MetaKey::MetaKey(const char* str, char delimiter) :
    mType(eAddress),
    mChunk(~0),
    mWildcard(false)
{
    Parse(str, delimiter);
}

MetaKey::MetaKey(const std::vector<std::string>& pathIn) :
    mType(eAddress),
    mChunk(~0),
    mWildcard(false)
{
    Parse(pathIn);
}

MetaKey::MetaKey(const MetaKey& other)
{
    mPath = other.mPath;

    mChunk = other.mChunk;
    mType = other.mType;
    mWildcard = other.mWildcard;
}

MetaKey
MetaKey::MainBranch()
{
    MetaKey r("main");
    return r;
}

MetaKey
MetaKey::EmptyAddress()
{
    MetaKey r;
    r.mType = eAddress;
    return r;
}

void
MetaKey::operator=(const MetaKey& other)
{
    mPath.clear();

    if (other.IsAddress())
    {
        mPath = other.mPath;
    }

    mChunk = other.mChunk;
    mType = other.mType;
    mWildcard = other.mWildcard;
}

MetaKey::~MetaKey()
{
}

std::string
MetaKey::TypeName() const
{
    if (mType == eAddress)
        return "address";
    else if (mType == eChunk)
        return "chunk";
    else
        return "none";
}

size_t
MetaKey::Size() const
{
    if (mType != eAddress)
        throw cor::Exception("MetaKey::Size() -- not an address");
    return mPath.size();
}

bool
MetaKey::Empty() const
{
    if (mType != eAddress)
        throw cor::Exception("MetaKey::Empty() -- not an address");
    return mPath.size() == 0;
}

const std::string&
MetaKey::Front() const
{
    if (mType != eAddress)
        throw cor::Exception("MetaKey::Front() -- not an address");
    if (mPath.size() == 0)
        throw cor::Exception("MetaKey::Front() -- object is empty");
    return mPath[0];
}

const std::string&
MetaKey::Back() const
{
    if (mType != eAddress)
        throw cor::Exception("MetaKey::Back() -- not an address");
    if (mPath.size() == 0)
        throw cor::Exception("MetaKey::Back() -- object is empty");
    return mPath[mPath.size() - 1];
}

void
MetaKey::PopFront()
{
    if (mType != eAddress)
        throw cor::Exception("MetaKey::PopFront() -- not an address");
    if (mPath.size() == 0)
        throw cor::Exception("MetaKey::PopFront() -- object is empty");

    std::vector<std::string> newPath(mPath.size() - 1);
    for (size_t i = 1; i < mPath.size(); i++)
    {
        newPath[i-1] = mPath[i];
    }
    mPath = newPath;
}

void
MetaKey::PopBack()
{
    if (mType != eAddress)
        throw cor::Exception("MetaKey::PopBack() -- not an address");
    if (mPath.size() == 0)
        throw cor::Exception("MetaKey::PopBack() -- object is empty");

    mPath.pop_back();
}

void
MetaKey::SetWildcard(bool b)
{
    mWildcard = b;

    // DEFENSIVE
    if (mType == eChunk)
        throw cor::Exception("Cannot set wildcard on Chunk-type MetaKey");

    mType = eAddress;
}

bool
MetaKey::Find(const std::string& component) const
{
    if (mType == eChunk)
        throw cor::Exception("Cannot Find() in Chunk-type MetaKey");
    if (mType == eNone)
        return false;

    for (size_t i = 0; i < mPath.size(); i++)
    {
        if (component == mPath[i])
            return true;
    }
    return false;
}

bool
MetaKey::SameType(const MetaKey& other) const
{
    return (mType != eNone) && (mType == other.mType);
}

Chunk
MetaKey::GetChunk() const
{
    return mChunk;
}

void
MetaKey::MD5Update(MD5_CTX& ctx) const
{
    if (IsAddress())
    {
        for (size_t i = 0; i < mPath.size(); i++)
            ::MD5_Update(&ctx, mPath[i].data(), mPath[i].length());
    }
    else
    {
        ::MD5_Update(&ctx, &mChunk, sizeof(Chunk));
    }
}

std::string&
MetaKey::At(size_t index)
{
    if (IsChunk())
        throw cor::Exception("MetaKey::At() -- Chunk type");

    std::string s = "";

    if (IsAddress())
    {
        if (index >= mPath.size())
            throw cor::Exception("MetaKey::At() -- index %ld out-of-range (length %ld)", index, mPath.size());
        return mPath[index];
    }
    throw cor::Exception("MetaKey::At() -- non-Address");
}

std::string
MetaKey::At(size_t index) const
{
    return const_cast<MetaKey*>(this)->At(index);
}

void
MetaKey::Replace(size_t at, const MetaKey& other)
{
    // appending a string to an Unknown turns it into an Address; but it is
    // an illegal operation for a Chunk type
    if (!other.IsAddress())
        throw cor::Exception("MetaKey::Replace() -- operand is not an Address");

    if (IsChunk())
        throw cor::Exception("MetaKey::Replace() -- Chunk type");
    if (IsUnknown())
        mType = eAddress;
    if (at >= mPath.size())
        throw cor::Exception("MetaKey::Replace() -- index %ld out-of-range (%ld)", at, mPath.size());

    size_t newSize = mPath.size() + other.Size() - 1;
    std::vector<std::string> r(newSize);

    // copy existing elements, except 'at'
    for (size_t i = 0; i < at; i++)
    {
        r[i] = mPath[i];
    }
    for (size_t i = 0; i < other.Size(); i++)
    {
        r[at + i] = other.mPath[i];
    }
    for (size_t i = at + 1; i < mPath.size(); i++)
    {
        r[i] = mPath[i];
    }

    mPath = r;
}

std::string
MetaKey::GetLiteral(char delimeter) const
{
    std::string s;
    if (IsAddress())
    {
        for (size_t i = 0; i < mPath.size();)
        {
            s += mPath[i];
            i++;
            if (i != mPath.size())
                s += delimeter;
        }
    }
    else if (IsChunk())
    {
        std::stringstream ss;
        ss << *this;
        s = ss.str();
    }
    else
    {
        s = "null";
    }
    return s;
}

// When a MetaKey is a wildcard (*), it is greater than any other MetaKey
// that has the same members, even if it is shorter than it.
// so normally "foo" is less than "foo/bar", but "foo*" is greater than
// "foo/bar", but of course still less than "goo".
//
// This allows lower_bound(foo) and upper_bound(foo*) to return
// all keys that match foo/*

bool
MetaKey::gt(const MetaKey& other) const
{
    const size_t n = std::min(mPath.size(), other.mPath.size());
    for (size_t i = 0; i < n; i++)
    {
        if (mPath[i] > other.mPath[i])
            return true;
        else if (mPath[i] < other.mPath[i])
            return false;
    }
    // if a wildcard has a longer path, it is 'greater'
    if (mWildcard && (mPath.size() > other.mPath.size()))
        return true;
    if (other.mWildcard && (other.mPath.size() > mPath.size()))
        return false;
    if (!mWildcard && !other.mWildcard)
    {
        if (mPath.size() != other.mPath.size())
            return mPath.size() > other.mPath.size();
    }

    // equal
    return false;
}

bool
MetaKey::ge(const MetaKey& other) const
{
    const size_t n = std::min(mPath.size(), other.mPath.size());
    for (size_t i = 0; i < n; i++)
    {
        if (mPath[i] > other.mPath[i])
            return true;
        if (mPath[i] < other.mPath[i])
            return false;
    }
    // if a wildcard has a longer path, it is 'greater'
    if (mWildcard && (mPath.size() > other.mPath.size()))
        return true;
    if (other.mWildcard && (other.mPath.size() > mPath.size()))
        return false;
    if (!mWildcard && !other.mWildcard)
    {
        if (mPath.size() != other.mPath.size())
            return mPath.size() > other.mPath.size();
    }
    // equal
    return true;
}

bool
MetaKey::lt(const MetaKey& other) const
{
    const size_t n = std::min(mPath.size(), other.mPath.size());
    for (size_t i = 0; i < n; i++)
    {
        if (mPath[i] < other.mPath[i])
            return true;
        if (mPath[i] > other.mPath[i])
            return false;
    }
    // if a wildcard has a longer path, it is 'greater'
    if (mWildcard && (mPath.size() > other.mPath.size()))
        return false;
    if (other.mWildcard && (other.mPath.size() > mPath.size()))
        return true;
    if (!mWildcard && !other.mWildcard)
    {
        if (mPath.size() != other.mPath.size())
            return mPath.size() < other.mPath.size();
    }
    // equal
    return false;
}

bool
MetaKey::le(const MetaKey& other) const
{
    const size_t n = std::min(mPath.size(), other.mPath.size());
    for (size_t i = 0; i < n; i++)
    {
        if (mPath[i] < other.mPath[i])
            return true;
        if (mPath[i] > other.mPath[i])
            return false;
    }
    // if a wildcard has a longer path, it is 'greater'
    if (mWildcard && (mPath.size() > other.mPath.size()))
        return false;
    if (other.mWildcard && (other.mPath.size() > mPath.size()))
        return true;
    if (!mWildcard && !other.mWildcard)
    {
        if (mPath.size() != other.mPath.size())
            return mPath.size() < other.mPath.size();
    }
    // equal
    return true;
}

bool
MetaKey::eq(const MetaKey& other) const
{
    const size_t n = std::min(mPath.size(), other.mPath.size());
    if (mPath.size() != other.mPath.size())
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
MetaKey::ne(const MetaKey& other) const
{
    return !eq(other);
}

void
MetaKey::Append(const MetaKey& other)
{
    // appending a string to an Unknown turns it into an Address; but it is
    // an illegal operation for a Chunk type
    if (IsChunk() || other.IsChunk())
        throw cor::Exception("MetaKey::Append() -- Chunk type");
    if (IsUnknown())
        mType = eAddress;

    std::vector<std::string> r(mPath.size() + other.mPath.size());

    // copy existing elements
    for (size_t i = 0; i < mPath.size(); i++)
    {
        r[i] = mPath[i];
    }

    // copy new elements
    for (size_t i = 0; i < other.Size(); i++)
    {
        r[mPath.size() + i] = other.mPath[i];
    }

    mPath = r;
}

MetaKey
MetaKey::PathDownTo(size_t layer) const
{
    if (!IsAddress())
        throw cor::Exception("MetaKey::Append() -- not an Address type");

    if (layer > mPath.size())
        throw cor::Exception("MetaKey::PathDownTo() -- layer %ld out-of-range", layer);

    std::vector<std::string> r(layer);
    for (size_t i = 0; i < layer; i++)
    {
        r[i] = mPath[i];
    }

    // this version of the constructor is private and take ownership of r
    return MetaKey(r);
}

bool
operator >(const MetaKey& t1, const MetaKey& t2)
{
    if (!t1.SameType(t2))
    {
        throw cor::Exception("MetaKey::operator > : key type mismatch (%s and %s)", t1.GetLiteral().c_str(), t2.GetLiteral().c_str());
    }

    if (t1.IsAddress())
    {
        return t1.gt(t2);
    }

    return t1.GetChunk() > t2.GetChunk();
}

bool
operator >=(const MetaKey& t1, const MetaKey& t2)
{
    if (!t1.SameType(t2))
    {
        throw cor::Exception("MetaKey::operator >= : key type mismatch (%s and %s)", t1.GetLiteral().c_str(), t2.GetLiteral().c_str());
    }

    if (t1.IsAddress())
    {
        return t1.ge(t2);
    }

    return t1.GetChunk() >= t2.GetChunk();
}

bool
operator <(const MetaKey& t1, const MetaKey& t2)
{
    if (!t1.SameType(t2))
    {
        throw cor::Exception("MetaKey::operator < : key type mismatch (%s and %s)", t1.GetLiteral().c_str(), t2.GetLiteral().c_str());
    }

    if (t1.IsAddress())
    {
        return t1.lt(t2);
    }

    return t1.GetChunk() < t2.GetChunk();
}

bool
operator <=(const MetaKey& t1, const MetaKey& t2)
{
    if (!t1.SameType(t2))
    {
        throw cor::Exception("MetaKey::operator <= : key type mismatch (%s and %s)", t1.GetLiteral().c_str(), t2.GetLiteral().c_str());
    }

    if (t1.IsAddress())
    {
        return t1.le(t2);
    }

    return t1.GetChunk() <= t2.GetChunk();
}

bool
operator ==(const MetaKey& t1, const MetaKey& t2)
{
    if (t1.IsUnknown() && t2.IsUnknown())
        return true;

    if (!t1.SameType(t2))
    {
        throw cor::Exception("MetaKey::operator == : key type mismatch (%s and %s)", t1.GetLiteral().c_str(), t2.GetLiteral().c_str());
    }

    if (t1.IsAddress())
    {
        return t1.eq(t2);
    }

    return t1.GetChunk() == t2.GetChunk();
}


bool
operator !=(const MetaKey& t1, const MetaKey& t2)
{
    return !(t1 == t2);
}

MetaKey
operator+(const MetaKey& a1, const std::string& s)
{
    MetaKey r = a1;
    r.Append(s);
    return r;
}

MetaKey
operator+(const MetaKey& a1, const MetaKey& a2)
{
    MetaKey r = a1;
    r += a2;
    return r;
}

std::ostream&
operator<<(std::ostream& os, const MetaKey& a)
{
    if (a.IsAddress())
    {
        os << "\"" << a.GetLiteral(' ') << "\"";
    }
    else if (a.IsChunk())
    {
        os << a.GetChunk();
    }
    else
    {
        os << "root";
    }
    return os;
}

void
MetaKey::Parse(std::istream& is, Type expected)
{
    if (expected == eNone)
    {
        // type is auto detected; if starting with a quote, then eAddress;
        // otherwise, eChunk

        // DEFENSIVE
        if (!is.good())
            throw cor::Exception("MetaKey::Parse : bad stream");

        char c = is.peek();
        if (c == '"')
            expected = eAddress;
        else
            expected = eChunk;
    }

    if (expected == eAddress)
    {
        std::string s;
        // find first quote,
        is.ignore(std::numeric_limits<std::streamsize>::max(), '"');
        std::getline(is, s, '"');

        std::vector<std::string> v;
        cor::Split(s, " ", v);

        Parse(v);
    }
    else
    {
        is >> mChunk;

        mType = eChunk;
    }

}

void
MetaKey::Parse(const std::string &string, char delimiter)
{
    std::vector<std::string> path;
    std::string del(1, delimiter);
    cor::Split(string, del, path);

    Parse(path);
}

void
MetaKey::Parse(const std::vector<std::string> &pathIn)
{
    mPath.clear();

    std::vector<std::string> path = pathIn;

    // DEFENSIVE: remove any empty strings or 'null' from path
    for (size_t i = 0; i < path.size(); i++)
    {
        if ((path[i].empty()) || (path[i] == "null"))
        {
            path.erase(path.begin() + i);
        }
    }

    if (!path.empty())
    {
        mPath.resize(path.size());
        for (size_t i = 0; i < path.size(); i++)
            mPath[i] = path[i];
    }


    if (mPath.size() == 0)
        mType = eNone;
    else
    {
        mType = eAddress;
    }
}


}
