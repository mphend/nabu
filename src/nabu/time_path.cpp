//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <algorithm>
#include <sstream>

#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include "time_path.h"


namespace nabu
{

void
TimePath::RenderToJson(Json::Value& v) const
{
    for (size_t i = 0; i < size(); i++)
        v[(int)i] = (*this)[i];
}

void
TimePath::ParseFromJson(const Json::Value& v)
{
    if (!v.isArray())
        throw cor::Exception("TimePath::ParseFromJson -- JSON is not an array");
    resize(v.size());
    for (size_t i = 0; i < size(); i++)
        (*this)[i] = v[(int)i].asUInt();
}

std::string
TimePath::GetLiteral(char delimiter) const
{
    if (empty())
        return "root";

    std::string s;
    for (int i = size() - 1; i >= 0; i--)
    {
        // to_string not available before C++11
        //s += std::to_string((*this)[i]);
        std::ostringstream oss;
        oss << (*this)[i];
        s += oss.str();

        if (i != 0)
            s += delimiter;
    }

    return s;
}

void
TimePath::Parse(const std::string& s, char delimiter)
{
    clear();
    if (s.empty())
        return;

    std::vector<std::string> v;
    std::string ds;
    ds.push_back(delimiter);
    cor::Split(s, ds, v);

    if ((v.size() == 1) && (v[0] == "root"))
        return;

    resize(v.size());
    for (size_t i = 0; i < size(); i++)
    {
        std::istringstream iss(v[i]);
        Chunk n;
        iss >> n;
        if (iss.fail())
            throw cor::Exception("TimePath::Parse() -- '%s' not parsable into integers", s.c_str());
        (*this)[i] = n;
    }
    Reverse();

}

TimePath
TimePath::operator+(unsigned int x) const
{
    TimePath r = *this;
    r.push_back(x);
    return r;
}

TimePath
TimePath::RootDownTo(size_t layer) const
{
    TimePath r(size() - layer);

    if (layer >= size())
        throw cor::Exception("TimePath::RootDownTo(%d) exceeds size %d\n", layer, size());
    std::copy(begin() + layer, end(), r.begin());

    return r;
}

TimePath
TimePath::ZeroOut(size_t layer)
{
    if (layer > size())
        throw cor::Exception("TimePath::ZeroOut(%d) exceeds size %d\n", layer, size());

    TimePath r;
    size_t i = size() - 1;

    if (i >= layer)
    {
        while (true)
        {
            r.push_back((*this)[i]);
            if (i == layer)
                break;
            i--;
        }
    }

    for (size_t i = 0; i < layer; i++)
    {
        (*this)[i] = 0;
    }

    return r;
}

void
TimePath::Reverse()
{
    std::reverse(begin(), end());
}

bool
operator >(const TimePath& t1, const TimePath& t2)
{
    if (t1.size() != t2.size())
        return t1.size() > t2.size();

    for (size_t i = 0; i < t1.size(); i++)
    {
        if (t1[i] > t2[i])
            return true;
        if (t1[i] < t2[i])
            return false;
    }
    return false; // equal
}

bool
operator >=(const TimePath& t1, const TimePath& t2)
{
    if (t1.size() != t2.size())
        return t1.size() > t2.size();

    for (size_t i = 0; i < t1.size(); i++)
    {
        if (t1[i] > t2[i])
            return true;
        if (t1[i] < t2[i])
            return false;
    }
    return true; // equal
}

bool
operator <(const TimePath& t1, const TimePath& t2)
{
    if (t1.size() != t2.size())
        return t1.size() > t2.size();

    for (size_t i = 0; i < t1.size(); i++)
    {
        if (t1[i] < t2[i])
            return true;
        if (t1[i] > t2[i])
            return false;
    }
    return false; // equal
}

bool
operator <=(const TimePath& t1, const TimePath& t2)
{
    if (t1.size() != t2.size())
        return t1.size() > t2.size();

    for (size_t i = 0; i < t1.size(); i++)
    {
        if (t1[i] < t2[i])
            return true;
        if (t1[i] > t2[i])
            return false;
    }
    return true; // equal
}

bool
operator ==(const TimePath& t1, const TimePath& t2)
{
    if (t1.size() != t2.size())
        return false;

    for (size_t i = 0; i < t1.size(); i++)
    {
        if (t1[i] != t2[i])
            return false;
    }
    return true; // equal
}

bool
operator !=(const TimePath& t1, const TimePath& t2)
{
    return !(t1 == t2);
}

TimePath operator+(const MetaKey& key, const TimePath& path)
{
    // prepend chunks, ignore addresses
    if (!key.IsChunk())
        return path;

    TimePath r(path.size() + 1);
    r[0] = key.GetChunk();
    for (size_t i = 0; i < path.size(); i++)
        r[i+1] = path[i];
    return r;
}


std::ostream& operator <<(std::ostream& os, const TimePath& p)
{
    os << p.GetLiteral();
    return os;
}

} //  end namespace
