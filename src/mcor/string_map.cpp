//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <stdio.h>
#include <sstream>

#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include "string_map.h"
#include "mexception.h"

namespace cor {

StringMap::StringMap()
{}

StringMap::~StringMap()
{}

void
StringMap::InsertString(const std::string& key, const std::string& value)
{
    (*this)[key] = value;
}

void
StringMap::InsertInt(const std::string& key, int& value)
{
    std::ostringstream oss;
    oss << value;
    InsertString(key, oss.str());
}

void
StringMap::InsertBool(const std::string& key, bool& value)
{
    InsertString(key, value ? "1" : "0");
}

void
StringMap::InsertDouble(const std::string& key, double& value)
{
    std::ostringstream oss;
    oss << value;
    InsertString(key, oss.str());
}

int
StringMap::ReadInt(const std::string& key) const
{
    std::istringstream iss(ReadString(key));
    int r;
    iss >> r;
    if (iss.fail())
        throw cor::Exception("'%s' is not an integer", key.c_str());
    return r;
}

bool
StringMap::ReadBool(const std::string& key) const
{
    std::istringstream iss(ReadString(key));
    int r;
    iss >> r;
    if (iss.fail())
        throw cor::Exception("'%s' is not convertable to a bool", key.c_str());
    return r != 0;
}

double
StringMap::ReadDouble(const std::string& key) const
{
    std::istringstream iss(ReadString(key));
    double r;
    iss >> r;
    if (iss.fail())
        throw cor::Exception("'%s' is not a float", key.c_str());
    return r;
}

std::string
StringMap::ReadString(const std::string& key) const
{
    const_iterator i = find(key);
    if (i == end())
        throw Exception("Missing value for '%s'", key.c_str());

    return i->second;
}

std::string
StringMap::Print() const
{
    std::string r;

    const_iterator i = begin();
    for (; i != end(); i++)
    {
        char buffer[i->first.size() + i->second.size() + 30]; // one line
        snprintf(buffer, sizeof(buffer), "%20s = %s\n", i->first.c_str(), i->second.c_str());
        r += buffer;
    }

    return r;
}

std::string
StringMap::ToQuasiJson() const
{
    if (empty()) {
        return "[]";
    }

    std::string s = "[";
    const_iterator i = begin();
    while (true)
    {
        // these are illegal for environment variables
        if (i->first.empty())
            throw cor::Exception("StringMap::ToQuasiJson() -- empty key");
        if (i->first.find('=') != std::string::npos)
            throw cor::Exception("StringMap::ToQuasiJson() -- '=' character in key");

        s += "\"" + i->first + "=" + i->second + "\"";
        i++;
        if (i == end())
        {
            s += "]";
            break;
        }
        else
        {
            s += ", ";
        }
    }
    return s;
}

void
StringMap::InsertQuasiJson(const std::string& item)
{
    if (item.empty())
        throw cor::Exception("StringMap::InsertQuasiJson() -- empty item");

    std::string::size_type n = item.find('=');
    if ((n == std::string::npos) || (n == 0))
        throw cor::Exception("StringMap::InsertQuasiJson() -- Illegal 'key=value' format in %s\n", item.c_str());

    std::string key = item.substr(0, n);
    std::string value = item.substr(n+1);
    InsertString(key, value);
}

}
