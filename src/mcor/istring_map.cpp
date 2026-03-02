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

#include "istring_map.h"
#include "mexception.h"

namespace cor {

IStringMap::~IStringMap()
{}

void
IStringMap::InsertString(const std::string& key, const std::string& value)
{
    // to make this case-insensitive, always use upper case for implementation
    (*this)[cor::ToUpper(key)] = value;
}

void
IStringMap::InsertInt(const std::string& key, int& value)
{
    std::ostringstream oss;
    oss << value;
    InsertString(key, oss.str());
}

void
IStringMap::InsertDouble(const std::string& key, double& value)
{
    std::ostringstream oss;
    oss << value;
    InsertString(key, oss.str());
}

int
IStringMap::ReadInt(const std::string& key) const
{
    std::istringstream iss(ReadString(key));
    int r;
    iss >> r;
    if (iss.fail())
        throw cor::Exception("'%s' is not an integer", key.c_str());
    return r;
}

double
IStringMap::ReadDouble(const std::string& key) const
{
    std::istringstream iss(ReadString(key));
    double r;
    iss >> r;
    if (iss.fail())
        throw cor::Exception("'%s' is not a float", key.c_str());
    return r;
}

std::string
IStringMap::ReadString(const std::string& key) const
{
    // case-insensitive lookup
    std::string lookup = cor::ToUpper(key);

    const_iterator i = find(lookup);
    if (i == end())
        throw Exception("Missing value for '%s'", key.c_str());

    return i->second;
}

std::string
IStringMap::Print() const
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


}
