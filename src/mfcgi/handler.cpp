//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <errno.h>

#include <sstream>

#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include "exception.h"
#include "handler.h"

namespace fcgi {


bool
ParameterMap::GetBool(const std::string& key) const
{
    const_iterator i = find(key);
    if (i == end())
        throw Exception(400, "missing required parameter '%s'", key.c_str());

    return GetBool(key, false); // second arg will be unused
}


bool
ParameterMap::GetBool(const std::string& key, bool def) const
{
    const_iterator i = find(key);
    if (i == end())
        return def;

    if (cor::IEquals(i->second, "true"))
        return true;
    if (cor::IEquals(i->second, "false"))
        return false;

    std::istringstream iss(i->second);
    bool b;
    iss >> b;
    if (iss.fail() || iss.bad())
    {
        throw Exception(400, "invalid boolean value '%s' for required parameter '%s'", i->second.c_str(), key.c_str());
    }
    return b;
}


int
ParameterMap::GetInteger(const std::string& key) const
{
    const_iterator i = find(key);
    if (i == end())
        throw Exception(400, "missing required parameter '%s'", key.c_str());

    return GetInteger(key, 0); // second arg will be unused
}


int
ParameterMap::GetInteger(const std::string& key, int def) const
{
    const_iterator i = find(key);
    if (i == end())
        return def;

    std::istringstream iss(i->second);
    int r;
    iss >> r;
    if (iss.fail() || iss.bad())
    {
        throw Exception(400, "invalid integer value '%s' for required parameter '%s'", i->second.c_str(), key.c_str());
    }
    return r;
}


std::string
ParameterMap::GetString(const std::string& key) const
{
    const_iterator i = find(key);
    if (i == end())
        throw Exception(400, "missing required parameter '%s'", key.c_str());

    return GetString(key, ""); // second arg will be unused
}


std::string
ParameterMap::GetString(const std::string& key, const std::string& def) const
{
    const_iterator i = find(key);
    if (i == end())
        return def;

    return i->second;
}

cor::Time
ParameterMap::GetTime(const std::string& key) const
{
    const_iterator i = find(key);
    if (i == end())
        throw Exception(400, "missing required parameter '%s'", key.c_str());

    return GetTime(key, cor::Time(cor::Time::eInvalid)); // second arg will be unused
}


cor::Time
ParameterMap::GetTime(const std::string& key, const cor::Time& def) const
{
    const_iterator i = find(key);
    if (i == end())
        return def;

    std::vector<std::string> v;
    cor::Split(i->second, ",", v);
    if (v.size() != 2)
        throw Exception(400, "invalid <sec,nsec> value '%s' for required parameter '%s'", i->second.c_str(), key.c_str());

    struct timespec ts;
    {
        std::istringstream iss(v[0]);
        iss >> ts.tv_sec;
        if (iss.fail() || iss.bad())
            throw Exception(400, "invalid <sec,nsec> value '%s' for required parameter '%s'", i->second.c_str(), key.c_str());
    }

    long nsec;
    {
        std::istringstream iss(v[1]);
        iss >> ts.tv_nsec;
        if (iss.fail() || iss.bad())
            throw Exception(400, "invalid <sec,nsec> value '%s' for required parameter '%s'", i->second.c_str(), key.c_str());
    }

    cor::Time r(ts);
    return r;
}

}

