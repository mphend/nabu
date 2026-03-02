//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <iostream>
#include <sstream>
#include <stdio.h>

#include <json/writer.h>
#include <mcor/strutil.h>

#include "http_parameters.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)



namespace http {

void
ParameterMap::SetBool(const std::string& key, bool b)
{
    this->operator[](key) = b ? "true" : "false";
}

void
ParameterMap::SetInteger(const std::string& key, int i)
{
    std::ostringstream oss;
    oss << i;
    this->operator[](key) = oss.str();
}

void
ParameterMap::SetString(const std::string& key, const std::string& value)
{
    this->operator[](key) = value;
}

void
ParameterMap::SetTime(const std::string& key, const cor::Time& t)
{
    std::ostringstream oss;
    struct timespec ts = t.ToTimeSpec();
    oss << ts.tv_sec << "," << ts.tv_nsec;

    this->operator[](key) = oss.str();
}

std::string
ParameterMap::Print() const
{
    std::ostringstream oss;
    for (const_iterator i = begin(); i != end(); i++)
    {
        oss << i->first << " = " << i->second << std::endl;
    }
    return oss.str();
}

std::string
ParameterMap::BuildURI(const std::string& baseUrl) const
{
    std::string r = baseUrl;
    if (!empty())
    {
        r += "?";
        const_iterator i = begin();
        while (true)
        {
            std::string v = i->second;
            // replace spaces with '%20'
            cor::ReplaceSubstring(v, " ", "%20");
            r += i->first + "=" + v;
            i++;
            if (i == end())
                break;
            r += "&";
        }
    }
    return r;
}


}