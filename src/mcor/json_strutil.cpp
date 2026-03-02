//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <json/reader.h>
#include <json/writer.h>

#include "strutil.h"
#include "json_strutil.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace Json {

std::string
Escape(const std::string& in)
{
    if (in.empty())
        return in;

    std::string ve;
    Json::FastWriter writer;
    return cor::Trim(writer.write(in), " \n\t\r\"");
}

std::string
Unescape(const std::string& in)
{
    if (in.empty())
        return in;

    Json::Reader r;
    Json::Value v;
    // add quotes to 'in'
    r.parse("\"" + in + "\"", v);
    return v.asString();
}

}
