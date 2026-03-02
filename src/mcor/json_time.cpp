//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include "json_time.h"
#include <json/writer.h>

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace Json {

void TimeToJson(const cor::Time& t, Json::Value& v)
{
    if (!t.Valid())
    {
        v["invalid"] = true;
    }
    else
    {
        struct timespec ts = t.ToTimeSpec();
        //std::cout << "TimeToJson: "  << ts.tv_sec << ", " << ts.tv_nsec << std::endl;
        v["sec"] = (int)ts.tv_sec;
        v["nsec"] = (int)ts.tv_nsec;
    }
}

cor::Time TimeFromJson(const Json::Value& v)
{
    if (v.isMember("invalid"))
        return cor::Time(cor::Time::eInvalid);

    struct timespec t;
    t.tv_sec = v["sec"].asInt();
    t.tv_nsec = v["nsec"].asInt();

    return cor::Time(t);
}

void TimeRangeToJson(const cor::TimeRange& tr, Json::Value& v)
{
    if (!tr.Valid())
    {
        v["invalid"] = true;
    }
    else
    {
        TimeToJson(tr.First(), v["first"]);
        TimeToJson(tr.Final(), v["final"]);
    }
}

cor::TimeRange TimeRangeFromJson(const Json::Value& v)
{
    if (v.isMember("invalid"))
        return cor::TimeRange();
    
    cor::Time f = TimeFromJson(v["first"]);
    cor::Time e = TimeFromJson(v["final"]);
    return cor::TimeRange(f,e);
}


    
}
