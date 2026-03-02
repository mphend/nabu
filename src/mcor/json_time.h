//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_JSONCPP_JSON_TIME_
#define __PKG_JSONCPP_JSON_TIME_


#include <mcor/mtime.h>
#include <mcor/timerange.h>

#include <json/value.h>


namespace Json
{

void TimeToJson(const cor::Time& t, Json::Value& v);
cor::Time TimeFromJson(const Json::Value& v);

void TimeRangeToJson(const cor::TimeRange& tr, Json::Value& v);
cor::TimeRange TimeRangeFromJson(const Json::Value& v);

}

#endif
