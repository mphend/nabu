//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __interweb_http_parameters__
#define __interweb_http_parameters__

#include <map>
#include <string>
#include <vector>

#include <curl/curl.h>
#include <json/value.h>
#include <mcor/mmutex.h>
#include <mcor/mthread.h>
#include <mcor/mtime.h>


namespace http {


/** Class ParameterMap : map of key=value parameters
 *
 */
class ParameterMap : private std::map<std::string, std::string>
{
public:
    void SetBool(const std::string& key, bool b);
    void SetInteger(const std::string& key, int i);
    void SetString(const std::string& key, const std::string& value);
    void SetTime(const std::string& key, const cor::Time& t);

    std::string Print() const;

    // encode the parameters into a URL with the given base
    std::string BuildURI(const std::string& baseUrl) const;
};


} // end namespace

#endif
