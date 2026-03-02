//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __interweb_http_headers_
#define __interweb_http_headers_

#include <map>
#include <string>
#include <vector>

#include <curl/curl.h>
#include <json/value.h>
#include <mcor/istring_map.h>
#include <mcor/mmutex.h>
#include <mcor/mthread.h>
#include <mcor/mtime.h>


namespace http {


/** Class HeaderMap : map of HTTP headers, plus return code
 *
 */
class HeaderMap : public cor::IStringMap
{
public:
    HeaderMap();
    ~HeaderMap();

    // parse the entire header string
    void Parse(const std::string& s);

    // return the numeric code, ala 200, 404, etc.
    int Code() const { return mCode; }

    // return the string following the code, ala "Bad Gateway"
    std::string CodeString() const { return mCodeString; }

    // creates a curl_slist for the contained headers, which this
    // object owns. If there are no headers, returned curl_slist* will be NULL.
    const struct curl_slist* GetCurlHeaders();

protected:
    int mCode;
    std::string mCodeString;

    struct curl_slist* mHeadersCurl;
};

// utility to dump the curl headers directly, in case you really want
// to see them
std::string DumpCurlHeaders(const struct curl_slist* headers);


} // end namespace

#endif
