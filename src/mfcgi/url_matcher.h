//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_FCGI_URL_MATCHER_H
#define GICM_FCGI_URL_MATCHER_H

#include <map>
#include <set>
#include <string>

#include <mcor/field_matcher.h>
#include <mcor/mfile.h>
#include <mcor/mthread.h>

#include "stream.h"

namespace fcgi {


/** class KeyedFieldMap : subclass to throw a fcgi::Exception
 *  400 for missing field
 *
 */
class KeyedFieldMap : public cor::KeyedFieldMap
{
public:
    // lower-case compare, provide a better error message,
    // and make sure exception type is cor::Exception
    const std::string& at(const std::string& key) const;
};


/** class UrlMatcher : matches callbacks for URL ala Flask
 *
 *  url is mix of static and carat-delimited keyed components
    ex)
       /static1/static2/<key1>/static3/<key2>

       static components are case-sensitive
       keyed components are forced to lowercase (this is the keyname, not the
          value, so /<Direction>/ will be found at 'direction' but when
          evaluated, /North/ will be 'North',
          i.e., keyed["direction"] = "North")

    if 'methods' is empty, all methods are valid; else
       methods should be slash delimited, e.g., "GET", "GET/POST", etc.
 *
 */

class UrlMatcher
{
public:
    UrlMatcher(const std::string& url, const std::string& methods);
    virtual ~UrlMatcher();

    // if url and method ("GET", "POST", etc.) match this, returns true and fills
    // in any wildcard components into 'keyed'
    // otherwise returns false
    bool Equals(const std::string& url, const std::string& method, fcgi::KeyedFieldMap& keyed) const;

    // used for container key value sorting only
    bool operator<(const UrlMatcher& other) const;
    bool operator==(const UrlMatcher& other) const;

    std::string GetLiteral() const;

protected:
    const std::string mUrl;
    const std::string mMethodsLiteral;

    void Parse(const std::string& url, const std::string& methods);
    cor::FieldMatcher mFieldMatcher;

    std::set<std::string> mMethods;
};



}

#endif
