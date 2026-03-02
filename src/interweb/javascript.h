//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __interweb_javascript
#define __interweb_javascript

#include <map>
#include <string>
#include <ostream>

/** Javascript Object parser class
 *  This is not JSON, but whatever Javascript uses to serialize its objects.
 *
 *  This was built to parse SEL-3060 responses and isn't ready for
 *  general use without care; in particular:
 *
 *    1) single nested (one set of curly braces)
 *    2) keys are strings
 *    3) values are single quoted strings with no nested single quotes
 *
 *  Expects this:
 *     {webVersion:'SEL-3060-R104-0217',deviceType:'Access Point'}
 *  where JSON would be:
 *     {"webVersion":"SEL-3060-R104-0217","deviceType":"Access Point"}
 */
class JavascriptObject : public std::map<std::string, std::string>
{
    typedef std::map<std::string, std::string> Base;
public:
    void Parse(const std::string& s);
};

std::ostream& operator<<(std::ostream& oss, const JavascriptObject& js);

#endif
