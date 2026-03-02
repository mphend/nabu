//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_FCGI_HANDLER_H
#define GICM_FCGI_HANDLER_H

#include <map>
#include <string>

#include <fcgiapp.h>

#include <mcor/mfile.h>
#include <mcor/mthread.h>
#include <mcor/mtime.h>
#include <mcor/timerange.h>

#include "header.h"
#include "request.h"
#include "url_matcher.h"

namespace fcgi {


/** Class ParameterMap : map of key=value parameters from the
 *  URI, plus convenience methods to access common types
 *
 *  Note that this part of the URI (the query) is case SENSITIVE.
 *
 */
class ParameterMap : public std::map<std::string, std::string>
{
public:
    // return bool; if missing, throw 400
    bool GetBool(const std::string& key) const;
    // return bool; if missing, use default
    bool GetBool(const std::string& key, bool def) const;

    // return integer; if missing, throw 400
    int GetInteger(const std::string& key) const;
    // return bool; if missing, use default
    int GetInteger(const std::string& key, int def) const;

    // return string; if missing, throw 400
    std::string GetString(const std::string& key) const;
    // return string; if missing, use default
    std::string GetString(const std::string& key, const std::string& def) const;

    // return time; if missing, throw 400
    cor::Time GetTime(const std::string& key) const;
    // return time; if missing, use default
    cor::Time GetTime(const std::string& key, const cor::Time& def) const;
};


/** class Handler : a callback for a URL pattern
 *
 *  Subclasses are responsible for generating the entire response.
 *
 *  Ex:
 *      fcgi::Header responseHeader;
        responseHeader.Set("content-Type", "text/plain");
        responseHeader.Print(request.GetWriter()); // output header first

        std::string s = "Hello, World";
        request.GetWriter().Write(s.c_str(), s.size()); // output remainder
 *
 *
 */
class Handler
{
public:
    // descriptiveName is for debug purposes only
    Handler(const std::string& descriptiveName) : mDescriptiveName(descriptiveName)
    {}
    virtual ~Handler()
    {}

    // keyedFields is a map of URL components that were matched via
    // wildcard. Keys are lower-case only.
    // parameters is the map of URL query parameters
    virtual void Process(Request& request,
                         const KeyedFieldMap& keyedFields,
                         const ParameterMap& parameters) = 0;

    // produce a log-type entry about this request
    virtual std::string DebugString(Request& request,
                            const KeyedFieldMap& keyedFields,
                            const ParameterMap& parameters)
    {
        return mDescriptiveName;
    }

    std::string Description() const { return mDescriptiveName; }

protected:
    const std::string mDescriptiveName;
};


}

#endif
