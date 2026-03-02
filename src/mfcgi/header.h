//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_FCGI_HEADER_H
#define GICM_FCGI_HEADER_H

#include <map>
#include <string>

#include <fcgiapp.h>

#include <mcor/mfile.h>
#include <mcor/mthread.h>

#include "request.h"

namespace fcgi {


/** class Header : wraps HTTP response header
 *
 *  see: https://en.wikipedia.org/wiki/List_of_HTTP_header_fields
 *
 */
class Header
{
public:
    Header() {}
    virtual ~Header() {}

    // validates key
    void Set(const std::string& key, const std::string& value);
    void SetInt(const std::string& key, int value);
    // returns "" if key is missing
    std::string Get(const std::string& key);

    // throws if key is missing
    std::string ReadString(const std::string& key) const;
    int ReadInt(const std::string& key) const;

    // output in HTTP format
    void Print(std::string& out) const;
    void Print(cor::File& file) const;

protected:
    typedef std::map<std::string, std::string> Imp;
    Imp mImp;
};


}

#endif
