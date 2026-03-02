//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <errno.h>

#include <set>
#include <sstream>

#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include "exception.h"
#include "header.h"

namespace fcgi {


/* Class ValidFields : set to validate the field names
 *
 */
class ValidFields : public std::set<std::string>
{
public:
    ValidFields();

    bool Find(const std::string& field) const;
} validFields;

ValidFields::ValidFields()
{
    // following are 'standard' response fields
    // keep all lowercase
    insert("accept-ch");
    insert("access-control-allow-origin");
    insert("access-control-allow-credentials");
    insert("access-control-expose-headers");
    insert("access-control-max-age");
    insert("access-control-allow-methods");
    insert("access-control-allow-headers");
    insert("accept-patch");
    insert("accept-ranges");
    insert("age");
    insert("allow");
    insert("cache-control");
    insert("connection");
    insert("content-disposition");
    insert("content-encoding");
    insert("content-language");
    insert("content-length");
    insert("content-location");
    insert("content-md5");
    insert("content-range");
    insert("content-type");
    insert("date");
    insert("delta-base");
    insert("etag");
    insert("expires");
    insert("im");
    insert("last-modified");
    insert("link");
    insert("location");
    insert("p3p");
    insert("pragma");
    insert("preference-applied");
    insert("proxy-authenticate");
    insert("public-key-pins");
    insert("retry-after");
    insert("server");
    insert("set-cookie");
    insert("status"); // not standard, but implied
    insert("strict-transport-security");
    insert("trailer");
    insert("transfer-encoding");
    insert("tk");
    insert("upgrade");
    insert("vary");
    insert("via");
    insert("warning");
    insert("www-authenticate");
    insert("x-frame-options");
}

bool
ValidFields::Find(const std::string& field) const
{
    // fields are case-insensitive
    std::string s = cor::ToLower(field);
    return find(s) != end();
}


/* Header Implementation
 *
 */

void
Header::Set(const std::string& key, const std::string& value)
{
    if (!validFields.Find(key))
        throw cor::Exception("key '%s' is not a standard response field\n", key.c_str());
    mImp[cor::ToLower(key)] = value;
}

void
Header::SetInt(const std::string& key, int value)
{
    std::ostringstream oss;
    oss << value;
    Set(key, oss.str());
}

std::string
Header::Get(const std::string& key)
{
    Imp::const_iterator i = mImp.find(cor::ToLower(key));
    if (i == mImp.end())
        return "";
    return i->second;
}

std::string
Header::ReadString(const std::string& key) const
{
    Imp::const_iterator i = mImp.find(cor::ToLower(key));
    if (i == mImp.end())
        throw fcgi::Exception(400, "Failed to find header field '%s'", key.c_str());
    return i->second;
}

int
Header::ReadInt(const std::string& key) const
{
    std::stringstream iss(ReadString(key));
    int r;
    iss >> r;
    if (iss.fail())
        throw fcgi::Exception(400, "Header field '%s' is not an integer", key.c_str());
    return r;
}

void
Header::Print(std::string& out) const
{
    Imp::const_iterator i = mImp.begin();
    for (; i != mImp.end(); i++)
    {
        out += i->first + ": " + i->second + "\r\n";
    }
    out += "\r\n";
}

void
Header::Print(cor::File& out) const
{
    Imp::const_iterator i = mImp.begin();
    for (; i != mImp.end(); i++)
    {
        std::string line = i->first + ": " + i->second + "\r\n";
        out.Write(line.c_str(), line.size());
    }
    i = mImp.find("status");

    // force a bad reply if we miss status, just to make sure
    // it is returned everywhere
    if (i == mImp.end())
    {
        out.Write("status: 518\r\n", 13);
    }

    out.Write("\r\n", 2);
}

}

