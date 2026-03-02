//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <sstream>

#include <mcor/mfile.h>
#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include "url.h"

namespace cor
{

Url::Url(const std::string& s) : mValid(false), mPort(0), mProtocol(eUnknown)
{
    Set(s);
}

Url::Url() : mValid(false), mPort(0), mProtocol(eUnknown)
{
}

Url::~Url()
{
}

bool
Url::Valid() const
{
    return mValid;
}

std::string
Url::GetProtocolLiteral() const
{
    if (!mValid)
        throw cor::Exception("Url::GetProtocolLiteral() -- object is invalid");

    return mProtocolLiteral;
}

Url::Protocol
Url::GetProtocol() const
{
    if (!mValid)
        throw cor::Exception("Url::GetProtocol() -- object is invalid");

    return mProtocol;
}

std::string
Url::GetHost() const
{
    if (!mValid)
        throw cor::Exception("Url::GetHost() -- object is invalid");

    return mHost;
}

int
Url::GetPort() const
{
    if (!mValid)
        throw cor::Exception("Url::GetPort() -- object is invalid");
    return mPort;
}

std::string
Url::GetResource() const
{
    if (!mValid)
        throw cor::Exception("Url::GetResource() -- object is invalid");

    return mResource;
}

std::string
Url::GetHostAndResource() const
{
    if (!mValid)
        throw cor::Exception("Url::GetHostAndResource() -- object is invalid");

    return mProtocolLiteral + "://" + mHost + "/" + mResource;
}


std::string
Url::Print() const
{
    if (!mValid)
        return "invalid";

    std::ostringstream oss;
    oss << "Protocol: " << mProtocolLiteral << std::endl;
    oss << "    Host: " << mHost << std::endl;
    oss << "    Port: " << mPort << std::endl;
    oss << "Resource: " << mResource;

    return oss.str();
}

// return reconstituted URL
std::string
Url::GetLiteral() const
{
    if (!mValid)
        return "invalid";

    std::ostringstream oss;
    oss << mPort;
    return mProtocolLiteral + "://" + mHost + ":" + oss.str() + "/" + mResource;
}

/** class ProtocolMap : helper class to decode protocol literal
 *  into enumeration
 */
class ProtocolMap : public std::map<std::string, cor::Url::Protocol>
{
public:
    ProtocolMap();

    cor::Url::Protocol Get(const std::string& key);
} protocolMap;

ProtocolMap::ProtocolMap()
{
    (*this)["file"] = cor::Url::eFile;
    (*this)["http"] = cor::Url::eHttp;
    (*this)["https"] = cor::Url::eHttps;
    (*this)["ftp"] = cor::Url::eFtp;
    (*this)["s3"] = cor::Url::eS3;
}

Url::Protocol
ProtocolMap::Get(const std::string& key)
{
    // there are not a fixed sets of schemes, so anything might be
    // valid; if not one of the keys of the map, just return "unknown"
    // rather than throwing an exception
    const_iterator i = find(key);
    if (i != end())
        return i->second;
    return cor::Url::eUnknown;
}

std::string
Url::ProtocolLiteral(Url::Protocol protocol)
{
    if (protocol == cor::Url::eFile)
        return "file";
    else if (protocol == cor::Url::eHttps)
        return "https";
    else if (protocol == cor::Url::eHttp)
        return "http";
    else if (protocol == cor::Url::eFtp)
        return "ftp";
    else if (protocol == cor::Url::eS3)
        return "s3";

    return "unknown";
}

void
Url::Set(const std::string& sIn)
{
    // expect
    // protocol://<host>[:port=80]/<resource>

    // use a temporary object to parse s until it is verified as completely
    // valid, then assign it to *this
    Url r;

    std::string::size_type doubleSlash = sIn.find("//");
    if (doubleSlash == std::string::npos)
        throw cor::Exception("Could not find at least one '//' separator in URL '%s'", sIn.c_str());

    // replace all subsequent '//' with '/'
    std::string s = sIn.substr(doubleSlash + 2);
    while(cor::ReplaceSubstring(s, "//", "/"));

    s = sIn.substr(0, doubleSlash + 2) + s;

    //printf("Input: %s\n", s.c_str());
    std::vector<std::string> v;
    cor::Split(s, "//", v);
    if (v.size() < 2)
        throw cor::Exception("Could not find at least one '//' separator in URL '%s'", s.c_str());

    r.mProtocolLiteral = v[0];
    // C++98
    //if (v[0].back() != ':')
    if (*v[0].rbegin() != ':')
        throw cor::Exception("Bad protocol '%s'\n", r.mProtocolLiteral.c_str());
    // C++98
    //r.mProtocol.pop_back(); // remove ':'
    r.mProtocolLiteral.erase(r.mProtocolLiteral.size() - 1);
    if (!cor::IsAlpha(r.mProtocolLiteral))
        throw cor::Exception("Bad protocol '%s'\n", v[0].c_str());
    // the protocol is case-insensitive; make it lower case for correct
    // comparisons, etc.
    r.mProtocolLiteral = cor::ToLower(r.mProtocolLiteral);
    r.mProtocol = protocolMap.Get(r.mProtocolLiteral);

    //printf("remainder: %s\n", v[1].c_str());
    cor::Split(v[1], "/", v);
    r.mHost = v[0];
    if (v.size() > 1)
    {
        for (size_t i = 1; i < v.size(); i++)
        {
            r.mResource += v[i] + '/';
        }
        // C++98
        //mResource.pop_back(); // drop final '/'
        r.mResource.erase(r.mResource.size() - 1);
    }

    cor::Split(r.mHost, ":", v);
    if (v.size() == 2)
    {
        r.mHost = v[0];
        std::istringstream iss(v[1]);
        iss >> r.mPort;
        if (iss.fail())
            throw cor::Exception("Invalid port number, '%s' in URL '%s'", v[1].c_str(), s.c_str());
    }
    else
        r.mPort = 80;

    if (!cor::IsAlphaNumericPlus(r.mHost, ".-"))
        throw cor::Exception("Invalid host '%s'", r.mHost.c_str());
    // C++98
    //if (!isalnum(r.mHost.front()) || !isalnum(r.mHost.back()))
    if (!isalnum(*r.mHost.begin()) || !isalnum(*r.mHost.rbegin()))
        throw cor::Exception("Invalid host '%s'", r.mHost.c_str());

    *this = r;
    mValid = true;
}

void
Url::operator+=(const std::string& s)
{
    mResource += s;
}

bool
operator==(const Url& u1, const Url& u2)
{
    if (u1.GetPort() != u2.GetPort())
        return false;
    if (u1.GetResource() != u2.GetResource())
        return false;
    if (u1.GetProtocol() != u2.GetProtocol())
        return false;
    if (u1.GetHost() != u2.GetHost())
        return false;

    return true;
}

bool
operator!=(const Url& u1, const Url& u2)
{
    return !(u1 == u2);
}

Url
operator+(const Url& u1, const std::string& u2)
{
    Url r = u1;
    r += u2;

    return r;
}

}
