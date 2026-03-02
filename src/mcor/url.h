//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_PKG_MCOR_URL
#define GICM_PKG_MCOR_URL

#include <arpa/inet.h>
#include <iostream>
#include <map>
#include <string>

#include <mcor/mexception.h>

namespace cor
{

/** class cor::Url
 *
 */
class Url
{
public:
    // parse string into URL
    // Expects:
    //    protocol://<host>[:port=80]/<resource>
    Url();
    Url(const std::string& s);
    virtual ~Url();

    bool Valid() const;

    typedef enum
    {
        eHttp,
        eHttps,
        eFtp,
        eFile,
        eS3,
        eUnknown
    } Protocol;

    // return protocol portion of URL
    std::string GetProtocolLiteral() const;
    Protocol GetProtocol() const;

    // decode enum into string
    static std::string ProtocolLiteral(Protocol);

    // return host portion of URL
	std::string GetHost() const;

    // return port number portion of URL
    int GetPort() const;

    // return resource potion of URL
    std::string GetResource() const;

    // return broken-down representation
    std::string Print() const;

    // return reconstituted URL
    std::string GetLiteral() const;

    // return the host and resource portion, ala
    //    '127.0.0.1/nabu/v1/columns'
    // with port and protocol (scheme) absent
    std::string GetHostAndResource() const;

    // set this object based on a compliant string; if not
    // valid, an exception is thrown
    void Set(const std::string& s);

    void operator+=(const std::string& s);

private:

    bool mValid;

    std::string mProtocolLiteral;
    Protocol mProtocol;
	std::string mHost;
    int mPort;
    std::string mResource;
};

bool operator==(const Url& u1, const Url& u2);
bool operator!=(const Url& u1, const Url& u2);

Url operator+(const Url& u1, const std::string& u2);

}

#endif
