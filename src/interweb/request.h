//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef PKG_INTERWEB_REQUEST_H_INCLUDED
#define PKG_INTERWEB_REQUEST_H_INCLUDED

#include <map>
#include <string>
#include <vector>

#include <curl/curl.h>
#include <json/value.h>
#include <mcor/url.h>

#include "http_headers.h"
#include "http_parameters.h"

namespace http {

// Content type
typedef enum {
    eBinary,
    eText,
    eJson,

    eUnknown
} ContentType;

std::string GetContentTypeString(ContentType type);

typedef std::vector<unsigned char> Binary;
typedef std::shared_ptr<Binary> BinaryPtr;
typedef std::shared_ptr<Json::Value> JsonPtr;
typedef std::shared_ptr<const Json::Value> JsonConstPtr;

class Request;
typedef std::shared_ptr<Request> RequestPtr;

const int defaultTimeoutSeconds = 30;

/** Class Request : an HTTP request or response
 *
 *  This class acts like a chimera, acting as any of several possible payloads
 *  types.
 *
 */
class Request
{
public:
    Request();
    virtual ~Request();

    ContentType GetType() const;

    // explicitly set the type; generally, this is not necessary, but is
    // useful to allow a 'forced' conversion. For instance, if data is
    // received as binary but is to be interpreted as JSON, etc.
    void SetType(ContentType type);

    // set content as JSON
    void SetJson(const Json::Value& v);
    void SetJson(JsonPtr v);

    // set content as text
    void SetText(const std::string& s);

    // set content as binary
    void SetBinary(const Binary& v);
    void SetBinary(const std::string& s);
    void SetBinary(BinaryPtr v);


    RequestPtr Get(const cor::Url& url,
                   const ParameterMap& parameterMap,
                   int timeoutSec = defaultTimeoutSeconds);
    RequestPtr Post(const cor::Url& url,
                    const ParameterMap& parameterMap,
                    int timeoutSec = defaultTimeoutSeconds);
    RequestPtr Put(const cor::Url& url,
                   const ParameterMap& parameterMap,
                   int timeoutSec = defaultTimeoutSeconds);
    RequestPtr Delete(const cor::Url& url,
                      const ParameterMap& parameterMap,
                      int timeoutSec = defaultTimeoutSeconds);

    // returns if there is any data, in any form
    bool Empty() const;

    // assert that payload is JSON or can be interpreted as JSON
    // and return it
    Json::Value GetJson() const;
    void GetJson(Json::Value& v) const;
    JsonPtr GetJsonPtr();
    JsonConstPtr GetJsonPtr() const;

    // assert that payload is text and return it
    std::string GetText() const;
    void GetText(std::string& s) const;

    // assert that payload is binary and return it
    Binary GetBinary() const;
    void GetBinary(Binary& v) const;
    // Do not use this to try to gain access to write the data; use
    // SetBinary() instead
    //
    // e.g.,
    //    BinaryPtr newData(new http::Binary);
    //    ...
    //    (do stuff to fill in newData)
    //    ...
    //    request.SetBinary(newData);
    BinaryPtr GetBinaryPtr();

    // return the numeric code, ala 200, 404, etc.
    int Code() const;
    // return the string following the code, ala "Bad Gateway"
    std::string CodeString() const;

    // throws an exception if Code() is not expectedCode
    void AssertReturnCode(int expectedCode) const;

    HeaderMap GetHeaders() const;
    void GetHeaders(HeaderMap& headerMap) const;
    // set the headers for outgoing request; this is normally not necessary
    void SetHeaders(const HeaderMap& headerMap);

    // debug dump of this object
    std::string Print() const;

protected:
    BinaryPtr mBinary;
    mutable JsonPtr   mJson;
    mutable bool  mJsonParsed; // tracks lazy parsing
    bool mAutomaticTyping;
    ContentType mType;
    HeaderMap mHeaders;

    static CURL* GetHandle();
    void Open(); // ensure handle is valid
    CURL* mHandle;

    // set the type of this by the 'Content-Type' header value
    // in contentType
    void SetType(const std::string& contentType);

    // convert mBinary into Json::Value and throw if invalid JSON
    void ParseJson() const;

    typedef enum {
        eGet,
        ePut,
        ePost,
        eDelete
    } Method;

    // returns a string describing the method
    static std::string MethodString(Method m);

    // Issues the request
    RequestPtr Transmit(const cor::Url& url,
                  Method method,
                  const ParameterMap& parameterMap,
                  long timeoutSec);
};

} // end namespace

#endif
