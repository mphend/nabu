//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __interweb_http_
#define __interweb_http_

#include <map>
#include <string>
#include <vector>

#include <curl/curl.h>
#include <json/value.h>
#include <mcor/mmutex.h>
#include <mcor/mthread.h>
#include <mcor/mtime.h>

#include "http_headers.h"
#include "http_parameters.h"

namespace http {


// Put a global mutex around libcurl calls; see implementation for
// rational and history
void SingleThreadLibCurl(bool yesSingleThreadIt);

/** Class HttpSession : a transaction with an HTTP server
 *
 */
class HttpSession
{
public:
    HttpSession(int port);
    virtual ~HttpSession();

    // returns true if success, false if timeout, and throws otherwise
    // timeout of 0 or lower means wait forever
    bool Get(const std::string& url, std::string& response, HeaderMap& headers, long timeoutSec = 10);
    bool GetRaw(const std::string& url, std::vector<unsigned char>& response, HeaderMap& headers, long timeoutSec = 10);
    bool GetJson(const std::string& url, Json::Value& response, HeaderMap& headers, long timeoutSec = 10);
    bool Delete(const std::string& url, const std::string& send, std::string& response, HeaderMap& headers, long timeoutSec = 10);
    bool PutJson(const std::string& url, const Json::Value& send, Json::Value& response,  HeaderMap& headers, long timeoutSec = 10);
    bool PutJsonB(const std::string& url, const Json::Value& send, std::vector<unsigned char>& response,  HeaderMap& headers, long timeoutSec = 10);
    bool PutPng(const std::string& url, const std::string& send, std::string& response, HeaderMap& headers, long timeoutSec = 10);
    bool PutBinary(const std::string& url, const std::vector<unsigned char>& send, std::string& response, HeaderMap& headers, long timeoutSec = 10);
    bool PostJson(const std::string& url, const Json::Value& send, Json::Value& response, HeaderMap& headers, long timeoutSec = 10);
    bool PostPng(const std::string& url, const std::string& send, std::string& response, HeaderMap& headers, long timeoutSec = 10);
    bool PostBinary(const std::string& url, const std::vector<unsigned char>& send, std::string& response, HeaderMap& headers, long timeoutSec = 10);
    bool PostBinary(const std::string& url, const std::vector<unsigned char>& send, std::vector<unsigned char>& response, HeaderMap& headers, long timeoutSec = 10);
    bool PostText(const std::string& url, const std::string& send, std::string& response, HeaderMap& headers, long timeoutSec = 10);

private:
    static CURL* GetHandle();
    CURL* mHandle;
    int mPort;

    static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp);
    static size_t BinaryWriteCallback(void *contents, size_t size, size_t nmemb, void *userp);
    static size_t ReadCallback(void *contents, size_t size, size_t nmemb, void *userp);
    static size_t BinaryReadCallback(void *contents, size_t size, size_t nmemb, void *userp);
    static size_t HeaderCallback(void *contents, size_t size, size_t nmemb, void *userp);

    bool Put(const std::string& type,
             const std::string& url,
             const std::string& send,
             std::string& response,
             bool post,
             HeaderMap& headers,
             long timeoutSec = 10);

    bool Put(const std::string& type,
             const std::string& url,
             const std::vector<unsigned char>& send,
             std::string& response,
             bool post,
             HeaderMap& headers,
             long timeoutSec = 10);

    bool Put(const std::string& type,
             const std::string& url,
             const std::vector<unsigned char>& send,
             std::vector<unsigned char>& response,
             bool post,
             HeaderMap& headers,
             long timeoutSec = 10);

    void Open();
};

} // end namespace

#endif
