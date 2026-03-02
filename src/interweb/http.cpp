//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <curl/curl.h>
#include <json/reader.h>
#include <json/writer.h>
#include <mcor/mmutex.h>
#include <mcor/mthread.h>
#include <mcor/strutil.h>

#include "http.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace http {

/*  Iolaus, running native (not in a container) was crashing in the call
 *  curl_easy_perform. I could not find any violation of memory access
 *  before this call, and it seemed to crash only when multiple threads
 *  entered and did not return immediately. In other words, either each
 *  thread entered and exited quickly, or they would all enter, and then
 *  a segmentation fault would occur before any would exit. It appears
 *  that libcurl is threadsafe, at least as it is used here (only local
 *  variables being used, signals being caught, etc.). However, there are
 *  some caveats about OpenSSL and other items, and experiments suggested
 *  that single-threading the curl_easy_perform call eliminated the
 *  segmentation fault.
 *
 *  --MPH Jan 2025
 */
cor::MPhonyMutex phonyCurlMutex("curlMutex");
cor::MMutex realCurlMutex("curlMutex");
cor::MMutex* curlMutex = &phonyCurlMutex; // by default, no single threading

void
SingleThreadLibCurl(bool yesSingleThreadIt)
{
    curlMutex = yesSingleThreadIt ? &realCurlMutex : &phonyCurlMutex;
}


static
void dump(const char *text,
          FILE *stream, unsigned char *ptr, size_t size)
{
    size_t i;
    size_t c;
    unsigned int width=0x10;

    fprintf(stream, "%s, %10.10ld bytes (0x%8.8lx)\n",
            text, (long)size, (long)size);

    for(i=0; i<size; i+= width) {
        fprintf(stream, "%4.4lx: ", (long)i);

        /* show hex to the left */
        for(c = 0; c < width; c++) {
            if(i+c < size)
                fprintf(stream, "%02x ", ptr[i+c]);
            else
                fputs("   ", stream);
        }

        /* show data on the right */
        for(c = 0; (c < width) && (i+c < size); c++) {
            char x = (ptr[i+c] >= 0x20 && ptr[i+c] < 0x80) ? ptr[i+c] : '.';
            fputc(x, stream);
        }

        fputc('\n', stream); /* newline */
    }
}

static
int my_trace(CURL *handle, curl_infotype type,
             char *data, size_t size,
             void *userp)
{
    const char *text;
    (void)handle; /* prevent compiler warning */
    (void)userp;

    switch (type) {
        case CURLINFO_TEXT:
            fprintf(stderr, "== Info: %s", data);
        default: /* in case a new one is introduced to shock us */
            return 0;

        case CURLINFO_HEADER_OUT:
            text = "=> Send header";
            break;
        case CURLINFO_DATA_OUT:
            text = "=> Send data";
            break;
        case CURLINFO_SSL_DATA_OUT:
            text = "=> Send SSL data";
            break;
        case CURLINFO_HEADER_IN:
            text = "<= Recv header";
            break;
        case CURLINFO_DATA_IN:
            text = "<= Recv data";
            break;
        case CURLINFO_SSL_DATA_IN:
            text = "<= Recv SSL data";
            break;
    }

    dump(text, stderr, (unsigned char *)data, size);
    return 0;
}


HttpSession::HttpSession(int port) : mHandle(NULL), mPort(port)
{
}


HttpSession::~HttpSession()
{
    if (mHandle != NULL)
        curl_easy_cleanup(mHandle);
}


CURL*
HttpSession::GetHandle()
{
    // handle once-per-process-lifetime library initialization; do this
    // in a thread-safe manner. This is why curl_easy_init() is wrapped
    // by this static, to make sure this gets called once before anyone
    // tries to use the curl library.
    //
    // Compliment of this is curl_global_cleanup().  This is not handled
    // right now, there would need to be a reference count and trust
    // that everyone cleaned up carefully or we would leak anyway.
    static cor::MMutex m("HttpSession::GetHandle");
    static bool globalInitialize = false;
    {
        cor::ObjectLocker ol(m);

        if (!globalInitialize)
        {
            CURLcode r;
            r = curl_global_init(CURL_GLOBAL_DEFAULT);
            if (r != 0)
                throw cor::Exception("curl_global_init() failed: %s", curl_easy_strerror(r));
            globalInitialize = true;
        }
    }

    CURL* r = curl_easy_init();
    if (r == NULL)
        throw cor::Exception("HttpSession::GetHandle() failed: curl_easy_init() returned no handle");

    return r;
}


void
HttpSession::Open()
{
    if (mHandle != NULL)
        return;

    mHandle = GetHandle();

    // this is supposed to avoid SIGALRM and
    // "*** longjmp causes uninitialized stack frame ***" errors
    // better is to build libcurl with c-ares support
    curl_easy_setopt(mHandle, CURLOPT_NOSIGNAL, 1);
}


size_t
HttpSession::WriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    DEBUG_LOCAL("WriteCallback entered%s\n", "");
    std::string* s = static_cast<std::string*>(userp);
    s->append((const char*)contents, size * nmemb);

    return size * nmemb;
}

size_t
HttpSession::BinaryWriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    DEBUG_LOCAL("BinaryWriteCallback entered%s\n", "");
    std::vector<unsigned char>* v = static_cast<std::vector<unsigned char>*>(userp);
    size_t end = v->size();
    v->resize(v->size() + size * nmemb);

    for (size_t i = 0; i < nmemb; i++)
    {
        (*v)[end + i] = ((unsigned char*)contents)[i];
    }

    return size * nmemb;
}

size_t
HttpSession::ReadCallback(void *ptr, size_t size, size_t nmemb, void *userp)
{
    DEBUG_LOCAL("ReadCallback entered%s\n", "");
    std::string* s = static_cast<std::string*>(userp);

    // minimum of length of data, or requested size
    size_t nBytes = size * nmemb;
    size_t copySize = nBytes > s->size() ? s->size() : nBytes;

    s->copy((char*)ptr, copySize, 0);
    s->erase(0, copySize);

    //printf("Payload is currently\n%s\n, s->c_str());

    return copySize;
}

size_t
HttpSession::BinaryReadCallback(void *ptr, size_t size, size_t nmemb, void *userp)
{
    DEBUG_LOCAL("BinaryReadCallback entered%s\n", "");
    std::vector<unsigned char>* v = static_cast<std::vector<unsigned char>*>(userp);

    // minimum of length of data, or requested size
    size_t nBytes = size * nmemb;
    size_t copySize = nBytes > v->size() ? v->size() : nBytes;

    //v->copy((char*)ptr, copySize, 0);
    //printf("BinaryReadCallback: sending %ld bytes\n", copySize);
    for(size_t i = 0; i < copySize; i++)
        ((char*)ptr)[i] = (*v)[i];
    v->erase(v->begin(), v->begin() + copySize);

    //printf("Payload is currently\n%s\n, s->c_str());

    return copySize;
}

size_t
HttpSession::HeaderCallback(void *ptr, size_t size, size_t nmemb, void *userp)
{
    DEBUG_LOCAL("HeaderCallback entered%s\n", "");
    std::string* s = static_cast<std::string*>(userp);

    size_t nBytes = size * nmemb;
    s->append((char*)ptr, nBytes);

    //printf("Header is currently:\n%s\n", s->c_str());

    return nBytes;
}

bool
HttpSession::Put(const std::string& type,
                 const std::string& url,
                 const std::string& send,
                 std::string& response,
                 bool post,
                 HeaderMap& headers,
                 long timeoutSec)
{
    DEBUG_LOCAL("HTTP::Put (string)...%s\n", "");
    std::vector<unsigned char> v(send.size());
    for (size_t i = 0;i < send.size(); i++)
        v[i] = send[i];
    return Put(type, url, v, response, post, headers, timeoutSec);
}

bool
HttpSession::Put(const std::string& type,
                 const std::string& url,
                 const std::vector<unsigned char>& send,
                 std::string& response,
                 bool post,
                 HeaderMap& headers,
                 long timeoutSec)
{
    DEBUG_LOCAL("HTTP::Put (string)...%s\n", "");
    std::vector<unsigned char> v(send.size());
    for (size_t i = 0;i < send.size(); i++)
        v[i] = send[i];
    std::vector<unsigned char> vr;
    bool b = Put(type, url, v, vr, post, headers, timeoutSec);
    cor::ToString(vr, response);
    return b;
}

bool
HttpSession::Put(const std::string& type,
                 const std::string& url,
                 const std::vector<unsigned char>& send,
                 std::vector<unsigned char>& response,
                 bool post,
                 HeaderMap& headers,
                 long timeoutSec)
{
    DEBUG_LOCAL("HTTP::Put (binary)...%s\n", "");
    DEBUG_LOCAL("   type = %s\n", type.c_str());
    DEBUG_LOCAL("   url = %s\n", url.c_str());
    DEBUG_LOCAL("   send.size() = %ld\n", send.size());
    DEBUG_LOCAL("   response.size() = %ld\n", response.size());
    DEBUG_LOCAL("   post = %s\n", post ? "true" : "false");
    DEBUG_LOCAL("   headers.size() = %ld\n", headers.size());
    DEBUG_LOCAL("   timeoutSec = %ld\n", timeoutSec);
    Open();

    CURLcode r;

    headers.InsertString("Accept", type);
    headers.InsertString("Content-Type", type);
    headers.InsertString("Charsets", "utf-8");
    // Internet says do not set "content-length" manually, as this goes around
    // the intended API. CURL will add "transfer-encoding" because it does not
    // think the length is known, but having both is illegal.
    // XXX does this work with PUT as well?

    // headersCurl* memory is managed by the Headers class, so do not
    // free it in any way
    const struct curl_slist* headersCurl = headers.GetCurlHeaders();

    //printf("Our headers:\n");
    //printf("%s\n", headers.GetLiteral().c_str());
    //printf("Curl headers:\n");
    //printf("%s\n", DumpCurlHeaders(headersCurl).c_str());

    try
    {
        // DEBUG
        //curl_easy_setopt(mHandle, CURLOPT_DEBUGFUNCTION, my_trace);
        /* the DEBUGFUNCTION has no effect until we enable VERBOSE */
        //curl_easy_setopt(mHandle, CURLOPT_VERBOSE, 1L);

        if ((r = curl_easy_setopt(mHandle, CURLOPT_URL, url.c_str())) != CURLE_OK)
            throw cor::Exception("CURLOPT_URL");

        if ((r = curl_easy_setopt(mHandle, post ? CURLOPT_POST : CURLOPT_PUT, 1)) != CURLE_OK)
            throw cor::Exception(post ? "CURLOPT_POST" : "CURLOPT_PUT");

        /* These do not appear to be necessary for https support; leaving them here
         * just to document.
         * MPH January 2025
        if ((r = curl_easy_setopt(mHandle, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1_3 )) != CURLE_OK)
            throw cor::Exception("CURLOPT_SSLVERSION");

        if ((r = curl_easy_setopt(mHandle, CURLOPT_SSL_VERIFYPEER, headersCurl)) != CURLE_OK)
            throw cor::Exception("CURLOPT_SSL_VERIFYPEER");

        if ((r = curl_easy_setopt(mHandle, CURLOPT_SSL_VERIFYHOST, headersCurl)) != CURLE_OK)
            throw cor::Exception("CURLOPT_SSL_VERIFYHOST");*/

        if ((r = curl_easy_setopt(mHandle, CURLOPT_HTTPHEADER, headersCurl)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HTTPHEADER");

        if ((r = curl_easy_setopt(mHandle, CURLOPT_PORT, mPort)) != CURLE_OK)
            throw cor::Exception("CURLOPT_PORT");

        if ((r = curl_easy_setopt(mHandle, CURLOPT_READFUNCTION, BinaryReadCallback)) != CURLE_OK)
            throw cor::Exception("CURLOPT_READFUNCTION");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_READDATA, &send)) != CURLE_OK)
            throw cor::Exception("CURLOPT_READDATA");

        if ((r = curl_easy_setopt(mHandle, CURLOPT_WRITEFUNCTION, BinaryWriteCallback)) != CURLE_OK)
            throw cor::Exception("CURLOPT_WRITEFUNCTION");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_WRITEDATA, &response)) != CURLE_OK)
            throw cor::Exception("CURLOPT_WRITEDATA");

        /*if ((r = curl_easy_setopt(mHandle, CURLOPT_INFILESIZE_LARGE, send.size())) != CURLE_OK)
            throw cor::Exception("HttpSession::Put(%s) failed: %s", type.c_str(), curl_easy_strerror(r));*/
        if ((r = curl_easy_setopt(mHandle, CURLOPT_POSTFIELDSIZE_LARGE, send.size())) != CURLE_OK)
            throw cor::Exception("CURLOPT_POSTFIELDSIZE_LARGE");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_POSTFIELDSIZE, send.size())) != CURLE_OK)
            throw cor::Exception("CURLOPT_POSTFIELDSIZE");

        if ((r = curl_easy_setopt(mHandle, CURLOPT_SSL_VERIFYPEER, 0L)) != CURLE_OK)
            throw cor::Exception("CURLOPT_SSL_VERIFYPEER");

        if ((r = curl_easy_setopt(mHandle, CURLOPT_SSL_VERIFYHOST, 0L)) != CURLE_OK)
            throw cor::Exception("CURLOPT_SSL_VERIFYHOST");

        if ((r = curl_easy_setopt(mHandle, CURLOPT_HEADERFUNCTION, HeaderCallback)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HEADERFUNCTION");
        std::string headerString;
        if ((r = curl_easy_setopt(mHandle, CURLOPT_HEADERDATA, &headerString)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HEADERDATA");

        // Note: this was previously commented out with this commit warning (Feb 13, 2020)
        //
        // "Avoid using the timeout feature (at least as written) of libcurl as this
        // uses signals. I have not seen an issue with this yet but it sounds like
        // other ways of handling timeouts are recommended instead of using this."
        //
        if (timeoutSec > 0)
        {
            // value of 0 is default, and means no timeout. There is no support for
            // 0 (immediate) timeout.
            if ((r = curl_easy_setopt(mHandle, CURLOPT_TIMEOUT, timeoutSec)) != CURLE_OK)
                throw cor::Exception("CURLOPT_TIMEOUT");
        }
        {
            cor::ObjectLocker ol(*curlMutex);
            r = curl_easy_perform(mHandle);
        }

        if (r == CURLE_OPERATION_TIMEDOUT)
            return false;

        headers.Parse(headerString);

        if (r != CURLE_OK)
            throw cor::Exception("");

    } catch (const cor::Exception& err)
    {
        throw cor::Exception("HttpSession::Put(%s) failed: %s\nURL=%s\nPort=%d",
                             err.what(),
                             curl_easy_strerror(r),
                             url.c_str(),
                             mPort);
    }

    return true;
}


bool
HttpSession::Get(const std::string& url, std::string& response, HeaderMap& headers, long timeoutSec)
{
    Open();
    DEBUG_LOCAL("HttpSession::Get()%s\n", "");
    response.clear();
    CURLcode r;

    try
    {
        if ((r = curl_easy_setopt(mHandle, CURLOPT_URL, url.c_str())) != CURLE_OK)
            throw cor::Exception("CURLOPT_URL");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_PORT, mPort)) != CURLE_OK)
            throw cor::Exception("CURLOPT_PORT");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_WRITEFUNCTION, WriteCallback)) != CURLE_OK)
            throw cor::Exception("CURLOPT_WRITEFUNCTION");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_HEADERFUNCTION, HeaderCallback)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HEADERFUNCTION");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_TIMEOUT, timeoutSec)) != CURLE_OK)
            throw cor::Exception("CURLOPT_TIMEOUT");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_WRITEDATA, &response)) != CURLE_OK)
            throw cor::Exception("CURLOPT_WRITEDATA");

        std::string headerString;
        if ((r = curl_easy_setopt(mHandle, CURLOPT_HEADERDATA, &headerString)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HEADERDATA");

        {
            cor::ObjectLocker ol(*curlMutex);
            r = curl_easy_perform(mHandle);
        }
        if (r == CURLE_OPERATION_TIMEDOUT)
            return false;
        if (r != CURLE_OK)
            throw cor::Exception("");

        headers.Parse(headerString);

        return true;
    } catch (const cor::Exception& err)
    {
        throw cor::Exception("HttpSession::Get(%s) failed: %s\nURL=%s\nPort=%d",
                             err.what(),
                             curl_easy_strerror(r),
                             url.c_str(),
                             mPort);
    }
}


bool
HttpSession::GetRaw(const std::string& url,
                    std::vector<unsigned char>& response,
                    HeaderMap& headers,
                    long timeoutSec)
{
    DEBUG_LOCAL("HttpSession::GetRaw%s\n","");
    Open();

    response.clear();
    CURLcode r;

    try
    {
        if ((r = curl_easy_setopt(mHandle, CURLOPT_URL, url.c_str())) != CURLE_OK)
            throw cor::Exception("CURLOPT_URL");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_PORT, mPort)) != CURLE_OK)
            throw cor::Exception("CURLOPT_PORT");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_WRITEFUNCTION, BinaryWriteCallback)) != CURLE_OK)
            throw cor::Exception("CURLOPT_WRITEFUNCTION");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_HEADERFUNCTION, HeaderCallback)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HEADERFUNCTION");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_TIMEOUT, timeoutSec)) != CURLE_OK)
            throw cor::Exception("CURLOPT_TIMEOUT");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_WRITEDATA, &response)) != CURLE_OK)
            throw cor::Exception("CURLOPT_WRITEDATA");

        std::string headerString;
        if ((r = curl_easy_setopt(mHandle, CURLOPT_HEADERDATA, &headerString)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HEADERDATA");

        {
            cor::ObjectLocker ol(*curlMutex);
            r = curl_easy_perform(mHandle);
        }
        if (r == CURLE_OPERATION_TIMEDOUT)
            return false;
        if (r != CURLE_OK)
            throw cor::Exception("");

        headers.Parse(headerString);

        return true;
    } catch (const cor::Exception& err)
    {
        throw cor::Exception("HttpSession::GetRaw(%s) failed: %s\nURL=%s\nPort=%d",
                             err.what(),
                             curl_easy_strerror(r),
                             url.c_str(),
                             mPort);
    }
}

bool
HttpSession::GetJson(const std::string& url,
                    Json::Value& out,
                    HeaderMap& headers,
                    long timeoutSec)
{
    DEBUG_LOCAL("HttpSession::GetJson%s\n","");

    std::string response;
    headers.InsertString("Content-type", "application/json");
    Get(url, response, headers, timeoutSec);

    Json::Reader reader;
    return reader.parse(response, out);
}

bool
HttpSession::Delete(const std::string& url,
                    const std::string& send,
                    std::string& response,
                    HeaderMap& headers,
                    long timeoutSec)
{
    DEBUG_LOCAL("HttpSession::Delete%s","");
    Open();

    response.clear();
    CURLcode r;

    try
    {
        if ((r = curl_easy_setopt(mHandle, CURLOPT_URL, url.c_str())) != CURLE_OK)
            throw cor::Exception("CURLOPT_URL");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_CUSTOMREQUEST, "DELETE")) != CURLE_OK)
            throw cor::Exception("CURLOPT_CUSTOMREQUEST");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_PORT, mPort)) != CURLE_OK)
            throw cor::Exception("CURLOPT_PORT");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_WRITEFUNCTION, WriteCallback)) != CURLE_OK)
            throw cor::Exception("CURLOPT_WRITEFUNCTION");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_HEADERFUNCTION, HeaderCallback)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HEADERFUNCTION");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_TIMEOUT, timeoutSec)) != CURLE_OK)
            throw cor::Exception("CURLOPT_TIMEOUT");
        if ((r = curl_easy_setopt(mHandle, CURLOPT_WRITEDATA, &response)) != CURLE_OK)
            throw cor::Exception("CURLOPT_WRITEDATA");

        std::string headerString;
        if ((r = curl_easy_setopt(mHandle, CURLOPT_HEADERDATA, &headerString)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HEADERDATA");

        {
            cor::ObjectLocker ol(*curlMutex);
            r = curl_easy_perform(mHandle);
        }
        if (r == CURLE_OPERATION_TIMEDOUT)
            return false;
        if (r != CURLE_OK)
            throw cor::Exception("");

        headers.Parse(headerString);

        return true;

    } catch (const cor::Exception& err)
    {
        throw cor::Exception("HttpSession::Delete(%s) failed: %s\nURL=%s\nPort=%d",
                             err.what(),
                             curl_easy_strerror(r),
                             url.c_str(),
                             mPort);
    }
}


bool
HttpSession::PutJson(const std::string& url,
                     const Json::Value& send,
                     Json::Value& response,
                     HeaderMap& headers,
                     long timeoutSec)
{
    std::string r;
    bool b = Put("application/json", url, Json::FastWriter().write(send), r, false, headers, timeoutSec);
    if (b)
    {
        Json::Reader reader;
        if (!reader.parse(r, response))
            throw cor::Exception("Bad JSON in response: %s", reader.getFormattedErrorMessages().c_str());
    }
    return b;
}

bool
HttpSession::PutPng(const std::string& url,
                    const std::string& send,
                    std::string& response,
                    HeaderMap& headers,
                    long timeoutSec)
{
    return Put("image/png", url, send, response, false, headers, timeoutSec);
}

bool
HttpSession::PutBinary(const std::string& url,
                        const std::vector<unsigned char>& send,
                        std::string& response,
                        HeaderMap& headers,
                        long timeoutSec)
{
    return Put("application/octet-stream", url, send, response, false, headers, timeoutSec);
}

bool
HttpSession::PostJson(const std::string& url,
                      const Json::Value& send,
                      Json::Value& response,
                      HeaderMap& headers,
                      long timeoutSec)
{
    Json::FastWriter writer;
    std::string r;
    bool b = Put("application/json", url, writer.write(send), r, true, headers, timeoutSec);
    if (b)
    {
        Json::Reader reader;
        if (!reader.parse(r, response))
            throw cor::Exception("Bad JSON in response: %s", reader.getFormattedErrorMessages().c_str());
    }
    return b;
}

bool
HttpSession::PostPng(const std::string& url,
                     const std::string& send,
                     std::string& response,
                     HeaderMap& headers,
                     long timeoutSec)
{
    return Put("image/png", url, send, response, true, headers, timeoutSec);
}

bool
HttpSession::PostBinary(const std::string& url,
                     const std::vector<unsigned char>& send,
                     std::string& response,
                     HeaderMap& headers,
                     long timeoutSec)
{
    return Put("application/octet-stream", url, send, response, true, headers, timeoutSec);
}

bool
HttpSession::PostBinary(const std::string& url,
                        const std::vector<unsigned char>& send,
                        std::vector<unsigned char>& response,
                        HeaderMap& headers,
                        long timeoutSec)
{
    return Put("application/octet-stream", url, send, response, true, headers, timeoutSec);
}

bool
HttpSession::PostText(const std::string& url,
                        const std::string& send,
                        std::string& response,
                        HeaderMap& headers,
                        long timeoutSec)
{
    return Put("text/plain", url, send, response, true, headers, timeoutSec);
}

}