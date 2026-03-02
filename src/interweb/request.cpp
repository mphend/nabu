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

#include "request.h"

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

Request::Request() :
    mJsonParsed(false),
    mAutomaticTyping(true),
    mType(eUnknown),
    mHandle(NULL)
{
}

Request::~Request()
{
    if (mHandle != NULL)
        curl_easy_cleanup(mHandle);
}

std::string
GetContentTypeString(ContentType type)
{
    if (type == eBinary)
        return "application/octet-stream";
    else if (type == eText)
        return "text";
    else if (type == eJson)
        return "application/json";
    else if (type == eUnknown)
        throw cor::Exception("Type is not specified");
    else // SANITY
        throw cor::Exception("Type is not any of known types");
}


ContentType
Request::GetType() const
{
    return mType;
}

void
Request::SetType(ContentType type)
{
    mType = type;
}

void
Request::SetJson(const Json::Value& v)
{
    if (mAutomaticTyping)
        mType = eJson;
    mJson.reset(new Json::Value(v));

    Json::FastWriter writer;
    std::string document = writer.write(*mJson);
    mBinary.reset(new Binary(document.begin(), document.end()));
}

void
Request::SetJson(JsonPtr v)
{
    if (mAutomaticTyping)
        mType = eJson;
    mJson = v;

    Json::FastWriter writer;
    std::string document = writer.write(*mJson);
    mBinary.reset(new Binary(document.begin(), document.end()));
}

void
Request::SetText(const std::string& s)
{
    if (mAutomaticTyping)
        mType = eText;
    mBinary.reset(new Binary(s.begin(), s.end()));
}

void
Request::SetBinary(const Binary& v)
{
    if (mAutomaticTyping)
        mType = eBinary;
    mBinary.reset(new Binary(v));
}

void
Request::SetBinary(const std::string& s)
{
    if (mAutomaticTyping)
        mType = eBinary;
    mBinary.reset(new Binary(s.begin(), s.end()));
}

void
Request::SetBinary(BinaryPtr v)
{
    if (mAutomaticTyping)
        mType = eBinary;
    mBinary = v;
}

std::shared_ptr<Request>
Request::Get(const cor::Url& url,
             const ParameterMap& parameterMap,
             int timeoutSec)
{
    return Transmit(url, eGet, parameterMap, timeoutSec);
}

std::shared_ptr<Request>
Request::Post(const cor::Url& url,
              const ParameterMap& parameterMap,
              int timeoutSec)
{
    return Transmit(url, ePost, parameterMap, timeoutSec);
}

std::shared_ptr<Request>
Request::Put(const cor::Url& url,
             const ParameterMap& parameterMap,
             int timeoutSec)
{
    return Transmit(url, ePut, parameterMap, timeoutSec);
}

std::shared_ptr<Request>
Request::Delete(const cor::Url& url,
                const ParameterMap& parameterMap,
                int timeoutSec)
{
    return Transmit(url, eDelete, parameterMap, timeoutSec);
}

bool
Request::Empty() const
{
    return !mBinary;
}

Json::Value
Request::GetJson() const
{
    if (mType != eJson)
        throw cor::Exception("Content is not JSON");

    ParseJson();
    if (!mJson)
        return Json::Value();

    return *mJson;
}

void
Request::GetJson(Json::Value& v) const
{
    if (mType != eJson)
        throw cor::Exception("Content is not JSON");

    ParseJson();
    v.clear();
    if (mJson)
        v = *mJson;
}

JsonPtr
Request::GetJsonPtr()
{
    if (mType != eJson)
        throw cor::Exception("Content is not JSON");

    ParseJson();
    return mJson;
}

JsonConstPtr
Request::GetJsonPtr() const
{
    if (mType != eJson)
        throw cor::Exception("Content is not JSON");

    ParseJson();
    return mJson;
}

std::string
Request::GetText() const
{
    if (mType != eText)
        throw cor::Exception("Content is not text");

    if (!mBinary)
        return std::string();

    return std::string(mBinary->begin(), mBinary->end());
}

void
Request::GetText(std::string& s) const
{
    if (mType != eText)
        throw cor::Exception("Content is not text");

    s.clear();
    if (mBinary)
    {
        s.insert(s.begin(),
                 (char*)mBinary->data(),
                 (char*)mBinary->data() + mBinary->size());
    }
}

Binary
Request::GetBinary() const
{
    if (mType != eBinary)
        throw cor::Exception("Content is not binary");

    return *mBinary;
}

void
Request::GetBinary(Binary& v) const
{
    if (mType != eBinary)
        throw cor::Exception("Content is not binary");

    v.clear();
    if (mBinary)
        v = *mBinary;
}

BinaryPtr
Request::GetBinaryPtr()
{
    if (mType != eBinary)
        throw cor::Exception("Content is not binary");

    return mBinary;
}

int
Request::Code() const
{
    return mHeaders.Code();
}

std::string
Request::CodeString() const
{
    return mHeaders.CodeString();
}

void
Request::AssertReturnCode(int expectedCode) const
{
    std::string errStr = "";
    if (mBinary)
    {
        try {
            errStr = std::string(mBinary->begin(), mBinary->end());
        } catch (...)
        {
            printf("not a valid string\n");
            // not a valid string
        }
    }
    
    if (Code() != expectedCode)
        throw cor::Exception("%ld %s %s",
                             Code(),
                             CodeString().c_str(),
                             errStr.c_str());
}

HeaderMap
Request::GetHeaders() const
{
    return mHeaders;
}

void
Request::GetHeaders(HeaderMap& headerMap) const
{
    headerMap = mHeaders;
}

void
Request::SetHeaders(const http::HeaderMap &headerMap)
{
    mHeaders = headerMap;
}

std::string
Request::Print() const
{
    std::string s;
    s += "Headers:\n";
    s += mHeaders.Print();
    s += "Payload (" + GetContentTypeString(mType) + "):\n";
    if (mType == eJson)
    {
        JsonConstPtr jp = GetJsonPtr();
        s += Json::StyledWriter().write(*jp) + "\n";
    }
    else if (mType == eText)
    {
        s += GetText() + "\n";
    }
    else if (mType == eBinary)
    {
        std::ostringstream oss;
        oss << "<" << mBinary->size() << " bytes>";
        s += oss.str() + "\n";
    }

    return s;
}

CURL*
Request::GetHandle()
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
Request::Open()
{
    if (mHandle != NULL)
        return;

    mHandle = GetHandle();

    // this is supposed to avoid SIGALRM and
    // "*** longjmp causes uninitialized stack frame ***" errors
    // better is to build libcurl with c-ares support
    curl_easy_setopt(mHandle, CURLOPT_NOSIGNAL, 1);
}

void
Request::SetType(const std::string& contentType)
{
    if (contentType == "application/octet-stream")
        mType = eBinary;
    else if (contentType == "application/json")
        mType = eJson;
    else if (contentType == "text/html")
        mType = eText;
    else
        mType = eBinary; // anything else
}

void
Request::ParseJson() const
{
    if (!mJsonParsed && mBinary)
    {
        mJson.reset(new Json::Value());
        Json::Reader reader;
        if (!reader.parse((char*)mBinary->data(),
                          (char*) mBinary->data() + mBinary->size(),
                          *mJson))
        {
            throw cor::Exception("Content is not valid JSON: %s",
                                 reader.getFormattedErrorMessages().c_str());
        }
        mJsonParsed = true;
    }
}

size_t
BinaryReadCallback(void *ptr, size_t size, size_t nmemb, void *userp)
{
    DEBUG_LOCAL("BinaryReadCallback entered, size = %ld, nmemb = %ld\n", size, nmemb);
    Binary* v = static_cast<std::vector<unsigned char>*>(userp);

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
BinaryWriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    DEBUG_LOCAL("BinaryWriteCallback entered, size = %ld, nmemb = %ld\n", size, nmemb);
    Binary* v = static_cast<std::vector<unsigned char>*>(userp);
    size_t end = v->size();
    v->resize(v->size() + size * nmemb);

    for (size_t i = 0; i < nmemb; i++)
    {
        (*v)[end + i] = ((unsigned char*)contents)[i];
    }

    DEBUG_LOCAL("v->size = %ld\n", v->size());
    DEBUG_LOCAL("Read %ld bytes\n", size * nmemb);
    return size * nmemb;
}

size_t
HeaderReadCallback(void *ptr, size_t size, size_t nmemb, void *userp)
{
    DEBUG_LOCAL("HeaderReadCallback entered%s\n", "");
    std::string* s = static_cast<std::string*>(userp);

    size_t nBytes = size * nmemb;
    s->append((char*)ptr, nBytes);

    //printf("Header is currently:\n%s\n", s->c_str());

    return nBytes;
}

std::string
Request::MethodString(Method m)
{
    if (m == eGet)
        return "GET";
    else if (m == ePut)
        return "PUT";
    else if (m == ePost)
        return "POST";
    else if (m == eDelete)
        return "DELETE";
    else // SANITY
        throw cor::Exception("Unknown method 0x%x\n", (unsigned int)m);
}

RequestPtr
Request::Transmit(const cor::Url& url,
                  Method method,
                  const ParameterMap& parameterMap,
                  long timeoutSec)
{
    DEBUG_LOCAL("Request::Transmit()%s\n", "");
    DEBUG_LOCAL("   url = %s %s\n", MethodString(method).c_str(), url.GetLiteral().c_str());
    DEBUG_LOCAL("   type = %s\n", mBinary ? GetContentTypeString(mType).c_str() : "NA");
    DEBUG_LOCAL("   binary size() = %ld\n", mBinary ? mBinary->size() : 0);
    DEBUG_LOCAL("   headers.size() = %ld\n", mHeaders.size());
    DEBUG_LOCAL("   timeoutSec = %ld\n", timeoutSec);
    Open();

    CURLcode cr;

    // this is a buffer CURL will put error strings into
    char errorBuffer[CURL_ERROR_SIZE + 1];
    if ((cr = curl_easy_setopt(mHandle, CURLOPT_ERRORBUFFER, errorBuffer)) != CURLE_OK)
        throw cor::Exception("Could not set CURLOPT_ERRORBUFFER: %d", cr);

    // note that POST and PUT MUST have a content-type header
    // or else curl_easy_perform will hang
    if ((method == ePost) || (method == ePut))
    {
        if (!mBinary)
        {
            SetText(""); // empty document
        }
    }

    //headers.InsertString("Accept", type);
    if (mBinary)
        mHeaders.InsertString("Content-Type", GetContentTypeString(mType));
    mHeaders.InsertString("Charsets", "utf-8");
    // Internet says do not set "content-length" manually, as this goes around
    // the intended API. CURL will add "transfer-encoding" because it does not
    // think the length is known, but having both is illegal.
    // XXX does this work with PUT as well?

    // headersCurl* memory is managed by the Headers class, so do not
    // free it in any way
    const struct curl_slist* headersCurl = mHeaders.GetCurlHeaders();

    //printf("Our headers:\n");
    //printf("%s\n", headers.GetLiteral().c_str());
    //printf("Curl headers:\n");
    //printf("%s\n", DumpCurlHeaders(headersCurl).c_str());

    RequestPtr r(new Request());
    r->mBinary.reset(new Binary);

    try
    {
        // uncomment for DEBUG
        //curl_easy_setopt(mHandle, CURLOPT_DEBUGFUNCTION, my_trace);
        /* the DEBUGFUNCTION has no effect until we enable VERBOSE */
        curl_easy_setopt(mHandle, CURLOPT_VERBOSE, 0L);

        std::string urlString = parameterMap.BuildURI(url.GetHostAndResource());
        if ((cr = curl_easy_setopt(mHandle, CURLOPT_URL, urlString.c_str())) != CURLE_OK)
            throw cor::Exception("CURLOPT_URL");

        // CURL does not handle the different methods in a consistent way;
        //     GET is the default, so no options are set
        //     POST and PUT are direct options
        //     DELETE is a custom option
        if (method == eDelete)
        {
            if ((cr = curl_easy_setopt(mHandle, CURLOPT_CUSTOMREQUEST, "DELETE")) != CURLE_OK)
                throw cor::Exception("CURLOPT_CUSTOMREQUEST (DELETE)");
        }
        else if (method == ePost)
        {
            if ((cr = curl_easy_setopt(mHandle, CURLOPT_POST, 1)) != CURLE_OK)
                throw cor::Exception("CURLOPT_POST");
        }
        else if (method == ePut)
        {
                if ((cr = curl_easy_setopt(mHandle, CURLOPT_PUT, 1)) != CURLE_OK)
                    throw cor::Exception("CURLOPT_PUT");
        }

        /* These do not appear to be necessary for https support; leaving them here
         * just to document.
         * MPH January 2025
        if ((r = curl_easy_setopt(mHandle, CURLOPT_SSLVERSION, CURL_SSLVERSION_TLSv1_3 )) != CURLE_OK)
            throw cor::Exception("CURLOPT_SSLVERSION");

        if ((r = curl_easy_setopt(mHandle, CURLOPT_SSL_VERIFYPEER, headersCurl)) != CURLE_OK)
            throw cor::Exception("CURLOPT_SSL_VERIFYPEER");

        if ((r = curl_easy_setopt(mHandle, CURLOPT_SSL_VERIFYHOST, headersCurl)) != CURLE_OK)
            throw cor::Exception("CURLOPT_SSL_VERIFYHOST");*/
        if ((cr = curl_easy_setopt(mHandle, CURLOPT_FOLLOWLOCATION, 1)) != CURLE_OK)
            throw cor::Exception("CURLOPT_FOLLOWLOCATION");

        if ((cr = curl_easy_setopt(mHandle, CURLOPT_HTTPHEADER, headersCurl)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HTTPHEADER");

        if ((cr = curl_easy_setopt(mHandle, CURLOPT_PORT, url.GetPort())) != CURLE_OK)
            throw cor::Exception("CURLOPT_PORT");

        // set up read callback iff there is anything to send
        if (mBinary)
        {
            if ((cr = curl_easy_setopt(mHandle, CURLOPT_READFUNCTION, BinaryReadCallback)) != CURLE_OK)
                throw cor::Exception("CURLOPT_READFUNCTION");
            if ((cr = curl_easy_setopt(mHandle, CURLOPT_READDATA, mBinary.get())) != CURLE_OK)
                throw cor::Exception("CURLOPT_READDATA");

            /*if ((r = curl_easy_setopt(mHandle, CURLOPT_INFILESIZE_LARGE, send.size())) != CURLE_OK)
              throw cor::Exception("HttpSession::Put(%s) failed: %s", type.c_str(), curl_easy_strerror(r));*/
            if ((cr = curl_easy_setopt(mHandle, CURLOPT_POSTFIELDSIZE_LARGE, mBinary->size())) != CURLE_OK)
                throw cor::Exception("CURLOPT_POSTFIELDSIZE_LARGE");
            if ((cr = curl_easy_setopt(mHandle, CURLOPT_POSTFIELDSIZE, mBinary->size())) != CURLE_OK)
                throw cor::Exception("CURLOPT_POSTFIELDSIZE");
        }

        if ((cr = curl_easy_setopt(mHandle, CURLOPT_WRITEFUNCTION, BinaryWriteCallback)) != CURLE_OK)
            throw cor::Exception("CURLOPT_WRITEFUNCTION");
        if ((cr = curl_easy_setopt(mHandle, CURLOPT_WRITEDATA, r->mBinary.get())) != CURLE_OK)
            throw cor::Exception("CURLOPT_WRITEDATA");

        if ((cr = curl_easy_setopt(mHandle, CURLOPT_SSL_VERIFYPEER, 0L)) != CURLE_OK)
            throw cor::Exception("CURLOPT_SSL_VERIFYPEER");

        if ((cr = curl_easy_setopt(mHandle, CURLOPT_SSL_VERIFYHOST, 0L)) != CURLE_OK)
            throw cor::Exception("CURLOPT_SSL_VERIFYHOST");

        if ((cr = curl_easy_setopt(mHandle, CURLOPT_HEADERFUNCTION, HeaderReadCallback)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HEADERFUNCTION");
        std::string headerString;
        if ((cr = curl_easy_setopt(mHandle, CURLOPT_HEADERDATA, &headerString)) != CURLE_OK)
            throw cor::Exception("CURLOPT_HEADERDATA");

        // Note: this was previously commented out with this commit warning (Feb 13, 2020)
        //
        // "Avoid using the timeout feature (at least as written) of libcurl as this
        // uses signals. I have not seen an issue with this yet, but it sounds like
        // other ways of handling timeouts are recommended instead of using this."
        //
        if (timeoutSec > 0)
        {
            // value of 0 is default, and means no timeout. There is no support for
            // 0 (immediate) timeout.
            DEBUG_LOCAL("Setting timeout for %ld seconds\n", timeoutSec);
            if ((cr = curl_easy_setopt(mHandle, CURLOPT_TIMEOUT, timeoutSec)) != CURLE_OK)
                throw cor::Exception("CURLOPT_TIMEOUT");
        }

        {
            cor::ObjectLocker ol(*curlMutex);
            cr = curl_easy_perform(mHandle);
        }

        if (cr == CURLE_OPERATION_TIMEDOUT)
            throw cor::Exception("Timeout");
        if (cr != CURLE_OK)
        {
            throw cor::Exception("cr = %d", cr);
        }

        r->mHeaders.Parse(headerString);
        if (r->Code() != 204) // "NO CONTENTS", i.e., "Content-Type" will not exist
        {
        try {
                r->SetType(r->mHeaders.ReadString("Content-Type"));
            }
            catch (const cor::Exception& err)
            {
                printf("Error reading content-type: %s\n", err.what());
                printf("Header string:\n%s\n", headerString.c_str());
                printf("response:\n%s\n", r->GetText().c_str());
                throw;
            }
        }

    } catch (cor::Exception& err)
    {
        if (cr != CURLE_OK) // CURL error
        {
            err.SetWhat("error (URL %s) %s: %s %s",
                        url.GetLiteral().c_str(),
                        err.what(),
                        curl_easy_strerror(cr),
                        errorBuffer);
        }
        else  // some other error
        {
            err.SetWhat("error (URL %s) %s",
                        url.GetLiteral().c_str(),
                        err.what());
        }
        throw err;
    }

    return r;
}

}