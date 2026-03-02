//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_FCGI_REQUEST_H
#define GICM_FCGI_REQUEST_H

#include <map>
#include <set>
#include <string>

#include <fcgiapp.h>

#include <mcor/istring_map.h>
#include <mcor/mfile.h>
#include <mcor/mthread.h>

#include "stream.h"

namespace fcgi {


/** class Environment : wrap of the stuff in the headers, but these
 *  are not the header directly; the key is specific to FCGI
 *
 */
class Environment : public cor::IStringMap
{
public:
    // overloaded to throw 400 error on miss
    int ReadInt(const std::string& key) const;
    std::string ReadString(const std::string& key) const;
};


/** class Request
 *
 */
class Request
{
public:
    Request(FCGX_Request* request);
    ~Request();

    Stream& GetReader() { return mReader; }
    Stream& GetWriter() { return mWriter; }
    Stream& GetError() { return mError; }
    // these are shortcuts, meant to avoid manual and error-prone
    // invocation of request->GetWriter().Write(*) stuff in the
    // correct order, remembering to set the status code, etc.
    // if type is NULL, sets content type to "application/json"
    void Reply(int code, Header& header, const std::string& s, const char* contentType = NULL);
    void Reply(int code, Header& header, const Json::Value& v);
    void Reply(int code, Header& header);
    // if type is NULL, sets content type to "application/octet-stream"
    void Reply(int code, Header& header, const std::vector<char>& data, const char* contentType = NULL);
    void Reply(int code, Header& header, const std::vector<unsigned char>& data, const char* contentType = NULL);

    const Environment& GetEnvironment();

    void Commit();

    // throws if writing was not successful
    void AssertWriterStream() { GetWriter().Assert(); }

protected:
    FCGX_Request* mRequest;
    Stream mReader;
    Stream mWriter;
    Stream mError;
    Environment mEnvironment;

    void ParseEnvironment();
    bool mEnvironmentValid;
};


}

#endif
