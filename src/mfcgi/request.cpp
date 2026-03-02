//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <errno.h>
#include <stdio.h>
#include <sstream>

#include <json/writer.h>
#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include "exception.h"
#include "header.h"
#include "request.h"

namespace fcgi {


int
Environment::ReadInt(const std::string& key) const
{
    try {
        return IStringMap::ReadInt(key);
    }
    catch (const cor::Exception& err)
    {
        throw fcgi::Exception(400, "Header '%s' is not an integer", key.c_str());
    }
}

std::string
Environment::ReadString(const std::string& key) const
{
    try {
        return IStringMap::ReadString(key);
    }
    catch (const cor::Exception& err)
    {
        throw fcgi::Exception(400, "Missing header '%s'", key.c_str());
    }
}




Request::Request(FCGX_Request* request) : mRequest(request),
                                          mReader(request->in),
                                          mWriter(request->out),
                                          mError(request->err),
                                          mEnvironmentValid(false)
{
}

Request::~Request()
{
    const int close_ipc_fd = 0;
    FCGX_Free(mRequest, close_ipc_fd);
    mRequest = NULL;
}

void
Request::Reply(int code, Header& header, const std::string& s, const char* contentType)
{
    if (contentType == NULL)
        header.Set("content-Type", "application/json");
    else
        header.Set("content-Type", contentType);
    header.SetInt("status", code);
    header.Print(GetWriter());
    GetWriter().Write(s.c_str(), s.size());
}

void
Request::Reply(int code, Header& header, const Json::Value& v)
{
    Json::FastWriter writer;
    std::string document = writer.write(v);
    Reply(code, header, document, "application/json");
}

void
Request::Reply(int code, Header& header)
{
    Json::Value v;
    Reply(code, header, v);
}

void
Request::Reply(int code, Header& header,
               const std::vector<char>& data,
               const char* contentType)
{
    if (contentType == NULL)
        header.Set("content-Type", "application/octet-stream");
    else
        header.Set("content-Type", contentType);
    header.SetInt("status", code);
    header.Print(GetWriter());
    GetWriter().Write(data.data(), data.size());
}

void
Request::Reply(int code, Header& header,
               const std::vector<unsigned char>& data,
               const char* contentType)
{
    if (contentType == NULL)
        header.Set("content-Type", "application/octet-stream");
    else
        header.Set("content-Type", contentType);
    header.SetInt("status", code);
    header.Print(GetWriter());
    GetWriter().Write((char*)data.data(), data.size());
}

const Environment&
Request::GetEnvironment()
{
    if (!mEnvironmentValid)
    {
        ParseEnvironment();
        mEnvironmentValid = true;
    }
    return mEnvironment;
}

void
Request::Commit()
{
    FCGX_Finish_r(mRequest);
}

void
Request::ParseEnvironment()
{
    char** p = mRequest->envp;
    for (; *p != NULL; p++)
    {
        std::string line = *p;

        std::string::size_type equals = line.find('=');
        if (equals == std::string::npos)
            throw cor::Exception("Format error in environment line '%s': no '='", line.c_str());
        std::string key = line.substr(0, equals);
        std::string value = line.substr(equals + 1);
        mEnvironment.InsertString(key, value);
    }
}


}
