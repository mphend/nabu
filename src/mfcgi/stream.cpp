//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <errno.h>

#include <json/reader.h>
#include <json/writer.h>
#include <mcor/binary.h>
#include <mcor/mexception.h>


#include "exception.h"
#include "header.h"
#include "stream.h"

namespace fcgi {

Stream::Stream(FCGX_Stream* stream) :
    mStream(stream)
{}

Stream::~Stream()
{
    // Do NOT call this; causes access of memory free'd by ~Request
    //FCGX_FClose(mStream);
}

size_t
Stream::Read(char* buffer, size_t count)
{
    size_t n = FCGX_GetStr(buffer, count, mStream);
    if (n == -1)
        throw cor::Exception("Stream::Read() %s\n", GetError().c_str());
    return n;
}

void
Stream::Read(std::string& s)
{
    char buffer[201];
    while (1)
    {
        size_t n = Read(buffer, 200);
        buffer[n] = 0; // null terminate
        s += buffer;
        if (n != 200)
            break;
    }
}

size_t
Stream::Write(const char* buffer, size_t count)
{
    size_t n = FCGX_PutStr(buffer, count, mStream);
    if (n == -1)
        throw cor::Exception("Stream::Write() %s\n", GetError().c_str());
    if (n != count)
        throw cor::Exception("Stream::Write() -- only %ld of %ld characters written %s", n, count, GetError().c_str());
    return n;
}

void
Stream::Read(Json::Value& root)
{
    Json::Reader reader;

    std::string document;
    Read(document);
    if (!reader.parse(document, root))
    {
    throw fcgi::Exception(400, "bad JSON document: %s", reader.getFormattedErrorMessages().c_str());
    }
}

void
Stream::Write8(uint8_t v)
{
    Write((char*)&v, 1);
}

void
Stream::Write16(uint16_t v)
{
    unsigned char buffer[2];
    cor::Pack16(buffer, v);
    Write((char*)buffer, 2);
}

void
Stream::Write32(uint32_t v)
{
    unsigned char buffer[4];
    cor::Pack32(buffer, v);
    Write((char*)buffer, 4);
}

void
Stream::Write64(uint64_t v)
{
    unsigned char buffer[8];
    cor::Pack64(buffer, v);
    Write((char*)buffer, 8);
}

uint8_t
Stream::Read8()
{
    uint8_t r;
    size_t n = Read((char*)&r, 1);
    if (n == -1)
        throw cor::Exception("Stream::Read8 -- %s\n", GetError().c_str());
    if (n != 1)
        throw Exception(400, "Unexpected EOF");
    return r;
}

uint16_t
Stream::Read16()
{
    unsigned char buffer[2];
    size_t n = Read((char*)buffer, 2);
    if (n == -1)
        throw cor::Exception("Stream::Read16 -- %s\n", GetError().c_str());
    if (n != 2)
        throw Exception(400, "Unexpected EOF");
    return cor::Unpack16(buffer);
}

uint32_t
Stream::Read32()
{
    unsigned char buffer[4];
    size_t n = Read((char*)buffer, 4);
    if (n == -1)
        throw cor::Exception("Stream::Read32 -- %s\n", GetError().c_str());
    if (n != 4)
    {
        throw Exception(400, "Unexpected EOF");
    }
    return cor::Unpack32(buffer);
}

uint64_t
Stream::Read64()
{
    unsigned char buffer[8];
    size_t n = Read((char*)buffer, 8);
    if (n == -1)
        throw cor::Exception("Stream::Read64 -- %s\n", GetError().c_str());
    if (n != 8)
        throw Exception(400, "Unexpected EOF");
    return cor::Unpack64(buffer);
}

void
Stream::Assert()
{
    if (CheckError())
    {
        cor::Exception err = cor::Exception("Stream error: %s", GetError().c_str());
        FCGX_ClearError(mStream);
        throw err;
    }
}

bool
Stream::CheckError()
{
    return FCGX_GetError(mStream) != 0;
}

std::string
Stream::GetError()
{
    int rc = FCGX_GetError(mStream);
    if (rc == 0)
        return "no error";
    if (rc > 0)
    {
        cor::ErrException err("");
        return err.what();
    }
    // rc < 0)
    if (rc == FCGX_UNSUPPORTED_VERSION)
        return "unsupported version";
    else if (rc == FCGX_PROTOCOL_ERROR)
        return "protocol error";
    else if (rc == FCGX_PARAMS_ERROR)
        return "parameter error";
    else if (rc == FCGX_CALL_SEQ_ERROR)
        return "call sequence error";
    else
        return "unknown FCGX error";

}

void
Stream::Flush()
{
    FCGX_FFlush(mStream);
}

}

