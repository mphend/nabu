//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_FCGI_STREAM_H
#define GICM_FCGI_STREAM_H

#include <string>

#include <fcgiapp.h>

#include <json/value.h>
#include <mcor/mfile.h>
#include <mcor/mthread.h>


namespace fcgi {

class Header;

/** class Stream : wraps fcgi Stream
 *
 */
class Stream : public cor::File
{
public:
    Stream(FCGX_Stream* stream);
    virtual ~Stream();

    // File interface
    virtual void Open() {} // these objects cannot be opened
    virtual size_t Read(char* buffer, size_t count);
    virtual size_t Write(const char* buffer, size_t count);

    // string conveniences
    virtual void Read(std::string& s);
    // JSON
    virtual void Read(Json::Value& v);

    // binary conveniences
    virtual void Write8(uint8_t v);
    virtual void Write16(uint16_t v);
    virtual void Write32(uint32_t v);
    virtual void Write64(uint64_t v);

    virtual uint8_t Read8();
    virtual uint16_t Read16();
    virtual uint32_t Read32();
    virtual uint64_t Read64();

    // check stream error state, throws if set
    void Assert();
    // return error state
    bool CheckError();
    // return error description
    std::string GetError();

    // flush the stream; probably not useful
    void Flush();

protected:
    FCGX_Stream* mStream;

private:
    // prevent accidental copying
    Stream(const Stream& other);
    void operator=(const Stream& other);
};


}

#endif
