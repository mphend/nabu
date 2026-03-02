//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "exception.h"

namespace fcgi {

Exception::Exception(int code, const char* format, ... ) : mCode(code)
{
    char buffer[4056]; // lame length assumption

    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    mWhat = buffer;
    va_end (args);
}

Exception::~Exception() throw()
{
}

std::string
Exception::CodeStr() const
{
    char buffer[4]; // standard is limited to three-byte codes (in ASCII)
    snprintf(buffer, 4, "%d", mCode);
    return buffer;
}

}
