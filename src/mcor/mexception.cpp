//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include "mexception.h"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

namespace cor {
    
Exception::Exception(const char* format, ... )
{
    char buffer[4056]; // lame length assumption
    
    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    mWhat = buffer;
    va_end (args);
}

Exception::Exception(const Exception& other) throw()
{
    mWhat = other.mWhat;
}

Exception::~Exception() throw()
{
}

void
Exception::SetWhat(const char* format, ... )
{
    char buffer[4056]; // lame length assumption

    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    mWhat = buffer;
    va_end (args);
}
    
ErrException::ErrException(const char* format, ... ) : Exception(), mErrno(0)
{
    char buffer[4056]; // lame length assumption
    
    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    mWhat = buffer;
    mWhat += " -- ";
    mWhat += strerror(errno);
    va_end (args);
    mErrno = errno;
}

ErrException::ErrException(int errCode, const char* format, ... ) : Exception(), mErrno(errCode)
{
    char buffer[4056]; // lame length assumption

    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    mWhat = buffer;
    mWhat += " -- ";
    mWhat += strerror(mErrno);
    va_end(args);
}

ErrException::~ErrException() throw()
{
}
    
    
}
