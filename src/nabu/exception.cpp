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

namespace nabu {
    
FatalException::FatalException(const cor::Url& databaseUrl, const char* format, ... ) :
        mDatabaseUrl(databaseUrl)
{
    char buffer[4056]; // lame length assumption
    
    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    mWhat = buffer;
    va_end (args);
}

FatalException::~FatalException() throw()
{
}


SearchException::SearchException(const char* format, ... )
{
    char buffer[4056]; // lame length assumption

    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    mWhat = buffer;
    va_end (args);
}

SearchException::~SearchException() throw()
{
}
    
    
}
