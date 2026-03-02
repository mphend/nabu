//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef MFCGI_EXCEPTION_INCLUDED
#define MFCGI_EXCEPTION_INCLUDED

#include <mcor/mexception.h>

namespace fcgi {
  
    
/** class Exception
 *
 */
class Exception : public cor::Exception
{
public:
    Exception() {}
    Exception(int code, const char* format, ... );
    virtual ~Exception() throw();

    unsigned int Code() const { return mCode; }
    std::string CodeStr() const;

protected:
    unsigned int mCode;
};
    
    
}

#endif
