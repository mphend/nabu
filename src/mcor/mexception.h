//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef __threader__mexception__
#define __threader__mexception__

#include <string>

namespace cor {
  
    
/** class Exception
 *
 */
class Exception : public std::exception
{
public:
    Exception() {}
    Exception(const char* format, ... );
    Exception(const Exception& other) throw();
    virtual ~Exception() throw();

    void SetWhat(const char* format, ... );
    
    const char* what() const throw() { return mWhat.c_str(); }

    operator bool() const { return !mWhat.empty(); }
    
protected:
    std::string mWhat;
};
 
    
/** class ErrException : produce an error string based on errno, the last
 *  error for the context, or a passed standard error code
 *
 */

class ErrException : public Exception
{
public:
    ErrException(const char* format, ... );
    ErrException(int errCode, const char* format, ... );
    virtual ~ErrException() throw();

    int Errno() const throw() { return mErrno; }

protected:
    int mErrno;

};
    
    
}

#endif
