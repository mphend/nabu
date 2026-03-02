//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __gicm_nabu_exception__
#define __gicm_nabu_exception__

#include <mcor/mexception.h>
#include <mcor/url.h>

namespace nabu {
  
    
/** class FatalException
 *
 */
class FatalException : public cor::Exception
{
public:
    FatalException() {}
    FatalException(const cor::Url& databaseUrl, const char* format, ... );
    virtual ~FatalException() throw();

    const cor::Url mDatabaseUrl;
};


/** class SearchException : an exception caused by a badly-formatted search query
 *
 */
class SearchException : public cor::Exception
{
public:
    SearchException() {}
    SearchException(const char* format, ... );
    virtual ~SearchException() throw();
};
    
    
}

#endif
