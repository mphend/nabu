//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_URI_APPEND_H
#define GICM_NABU_SERVER_URI_APPEND_H


#include <mfcgi/handler.h>
#include <nabu/database.h>

#include "database_map.h"
#include "database_handler.h"

namespace nabu {



/* AppendHandler : appends new data
 *
 */
class AppendHandler : public fcgi::DatabaseHandler
{
public:
    AppendHandler(nabu::DatabaseMap& databases) :
            fcgi::DatabaseHandler(databases, "Append")
    {}
    virtual ~AppendHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);


};


}

#endif
