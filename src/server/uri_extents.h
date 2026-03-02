//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_URI_EXTENTS_H
#define GICM_NABU_SERVER_URI_EXTENTS_H


#include <mfcgi/handler.h>
#include <nabu/database.h>

#include "database_map.h"
#include "database_handler.h"

namespace nabu {



/* ExtentsHandler : provides time extents of data
 *
 */
class ExtentsHandler : public fcgi::DatabaseHandler
{
public:
    ExtentsHandler(nabu::DatabaseMap& databases) :
            fcgi::DatabaseHandler(databases, "GetExtents")
    {}
    virtual ~ExtentsHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);


};


}

#endif
