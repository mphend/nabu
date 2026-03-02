//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_URI_SYNC_H
#define GICM_NABU_SERVER_URI_SYNC_H


#include <mfcgi/handler.h>
#include <nabu/database_imp.h>

#include "database_map.h"
#include "database_handler.h"

namespace nabu {


/* SyncHandler : syncs database to a directory on the same machine
 *
 */
class SyncHandler : public fcgi::DatabaseHandler
{
public:
    SyncHandler(nabu::DatabaseMap& databases) :
            fcgi::DatabaseHandler(databases, "SyncDatabaseLocal")
    {}
    virtual ~SyncHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};


}

#endif
