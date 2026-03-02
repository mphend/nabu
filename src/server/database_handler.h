//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_NABU_HANDLER_H
#define GICM_NABU_SERVER_NABU_HANDLER_H


#include <mfcgi/handler.h>
#include <nabu/database.h>

#include "database_map.h"
#include "nabu_server.h"

namespace fcgi {


/* DatabaseHandler
 *
 */
class DatabaseHandler : public fcgi::Handler
{
public:
    DatabaseHandler(nabu::DatabaseMap& databaseMap, const std::string& name) :
            fcgi::Handler(name),
            mDatabaseMap(databaseMap)
    {}
    virtual ~DatabaseHandler() {}

    // return the database, checking for existence and online status
    // and throwing appropriate error codes if otherwise
    nabu::DatabaseImpPtr GetDatabase(const fcgi::KeyedFieldMap& keyedFieldMap);

protected:
    nabu::DatabaseMap& mDatabaseMap;
};


}

#endif
