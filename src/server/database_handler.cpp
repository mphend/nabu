//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <mfcgi/exception.h>

#include "database_handler.h"


namespace fcgi {


nabu::DatabaseImpPtr
DatabaseHandler::GetDatabase(const fcgi::KeyedFieldMap &keyedFields)
{
    nabu::DatabaseImpPtr database;
    std::string dbName = keyedFields.at("database");
    try {
        if (mDatabaseMap.Get(dbName, database) == nabu::eOffline)
            throw fcgi::Exception(404, "Database '%s' is offline", dbName.c_str());
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(404, "Database '%s' does not exist", dbName.c_str());
    }

    return database;
}


} // end namespace
