//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_URI_DATABASE_H
#define GICM_NABU_SERVER_URI_DATABASE_H


#include <mfcgi/handler.h>
#include <nabu/database.h>
#include <nabu/filenamer.h>

#include "database_map.h"
#include "database_handler.h"

namespace nabu {


/* ListDatabasesHandler
 *
 */
class ListDatabasesHandler : public fcgi::Handler
{
public:
    ListDatabasesHandler(nabu::DatabaseMap& databaseMap) :
            fcgi::Handler("GetDatabases"),
            mDatabaseMap(databaseMap)
    {}
    virtual ~ListDatabasesHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

protected:
    nabu::DatabaseMap& mDatabaseMap;
};


/* TimeoutHandler : returns activity timeout in use by the server
 *
 */
class TimeoutHandler : public fcgi::Handler
{
public:
    TimeoutHandler(int timeoutSeconds) :
        fcgi::Handler("GetTimeout"), mTimeoutSeconds(timeoutSeconds) {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

protected:
    int mTimeoutSeconds;
};


/* FingerprintHandler : returns database fingerprint
 *
 */
class FingerprintHandler : public fcgi::DatabaseHandler
{
public:
    FingerprintHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "GetFingerprint") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);


};


/* TimePeriodsHandler : returns time periods
 *
 */
class TimePeriodsHandler : public fcgi::DatabaseHandler
{
public:
    TimePeriodsHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "GetTimePeriods") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);


};


/* InitHandler : creates a new local database unique to the universe
 *
 */
class InitHandler : public fcgi::Handler
{
public:
    InitHandler(const std::string& rootDirectory, nabu::DatabaseMap& databases) :
        fcgi::Handler("Init"),
        mRootDirectory(rootDirectory),
        mDatabases(databases)
        {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

    const std::string mRootDirectory;
    DatabaseMap& mDatabases;
};


/* CloneHandler : creates a new local database that is the clone of a remote
 *
 */
class CloneHandler : public fcgi::Handler
{
public:
    CloneHandler(const std::string& rootDirectory, nabu::DatabaseMap& databases) :
        fcgi::Handler("CloneDatabase"),
        mRootDirectory(rootDirectory),
        mDatabases(databases)
        {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

    const std::string mRootDirectory;
    DatabaseMap& mDatabases;
};

/* ReadFileHandler :
 *
 */
class ReadFileHandler : public fcgi::DatabaseHandler
{
public:
    ReadFileHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "ReadFile") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};

/* WriteFileHandler :
 *
 */
class WriteFileHandler : public fcgi::DatabaseHandler
{
public:
    WriteFileHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "WriteFile") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};

}

#endif
