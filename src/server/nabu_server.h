//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_NABU_SERVER_H
#define GICM_NABU_SERVER_NABU_SERVER_H


#include <mfcgi/server.h>
#include <nabu/database_imp.h>
#include <nabu/io_op.h>

#include <mcor/resource_cache.h>

#include "database_map.h"

namespace nabu {

typedef cor::ResourceCache<IOOperationPtr> IOPCache;
typedef cor::ResourceAccessor<IOOperationPtr> IOP;

typedef cor::ResourceCache<AccessPtr> AccessCache;
typedef cor::ResourceAccessor<AccessPtr> IOAccess;

typedef cor::ResourceCache<TagPtr> TagCache;
typedef cor::ResourceAccessor<TagPtr> TagAccessor;

/** class Server : Wraps the library and listening socket
 *
 *  This is NOT a web server. It is not threadsafe; register everything
 *  before starting it. One request is handled at a time.
 *
 */
class Server : public fcgi::Server
{
public:
    // 'port' is the port number with which the web server will talk to this
    Server(DatabaseMap& databases, const std::string& prefix, unsigned int port);
    ~Server();

    void Start();
    void Stop();

    void ExecuteHandler(fcgi::Handler* handler,
                        fcgi::Request& request,
                        fcgi::KeyedFieldMap& keyed,
                        fcgi::ParameterMap& parameters);

protected:
    //nabu::DatabaseImp& mDataBase;
    DatabaseMap mDatabases;
    const std::string mPrefix;

    AccessCache mAccessCache;

    IOPCache mIOPCache;
    //TagCache mTagCache;

    // instantiate and attach handlers
    void CreateHandlers();
};

}

#endif
