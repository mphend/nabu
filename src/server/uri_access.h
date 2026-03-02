//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_URI_ACCESS_H
#define GICM_NABU_SERVER_URI_ACCESS_H


#include <mfcgi/handler.h>
#include <nabu/database.h>

#include "database_map.h"
#include "database_handler.h"
#include "nabu_server.h"

namespace nabu {


/* NewAccessHandler : returns new IO Operation handle
 *
 */
class NewAccessHandler : public fcgi::DatabaseHandler
{
public:
    NewAccessHandler(nabu::DatabaseMap& databaseMap, AccessCache& accessCache, IOPCache& iopCache) :
            fcgi::DatabaseHandler(databaseMap, "CreateAccess"),
            mAccessCache(accessCache),
            mIopCache(iopCache)
    {}
    virtual ~NewAccessHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

protected:
    AccessCache& mAccessCache;
    IOPCache& mIopCache;
};


/* QueryAccessHandler : returns if Access handle is Open (has access to its timerange)
 *
 */
class QueryAccessHandler : public fcgi::DatabaseHandler
{
public:
    // maxTimeoutSeconds is the amount of time that the Open will
    // block locally to implement a timeout. It should be short enough
    // to avoid web-server imposed keep-alive times, which are probably close
    // to 60 seconds.
    //
    // One thing to keep in mind is that unless the access is blocked in the
    // Open() call, it will not be considered to get access. You've got to wait
    // in line to get your turn, not step out of line and check back later.
    // So timeouts cannot be implemented on the client end, they must block
    // here. Also, a short timeout means that there is a greater density of
    // these short, but non-zero, periods of time in which the access might
    // become available but the caller is not in Open at that moment. So the
    // longer the client can tolerate the block, the better for fairness.
    //
    // Any length timeout, including infinite, can be implemented, but this is
    // the maximum period between polls during such a wait.
    QueryAccessHandler(nabu::DatabaseMap& databaseMap,
                       AccessCache& accessCache,
                       int maxTimeoutSeconds) :
            DatabaseHandler(databaseMap, "QueryAccess"),
            mAccessCache(accessCache),
            mMaxTimeoutSeconds(maxTimeoutSeconds)
    {}
    virtual ~QueryAccessHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

protected:
    AccessCache& mAccessCache;
    int mMaxTimeoutSeconds;
};


/* SelectAccessHandler : performs Select on an Access
 *
 */
class SelectAccessHandler : public fcgi::DatabaseHandler
{
public:
    // maxTimeoutSeconds is the amount of time that the Select will
    // block locally to implement a timeout; it should be at least less (perhaps
    // half) of the Access aging timeout, so that the access does not get
    // stale enough to be removed as inactive. But it must be also short enough
    // to avoid web-server imposed keep-alive times, which are probably closer
    // to 60 seconds.
    // Any length timeout, including infinite, can be implemented, but this is
    // the maximum period between polls during such a wait. So if in doubt, make
    // it shorter and live with a little chatty overhead.
    SelectAccessHandler(nabu::DatabaseMap& databases,
                        AccessCache& accessCache,
                        int maxTimeoutSeconds) :
            fcgi::DatabaseHandler(databases, "Select"),
            mAccessCache(accessCache),
            mMaxTimeoutSeconds(maxTimeoutSeconds)
    {}
    virtual ~SelectAccessHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

protected:
    AccessCache& mAccessCache;
    int mMaxTimeoutSeconds;
};


/* SelectAbortHandler : aborts a Select on an Access
 *
 */
class SelectAbortHandler : public fcgi::DatabaseHandler
{
public:

    SelectAbortHandler(nabu::DatabaseMap& databases,
                        AccessCache& accessCache) :
            fcgi::DatabaseHandler(databases, "Select"),
            mAccessCache(accessCache)
    {}
    virtual ~SelectAbortHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

protected:
    AccessCache& mAccessCache;
};

/* ExtentsAccessHandler : performs Extents on an Access
 *
 */
class ExtentsAccessHandler : public fcgi::DatabaseHandler
{
public:
    ExtentsAccessHandler(nabu::DatabaseMap& databases, AccessCache& accessCache) :
            fcgi::DatabaseHandler(databases, "GetAccessExtents"),
            mAccessCache(accessCache)
    {}
    virtual ~ExtentsAccessHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

protected:
    AccessCache& mAccessCache;
};

/* TickHandler : keeps access open
 *
 */
class TickHandler : public fcgi::DatabaseHandler
{
public:
    TickHandler(nabu::DatabaseMap& databases, AccessCache& accessCache) :
            fcgi::DatabaseHandler(databases, "Tick"),
            mAccessCache(accessCache)
    {}
    virtual ~TickHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

protected:
    AccessCache& mAccessCache;
};

/* EnableGCHandler : enables or disables garbage collection
 *
 */
class EnableGCHandler : public fcgi::DatabaseHandler
{
public:
    EnableGCHandler(nabu::DatabaseMap& databases, AccessCache& accessCache) :
            fcgi::DatabaseHandler(databases, "EnableGarbageCollection"),
            mAccessCache(accessCache)
    {}
    virtual ~EnableGCHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

protected:
    AccessCache& mAccessCache;
};

/* CloseAccessHandler : deletes Access
 *
 */
class CloseAccessHandler : public fcgi::DatabaseHandler
{
public:
    CloseAccessHandler(nabu::DatabaseMap& databases, AccessCache& accessCache) :
            fcgi::DatabaseHandler(databases, "DeleteAccess"),
            mAccessCache(accessCache)
    {}
    virtual ~CloseAccessHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

protected:
    AccessCache& mAccessCache;
};


}

#endif
