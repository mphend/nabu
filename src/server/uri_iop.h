//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_URI_IOP_H
#define GICM_NABU_SERVER_URI_IOP_H


#include <mfcgi/handler.h>
#include <nabu/database.h>

#include "database_map.h"
#include "database_handler.h"
#include "nabu_server.h"

namespace nabu {


/* NewIopHandler : returns new IO Operation handle
 *
 */
class NewIopHandler : public fcgi::DatabaseHandler
{
public:
    NewIopHandler(nabu::DatabaseMap& databases, IOPCache& iopCache, AccessCache& accessCache) :
        fcgi::DatabaseHandler(databases, "CreateIOP"),
        mIOPCache(iopCache),
        mAccessCache(accessCache)
        {}
    virtual ~NewIopHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);


    IOPCache& mIOPCache;
    AccessCache& mAccessCache;
};


/* CloseIopHandler : deletes IO Operation
 *
 */
class CloseIopHandler : public fcgi::DatabaseHandler
{
public:
    CloseIopHandler(nabu::DatabaseMap& databases, IOPCache& iopCache) :
            fcgi::DatabaseHandler(databases, "DeleteIOP"),
            mIOPCache(iopCache)
    {}
    virtual ~CloseIopHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);


    IOPCache& mIOPCache;
};


/* ReadIopHandler : performs a record read
*
*/
class ReadIopHandler : public fcgi::DatabaseHandler
{
public:
    ReadIopHandler(nabu::DatabaseMap& databases, IOPCache& iopCache) :
            fcgi::DatabaseHandler(databases, "ReadData"),
            mIOPCache(iopCache)
    {}
    virtual ~ReadIopHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);


    IOPCache& mIOPCache;
};


/* ReadSeekIopHandler : Read seek operation
 *
 */
class ReadSeekIopHandler : public fcgi::DatabaseHandler
{
public:
    ReadSeekIopHandler(nabu::DatabaseMap& databases, IOPCache& iopCache) :
            fcgi::DatabaseHandler(databases, "ReadSeek"),
            mIOPCache(iopCache)
    {}
    virtual ~ReadSeekIopHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);


    IOPCache& mIOPCache;
};


/* WriteIopHandler : performs a record write
*
*/
class WriteIopHandler : public fcgi::DatabaseHandler
{
public:
    WriteIopHandler(nabu::DatabaseMap& databases, IOPCache& iopCache) :
            fcgi::DatabaseHandler(databases, "WriteData"),
            mIOPCache(iopCache)
    {}
    virtual ~WriteIopHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);


    IOPCache& mIOPCache;
};



/* CommitIopHandler : commits a Write operation
 *
 */
class CommitIopHandler : public fcgi::DatabaseHandler
{
public:
    CommitIopHandler(nabu::DatabaseMap& databases, IOPCache& iopCache) :
            fcgi::DatabaseHandler(databases, "Commit"),
            mIOPCache(iopCache)
    {}
    virtual ~CommitIopHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);


    IOPCache& mIOPCache;
};



/* SearchIopHandler : search operation
 *
 */
class SearchIopHandler : public fcgi::DatabaseHandler
{
public:
    SearchIopHandler(nabu::DatabaseMap& databases, IOPCache& iopCache) :
            fcgi::DatabaseHandler(databases, "Search"),
            mIOPCache(iopCache)
    {}
    virtual ~SearchIopHandler() {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);


    IOPCache& mIOPCache;
};


}

#endif
