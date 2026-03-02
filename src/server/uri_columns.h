//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_URI_COLUMNS_H
#define GICM_NABU_SERVER_URI_COLUMNS_H


#include <mfcgi/handler.h>
#include <nabu/database.h>


#include "database_handler.h"
#include "database_map.h"

namespace nabu {


/* ColumnsHandler : returns columns
 *
 */
class ColumnsHandler : public fcgi::DatabaseHandler
{
public:
    ColumnsHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "GetColumns") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

};


/* DeleteColumBranchHandler : deletes a column from a branch
 *
 */
class DeleteColumnBranchHandler : public fcgi::DatabaseHandler
{
public:
    DeleteColumnBranchHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "DeleteBranchColumn") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

};


/* HasColumnHandler : returns whether a column exists
 *
 */
class HasColumnHandler : public fcgi::DatabaseHandler
{
public:
    HasColumnHandler(nabu::DatabaseMap& databases) :
            fcgi::DatabaseHandler(databases, "HasColumnHandler") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

};

}

#endif
