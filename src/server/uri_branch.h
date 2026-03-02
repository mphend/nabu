//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_NABU_SERVER_URI_BRANCH_H
#define GICM_NABU_SERVER_URI_BRANCH_H


#include <mfcgi/handler.h>
#include <nabu/database.h>

#include "database_handler.h"
#include "database_map.h"

namespace nabu {


/* GetBranchHandler : returns if branch exists
 *
 */
class GetBranchHandler : public fcgi::DatabaseHandler
{
public:
    GetBranchHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "GetBranch") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};


/* CreateBranchHandler : creates a branch
 *
 */
class CreateBranchHandler : public fcgi::DatabaseHandler
{
public:
    CreateBranchHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "CreateBranch") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};


/* MoveHeadHandler : moves head of branch
 *
 */
class MoveHeadHandler : public fcgi::DatabaseHandler
{
public:
    MoveHeadHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "MoveHead") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);

};


/* CreateTagHandler : creates a tag
 *
 */
class CreateTagHandler : public fcgi::DatabaseHandler
{
public:
    CreateTagHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "CreateTag") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};


/* GetHeadOfTagHandler : returns root MetaDataTable of tag
 *
 */
class GetHeadOfTagHandler : public fcgi::DatabaseHandler
{
public:
    GetHeadOfTagHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "GetHeadOfTag") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};


/* DeleteTagHandler : delete tag
 *
 */

class DeleteTagHandler : public fcgi::DatabaseHandler
{
public:
    DeleteTagHandler(nabu::DatabaseMap& databases) : fcgi::DatabaseHandler(databases, "DeleteTag") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};


/* GetTagHandler : returns version information of tag
 *
 */
class GetTagHandler : public fcgi::DatabaseHandler
{
public:
    GetTagHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "GetTag") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};


/* ListTagsHandler : returns tags on branch
 *
 */
class ListTagsHandler : public fcgi::DatabaseHandler
{
public:
    ListTagsHandler(nabu::DatabaseMap& databases) :
            fcgi::DatabaseHandler(databases, "GetTags") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};


/* VersionInUseHandler : determines if version of file is in use
 *
 */
class VersionInUseHandler : public fcgi::DatabaseHandler
{
public:
    VersionInUseHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "VersionInUse") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};


/* GetUnusedVersionsHandler : determines if version of file is in use
 *
 */
class GetUnusedVersionsHandler : public fcgi::DatabaseHandler
{
public:
    GetUnusedVersionsHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "GetUnusedVersions") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};

/* TrimTagsHandler : remove tags from database not named in payload
 *
 */
class TrimTagsHandler : public fcgi::DatabaseHandler
{
public:
    TrimTagsHandler(nabu::DatabaseMap& databases) :
        fcgi::DatabaseHandler(databases, "TrimTags") {}

    void Process(fcgi::Request& request,
                 const fcgi::KeyedFieldMap& keyedFields,
                 const fcgi::ParameterMap& parameters);
};
}

#endif
