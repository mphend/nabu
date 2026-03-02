//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include <mcor/json_time.h>
#include <json/writer.h>
#include <mfcgi/exception.h>
#include <mcor/strutil.h>

#include <nabu/branch.h>

#include "uri_extents.h"


namespace nabu {


void
ExtentsHandler::Process(fcgi::Request& request,
                        const fcgi::KeyedFieldMap& keyedFields,
                        const fcgi::ParameterMap& parameters)
{
    nabu::MetaKey branchPath;
    try {
        std::string branchPathStr = keyedFields.at("branch_path");
        if (!cor::IEquals(branchPathStr, "null")) // 'null' means invalid
            branchPath = branchPathStr;
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch name");
    }

    std::string columnString;
    try {
        columnString = keyedFields.at("column");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing column name");
    }

    std::string type = parameters.GetString("type", "read");
    bool write;
    if (cor::IEquals(type, "read"))
        write = false;
    else if (cor::IEquals(type, "write"))
        write = true;
    else
        throw fcgi::Exception(400, "invalid value for parameter 'type', '%s'", type.c_str());

    MetaKey column(columnString, ':');
    cor::TimeRange tr;
    {
        tr = GetDatabase(keyedFields)->GetBranch(branchPath)->Extents(
                column, write ? Branch::eWriteable : Branch::eReadable);
    }

    /*if (!tr)
        throw fcgi::Exception(404, "no column '%s'", columnString.c_str());*/

    Json::Value v;
    Json::TimeRangeToJson(tr, v);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


} // end namespace
