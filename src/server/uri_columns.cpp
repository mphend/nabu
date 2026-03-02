//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <mfcgi/exception.h>
#include <nabu/branch.h>

#include "uri_columns.h"


namespace nabu {


void
ColumnsHandler::Process(fcgi::Request& request,
                        const fcgi::KeyedFieldMap& keyedFields,
                        const fcgi::ParameterMap& parameters)
{
    std::vector<nabu::MetaKey> columns;

    std::string branchName;
    try {
        branchName = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch path");
    }

    nabu::MetaKey branchPath(branchName);
    {
        nabu::BranchPtr branch;
        branch = GetDatabase(keyedFields)->GetBranch(branchPath);
        if (!branch)
            throw fcgi::Exception(404, "no such branch '%s'", branchName.c_str());
        branch->GetColumns(nabu::MetaKey(), columns);
    }

    Json::Value v;
    v[(int)0]; // force to empty array type
    for (int i = 0; i < columns.size(); i++)
    {
        v[i] = columns[i].GetLiteral();
    }

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}





void
DeleteColumnBranchHandler::Process(fcgi::Request& request,
                             const fcgi::KeyedFieldMap& keyedFields,
                             const fcgi::ParameterMap& parameters)
{
    std::string columnString;

    try {
        columnString = keyedFields.at("column");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing column name");
    }

    std::string branchName;
    try {
        branchName = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch path");
    }

    {
        nabu::BranchPtr branch;
        branch = GetDatabase(keyedFields)->GetBranch(nabu::MetaKey(branchName));
        if (!branch)
            throw fcgi::Exception(404, "no such branch '%s'", branchName.c_str());
        branch->DeleteColumn(columnString);
    }

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader);
}


void
HasColumnHandler::Process(fcgi::Request& request,
                                   const fcgi::KeyedFieldMap& keyedFields,
                                   const fcgi::ParameterMap& parameters)
{
    std::string columnString;

    try {
        columnString = keyedFields.at("column");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing column name");
    }

    std::string branchName;
    try {
        branchName = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch path");
    }

    bool r;
    {
        nabu::BranchPtr branch;
        branch = GetDatabase(keyedFields)->GetBranch(nabu::MetaKey(branchName));
        if (!branch)
            throw fcgi::Exception(404, "no such branch '%s'", branchName.c_str());
        r = branch->HasColumn(columnString);
    }

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, r);
}


} // end namespace
