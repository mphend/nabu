//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <mfcgi/exception.h>

#include <nabu/branch.h>
#include <nabu/filenamer.h>

#include "uri_branch.h"


namespace nabu {


void
GetBranchHandler::Process(fcgi::Request& request,
                        const fcgi::KeyedFieldMap& keyedFields,
                        const fcgi::ParameterMap& parameters)
{
    std::string branchPath;
    try {
        branchPath = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch name");
    }

    bool found = false;
    try {
        GetDatabase(keyedFields)->GetBranch(branchPath);
        found = true;
    } catch (const cor::Exception& err)
    {
    }

    Json::Value v = found;
    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


void
CreateBranchHandler::Process(fcgi::Request& request,
                        const fcgi::KeyedFieldMap& keyedFields,
                        const fcgi::ParameterMap& parameters)
{
    std::string branchPathStr;
    try {
        branchPathStr = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch name");
    }

    std::string problem;
    if (!nabu::FileNamer::ValidateLabelName(branchPathStr, problem))
    {
        throw cor::Exception("Branch '%s' is illegal name: %s", branchPathStr.c_str(), problem.c_str());
    }

    bool found = false;
    try {
        GetDatabase(keyedFields)->GetBranch(branchPathStr);
        found = true;
    } catch (const cor::Exception& err)
    {
    }
    if (found)
        throw cor::Exception("Branch '%s' already exists");

    GetDatabase(keyedFields)->CreateBranch(branchPathStr);
    fcgi::Header responseHeader;
    request.Reply(200, responseHeader);
}


void
MoveHeadHandler::Process(fcgi::Request& request,
                          const fcgi::KeyedFieldMap& keyedFields,
                          const fcgi::ParameterMap& parameters)
{
    std::string branchPath;
    try {
        branchPath = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch name in URL");
    }

    // body is JSON representation of the version for the tag
    nabu::Version version;
    Json::Value v;
    request.GetReader().Read(v);
    version.ParseFromJson(v);

    nabu::BranchPtr branch = GetDatabase(keyedFields)->GetBranch(branchPath);
    branch->MoveHead(version);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


void
CreateTagHandler::Process(fcgi::Request& request,
                         const fcgi::KeyedFieldMap& keyedFields,
                         const fcgi::ParameterMap& parameters)
{
    std::string branchPath;
    try {
        branchPath = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch name");
    }

    std::string tagName;
    try {
        tagName = parameters.GetString("name");
    }
    catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing tag name");
    }

    std::string problem;
    if (!FileNamer::ValidateLabelName(tagName, problem))
    {
        throw fcgi::Exception(400, "bad tag name, '%s': %s", tagName.c_str(), problem.c_str());
    }

    nabu::TagPolicy policy;
    try {
        policy = StringToTagPolicy(parameters.GetString("policy"));
    }
    catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing tag policy");
    }


    // body is JSON representation of the version which will be new head
    nabu::Version version;
    Json::Value v;
    request.GetReader().Read(v);
    version.ParseFromJson(v);

    nabu::BranchPtr branch = GetDatabase(keyedFields)->GetBranch(branchPath);
    //printf("Creating tag '%s' at version '%s'\n", tagName.c_str(), version.GetLiteral().c_str());
    TagPtr newTag = branch->CreateTag(tagName, policy, version);
    v.clear();
    newTag->RenderToJson(v);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}

void
GetHeadOfTagHandler::Process(fcgi::Request& request,
                                const fcgi::KeyedFieldMap& keyedFields,
                                const fcgi::ParameterMap& parameters)
{
    std::string branchPath;
    try {
        branchPath = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch name");
    }

    std::string tagName;
    try {
        tagName = keyedFields.at("tag_name");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing tag name");
    }

    nabu::BranchPtr branch = GetDatabase(keyedFields)->GetBranch(branchPath);
    nabu::BranchImp* b = dynamic_cast<nabu::BranchImp*>(branch.get());
    // SANITY
    if (!b)
        throw cor::Exception("GetHeadOfTagHandler -- could not get branch");
    nabu::MetaDataTable root = b->GetTagMetaData(tagName);

    std::string s;
    root->SaveDocument(s);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, s);
}


void
DeleteTagHandler::Process(fcgi::Request& request,
                             const fcgi::KeyedFieldMap& keyedFields,
                             const fcgi::ParameterMap& parameters)
{
    std::string branchPath;
    try {
        branchPath = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch name");
    }

    std::string tagName;
    try {
        tagName = keyedFields.at("tag_name");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing tag name");
    }

    nabu::BranchPtr branch = GetDatabase(keyedFields)->GetBranch(branchPath);
    bool b = branch->DeleteTag(tagName);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, b);
}


void
GetTagHandler::Process(fcgi::Request& request,
                          const fcgi::KeyedFieldMap& keyedFields,
                          const fcgi::ParameterMap& parameters)
{
    std::string branchPath;
    try {
        branchPath = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch name");
    }

    std::string tagName;
    try {
        tagName = keyedFields.at("tag_name");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing tag name");
    }

    nabu::BranchPtr branch = GetDatabase(keyedFields)->GetBranch(branchPath);
    TagPtr tag = branch->GetTag(tagName);

    Json::Value v;

    if (tag)
        tag->GetVersion().RenderToJson(v);
    else
        v = "No tag '" + tagName + "'";

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


void
ListTagsHandler::Process(fcgi::Request& request,
                             const fcgi::KeyedFieldMap& keyedFields,
                             const fcgi::ParameterMap& parameters)
{
    std::string branchPath;
    try {
        branchPath = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch name");
    }
    nabu::BranchPtr branch = GetDatabase(keyedFields)->GetBranch(branchPath);
    if (!branch)
       throw fcgi::Exception(404, "no branch '%s'", branchPath.c_str());

    std::set<std::string> tags;
    branch->GetTags(tags);

    // create JSON array of tag names
    Json::Value vr;
    std::set<std::string>::const_iterator i = tags.begin();
    for (int n = 0; i != tags.end(); n++, i++)
    {
        vr[n] = *i;
    }

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, vr);
}


void
VersionInUseHandler::Process(fcgi::Request& request,
                          const fcgi::KeyedFieldMap& keyedFields,
                          const fcgi::ParameterMap& parameters)
{
    nabu::TableAddress ta;
    // body is JSON representation of the table address
    nabu::Version version;
    Json::Value v;
    request.GetReader().Read(v);
    ta.ParseFromJson(v);

    bool inUse = GetDatabase(keyedFields)->VersionInUse(ta);

    Json::Value vr = inUse;
    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, vr);
}


void
GetUnusedVersionsHandler::Process(fcgi::Request& request,
                          const fcgi::KeyedFieldMap& keyedFields,
                          const fcgi::ParameterMap& parameters)
{
    nabu::TableAddress ta;

    ta.ParseFromJson(keyedFields.at("address"));
    std::string s;
    request.GetReader().Read(s);
    nabu::MetaDataMap diff(new nabu::MetaDataMapImp);
    diff->LoadDocument(s);

    GetDatabase(keyedFields)->GetUnusedVersions(ta, diff);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader);
}

void
TrimTagsHandler::Process(fcgi::Request& request,
                             const fcgi::KeyedFieldMap& keyedFields,
                             const fcgi::ParameterMap& parameters)
{
    std::string branchPath;
    try {
        branchPath = keyedFields.at("branch_path");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch name");
    }

    nabu::TableAddress ta;

    // body is JSON object which is set of tag names
    std::set<std::string> tagNames;
    Json::Value v;
    request.GetReader().Read(v);

    if (!v.isObject())
        throw fcgi::Exception(400, "Expected JSON object payload");
    Json::Value::iterator i = v.begin();
    try
    {
        for (; i != v.end(); i++)
            tagNames.insert(i.key().asString());
    }
    catch (...)
    {
        throw fcgi::Exception(400, "Expected tag names");
    }

    nabu::BranchPtr branch = GetDatabase(keyedFields)->GetBranch(branchPath);
    if (!branch)
        throw fcgi::Exception(404, "No such branch '%s'", branchPath.c_str());

    branch->TrimTags(tagNames);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}

} // end namespace
