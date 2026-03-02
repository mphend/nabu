//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <string.h>

#include <mcor/binary.h>
#include <mfcgi/exception.h>
#include <mcor/strutil.h>

#include <nabu/filenamer.h>

#include "uri_access.h"


namespace nabu {


void
NewAccessHandler::Process(fcgi::Request& request,
                       const fcgi::KeyedFieldMap& keyedFields,
                       const fcgi::ParameterMap& parameters)
{
    std::vector<nabu::MetaKey> columns;
    cor::TimeRange tr;

    nabu::MetaKey branchPath;
    try {
        std::string branchPathStr = keyedFields.at("branch_path");
        if (!cor::IEquals(branchPathStr, "null")) // 'null' means invalid
            branchPath = branchPathStr;
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing branch name");
    }

    DatabasePtr database = GetDatabase(keyedFields);

    tr.First() = parameters.GetTime("start", cor::Time::NegativeInf());
    tr.Final() = parameters.GetTime("end", cor::Time::PositiveInf());
    std::string typeS = parameters.GetString("type");
    const std::string noTag = "";
    std::string tagName = parameters.GetString("tag", noTag);

    AccessType type;
    if (cor::IEquals(typeS, "read"))
        type = eRead;
    else if (cor::IEquals(typeS, "write"))
        type = eWrite;
    else
    {
        throw fcgi::Exception(400, "bad 'type' parameter value %s", typeS.c_str());
    }

    AccessPtr newAccess = database->CreateAccess(branchPath, tr, type, tagName);
    if (!newAccess)
        throw fcgi::Exception(404, "no such branch or tag");

    mAccessCache.Put(newAccess->GetHandle().string(), newAccess, "", NULL, &mIopCache);
    Json::Value newUuid = newAccess->GetHandle().string();

    bool ready = newAccess->Open(0);

    fcgi::Header responseHeader;
    request.Reply(ready ? 200 : 202, responseHeader, newUuid);
}


void
QueryAccessHandler::Process(fcgi::Request& request,
                         const fcgi::KeyedFieldMap& keyedFields,
                         const fcgi::ParameterMap& parameters)
{
    std::string accessHandle;

    try {
        accessHandle = keyedFields.at("access_id");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing access ID");
    }

    int timeoutRequested = parameters.GetInteger("timeout", -1);
    int timeout = timeoutRequested;
    if ((timeout > mMaxTimeoutSeconds) || (timeout < 0))
        timeout = mMaxTimeoutSeconds;

    IOAccess ac;
    try {
        ac = mAccessCache.Get(accessHandle);
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(404, "unknown or expired access ID");
    }
    bool ready = (*ac)->Open(timeout);

    Json::Value v;
    if (timeoutRequested < 0) // infinite
        v = 0;
    else
        v = timeout;

    fcgi::Header responseHeader;
    request.Reply(ready ? 200 : 202, responseHeader, v);
}

void
SelectAccessHandler::Process(fcgi::Request& request,
                            const fcgi::KeyedFieldMap& keyedFields,
                            const fcgi::ParameterMap& parameters)
{
    std::string iopUuid;
    try {
        iopUuid = keyedFields.at("access_id");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing access ID");
    }

    IOAccess ac;
    try {
        ac = mAccessCache.Get(iopUuid);
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(404, "unknown or expired access ID");
    }

    nabu::WrittenColumnsMap wcm;
    std::set<MetaKey> columns;

    Json::Value root;
    request.GetReader().Read(root);

    // parse JSON into selected column set
    std::set<MetaKey> selectColumns;
    if (!root.isArray())
        throw fcgi::Exception(400, "bad JSON document: should be an Array");
    Json::ValueIterator i = root.begin();
    for (size_t j = 0; i != root.end(); i++, j++)
    {
        // DEFENSIVE; expect sequential index
        if (i.key().asInt() != j)
            throw fcgi::Exception(400, "bad JSON document: expected sequential index %ld, got %d", j, i.key().asInt());
        std::string s = (*i).asString();
        selectColumns.insert(s);
    }

    int timeoutRequested = parameters.GetInteger("timeout", -1);

    SelectResult r = (*ac)->Select(selectColumns, wcm, timeoutRequested);

    Json::Value v;
    if (r == eComplete)
    {
        wcm.Render(v);
    }

    fcgi::Header responseHeader;
    int code = 200; // complete
    if (r == eWaitAgain)
        code = 202;
    else if (r == eTimeout)
        code = 408;
    else if (r == eAbort)
        code = 410;
    request.Reply(code, responseHeader, v);
}

void
SelectAbortHandler::Process(fcgi::Request& request,
                             const fcgi::KeyedFieldMap& keyedFields,
                             const fcgi::ParameterMap& parameters)
{
    std::string iopUuid;
    try {
        iopUuid = keyedFields.at("access_id");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing access ID");
    }

    IOAccess ac;
    try {
        ac = mAccessCache.Get(iopUuid);
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(404, "unknown or expired access ID");
    }

    bool aborted = (*ac)->AbortSelect();

    Json::Value v = aborted;

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}

void
ExtentsAccessHandler::Process(fcgi::Request& request,
                          const fcgi::KeyedFieldMap& keyedFields,
                          const fcgi::ParameterMap& parameters)
{
    std::string iopUuid;
    try {
        iopUuid = keyedFields.at("access_id");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing access ID");
    }

    IOAccess ac;
    try {
        ac = mAccessCache.Get(iopUuid);
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(404, "unknown or expired access ID");
    }

    nabu::WrittenColumnsMap wcm;
    std::set<MetaKey> columns;

    Json::Value root;
    request.GetReader().Read(root);

    // parse JSON into selected column set
    std::set<MetaKey> selectColumns;
    if (!root.isArray())
        throw fcgi::Exception(400, "bad JSON document: should be an Array");
    Json::ValueIterator i = root.begin();
    for (size_t j = 0; i != root.end(); i++, j++)
    {
        // DEFENSIVE; expect sequential index
        if (i.key().asInt() != j)
            throw fcgi::Exception(400, "bad JSON document: expected sequential index %ld, got %d", j, i.key().asInt());
        std::string s = (*i).asString();
        selectColumns.insert(s);
    }

    std::set<MetaKey>::const_iterator j = selectColumns.begin();
    (*ac)->Extents(selectColumns, wcm);

    Json::Value v;
    wcm.Render(v);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


void
TickHandler::Process(fcgi::Request& request,
                             const fcgi::KeyedFieldMap& keyedFields,
                             const fcgi::ParameterMap& parameters)
{
    std::string accessUuid;
    try {
        accessUuid = keyedFields.at("access_id");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing access ID");
    }

    IOAccess ac;
    try {
        ac = mAccessCache.Get(accessUuid);
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(404, "unknown or expired access ID");
    }

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader);
}

void
EnableGCHandler::Process(fcgi::Request& request,
                             const fcgi::KeyedFieldMap& keyedFields,
                             const fcgi::ParameterMap& parameters)
{
    std::string accessUuid;
    try {
        accessUuid = keyedFields.at("access_id");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing access ID");
    }

    IOAccess ac;
    try {
        ac = mAccessCache.Get(accessUuid);
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(404, "unknown or expired access ID");
    }
    bool disable = parameters.GetBool("disable");
    if (disable)
        ac->DisableGarbageCollection();
    else
        ac->EnableGarbageCollection();

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader);
}

void
CloseAccessHandler::Process(fcgi::Request& request,
                         const fcgi::KeyedFieldMap& keyedFields,
                         const fcgi::ParameterMap& parameters)
{
    std::string iopUuid;

    try {
        iopUuid = keyedFields.at("access_id");
    } catch (...)
    {
        throw fcgi::Exception(400, "missing access handle");
    }

    try {
        mAccessCache.Remove(iopUuid);
    } catch (...)
    {
        throw fcgi::Exception(404, "unknown or expired access handle");
    }

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader);
}


} // end namespace
