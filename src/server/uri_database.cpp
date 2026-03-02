//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <mfcgi/exception.h>

#include <nabu/private_types.h>
#include <nabu_client/database.h>

#include "uri_database.h"


namespace nabu {


void
ListDatabasesHandler::Process(fcgi::Request& request,
                        const fcgi::KeyedFieldMap& keyedFields,
                        const fcgi::ParameterMap& parameters)
{
    Json::Value v;
    v[(int)0];  // force to array type
    cor::ObjectLocker ol;
    DatabaseMap::Iterator i = mDatabaseMap.Begin(ol);
    for (int n = 0; i != mDatabaseMap.End(); i++, n++)
    {
        // XXX not transmitting online/offline status, just the name
        v[n] = i->first.c_str();
    }
    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


void
TimeoutHandler::Process(fcgi::Request& request,
                        const fcgi::KeyedFieldMap& keyedFields,
                        const fcgi::ParameterMap& parameters)
{
    Json::Value v = mTimeoutSeconds;

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


void
FingerprintHandler::Process(fcgi::Request& request,
                        const fcgi::KeyedFieldMap& keyedFields,
                        const fcgi::ParameterMap& parameters)
{
    Json::Value v = GetDatabase(keyedFields)->GetFingerprint().string();

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


void
TimePeriodsHandler::Process(fcgi::Request& request,
                            const fcgi::KeyedFieldMap& keyedFields,
                            const fcgi::ParameterMap& parameters)
{
    std::vector<unsigned int> tp = GetDatabase(keyedFields)->GetPeriods();
    Json::Value v;
    for (int i = 0; i < tp.size(); i++)
        v[i] = tp[i];

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


void
InitHandler::Process(fcgi::Request& request,
             const fcgi::KeyedFieldMap& keyedFields,
             const fcgi::ParameterMap& parameters)
{
    Json::Value v;

    request.GetReader().Read(v);

    Json::Value periods = v["periods"];
    if (!periods.isArray())
        throw fcgi::Exception(400, "Missing 'periods' array");

    std::vector<unsigned int> tp;
    tp.resize(periods.size());
    for (int i = 0; i < periods.size(); i++)
        tp[i] = periods[i].asUInt();

    Json::Value name = v["name"];
    if (!name.isString())
        throw fcgi::Exception(400, "Missing 'name' string");

    // make sure database name is legal
    std::string error;
    if (!FileNamer::ValidateLabelName(name.asString(), error))
        throw fcgi::Exception(400, "Bad database name '%s': %s", name.asCString(), error.c_str());

    // and unique
    if (mDatabases.Has(name.asString()))
        throw fcgi::Exception(400, "database '%s' already exists", name.asCString());

    // create the database
    DatabaseImpPtr newDatabase(new nabu::DatabaseImp(mRootDirectory));
    newDatabase->CreateNew(tp);
    mDatabases.Add(name.asString(), newDatabase);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


void
CloneHandler::Process(fcgi::Request& request,
                            const fcgi::KeyedFieldMap& keyedFields,
                            const fcgi::ParameterMap& parameters)
{
    Json::Value v;

    request.GetReader().Read(v);

    Json::Value name = v["name"];
    if (!name.isString())
        throw fcgi::Exception(400, "Missing 'name' STRING");

    // make sure database name is legal
    std::string error;
    if (!FileNamer::ValidateLabelName(name.asString(), error))
        throw fcgi::Exception(400, "Bad database name '%s': %s", name.asCString(), error.c_str());

    // and unique
    if (mDatabases.Has(name.asString()))
        throw fcgi::Exception(400, "database '%s' already exists", name.asCString());

    // create the database
    DatabaseImpPtr newDatabase(new nabu::DatabaseImp(mRootDirectory));

    Json::Value urlString = v["url"];
    if (!urlString.isString())
        throw fcgi::Exception(400, "Missing 'url' string");
    cor::Url url;
    try {
        url.Set(urlString.asString());
    }
    catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "bad URL '%s'", urlString.asCString());
    }

    Json::Value instanceString = v["instance"];
    if (!instanceString.isString())
        throw fcgi::Exception(400, "Missing 'instance' string");

    DatabasePtr remote(new nabu::client::Database(url, instanceString.asString()));
    newDatabase->Clone(remote);

    mDatabases.Add(name.asString(), newDatabase);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}


void
ReadFileHandler::Process(fcgi::Request& request,
                            const fcgi::KeyedFieldMap& keyedFields,
                            const fcgi::ParameterMap& parameters)
{
    nabu::MetaKey column;

    DatabaseImpPtr database = GetDatabase(keyedFields);

    column.Parse(parameters.GetString("column"), ':');
    nabu::TimePath path;
    path.Parse(parameters.GetString("path"), ':');
    nabu::Version version;
    version.Parse(parameters.GetString("version"));
    TableAddress ta(column, path, version);
    FileType fileType = StringToFileType(parameters.GetString("type"));

    std::vector<unsigned char> data;
    database->ReadFile(data, ta, fileType);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, data);
}

void
WriteFileHandler::Process(fcgi::Request& request,
                         const fcgi::KeyedFieldMap& keyedFields,
                         const fcgi::ParameterMap& parameters)
{
    nabu::MetaKey column;

    DatabaseImpPtr database = GetDatabase(keyedFields);

    column.Parse(parameters.GetString("column"), '_');
    nabu::TimePath path;
    path.Parse(parameters.GetString("path"), '_');
    nabu::Version version;
    version.Parse(parameters.GetString("version"));
    TableAddress ta(column, path, version);
    FileType fileType = StringToFileType(parameters.GetString("type"));

    std::vector<unsigned char> data;

    std::string response;
    request.GetReader().Read((char*)data.data(), data.size());
    database->WriteFile(data, ta, fileType);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader);
}


} // end namespace
