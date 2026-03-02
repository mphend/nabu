//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include <interweb/request.h>
#include <mcor/strutil.h>
#include <mcor/url.h>

#include "access.h"
#include "branch.h"
#include "iop.h"

#include "database.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace nabu {
namespace client {

void
GetDatabases(const cor::Url& url, std::set<std::string>& instances)
{
    try {
        http::Request rq;
        http::ParameterMap p;
        http::RequestPtr response = rq.Get(url + "/databases", p);
        response->AssertReturnCode(200);

        http::JsonPtr v = response->GetJsonPtr();
        if (!v->isArray())
            throw cor::Exception("Expected an Array");
        Json::ValueIterator i = v->begin();
        for (; i != v->end(); i++)
        {
            instances.insert((*i).asString());
        }
    }
    catch (cor::Exception& err)
    {
        printf("URL = %s\n", (url + "/databases").GetLiteral().c_str());
        err.SetWhat("GetDatabases() -- %s", err.what());
        throw err;
    }
}

Database::Database(const cor::Url& url, const std::string& instanceName) :
    nabu::Database(url),
    mInstance(instanceName)
{
}

Database::~Database()
{
}

nabu::BranchPtr
Database::GetBranch(const MetaKey& branchPath)
{
    DEBUG_LOCAL("Database(client)::GetBranch %s\n", branchPath.GetLiteral().c_str());
    try {
        http::Request rq;
        http::ParameterMap p;
        http::RequestPtr response = rq.Get(mUrl +
                                           "/database/" + GetInstanceName() +
                                           "/branch/" + branchPath.GetLiteral(':'), p);
        response->AssertReturnCode(200);
        return BranchPtr(new nabu::client::Branch(*this, branchPath.GetLiteral(':')));
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::GetBranch() -- %s", err.what());
        throw err;
    }
}

nabu::BranchPtr
Database::GetMain()
{
    return GetBranch(nabu::MetaKey());
}

nabu::BranchPtr
Database::CreateBranch(const MetaKey& branchPath)
{
    DEBUG_LOCAL("Database(client)::CreateBranch %s\n", branchPath.GetLiteral().c_str());
    try {
        http::Request rq;

        http::ParameterMap p;
        http::RequestPtr response = rq.Post(mUrl +
                                            "/database/" + GetInstanceName() +
                                            "/branch/" + branchPath.GetLiteral(':'), p);
        response->AssertReturnCode(200);

        return BranchPtr(new nabu::client::Branch(*this, branchPath.GetLiteral(':')));
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::CreateBranch() -- %s", err.what());
        throw err;
    }
}

void
Database::DeleteBranch(const MetaKey& branchPath)
{
    DEBUG_LOCAL("Database(client)::DeleteBranch %s\n", branchPath.GetLiteral().c_str());
    try {
        http::Request rq;

        http::ParameterMap p;
        http::RequestPtr response = rq.Delete(mUrl +
                                              "/database/" + GetInstanceName() +
                                              "/branch/" + branchPath.GetLiteral(':'), p);
        response->AssertReturnCode(200);
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::DeleteBranch() -- %s", err.what());
        throw err;
    }
}

nabu::AccessPtr
Database::CreateAccess(const MetaKey& branchPath,
                       const cor::TimeRange& timeRange,
                       AccessType type,
                       const std::string tagName)
{
    DEBUG_LOCAL("Database(client)::CreateAccess %s\n", branchPath.GetLiteral().c_str());
    try {
        http::ParameterMap p;
        if (timeRange.First().IsOrdinary())
            p.SetTime("start", timeRange.First());
        if (timeRange.Final().IsOrdinary())
            p.SetTime("end", timeRange.Final());
        if (!tagName.empty())
            p.SetString("tag", tagName);
        p.SetString("type", type == nabu::eRead ? "read" : "write");

        http::Request rq;
        http::RequestPtr response = rq.Post(mUrl +
                                            "/database/" + GetInstanceName() +
                                            "/branch/" + branchPath.GetLiteral(':') +
                                            "/access", p);
        muuid::uuid handle;
        if (response->Code() < 300)
        {
             handle.set(response->GetJson().asString());
        }
        else if (response->Code() == 404)
        {
            // XXX MPH: changed this behavior Oct 2025 to throw, below
            //return nabu::AccessPtr(); // no such branch or tag; return a NULL object

            if (!tagName.empty())
                throw cor::Exception("No such branch '%s' or tag '%s'", branchPath.GetLiteral().c_str(), tagName.c_str());
            throw cor::Exception("No such branch '%s'", branchPath.GetLiteral().c_str());
        }
        else
            response->AssertReturnCode(200); // all other codes

        nabu::AccessPtr r = nabu::AccessPtr(new Access(GetUrl(), GetBranch(branchPath), handle, type, timeRange, tagName));
        return r;
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::CreateAccess() -- %s", err.what());
        throw err;
    }
}

bool
Database::VersionInUse(const TableAddress& nodeAddress)
{
    DEBUG_LOCAL("Database(client)::VersionInUse %s\n", nodeAddress.GetLiteral().c_str());
    try {
        http::ParameterMap p;
        p.SetString("address", nodeAddress.GetLiteral());

        http::Request rq;
        http::RequestPtr response = rq.Post(mUrl +
                                            "/database/" + GetInstanceName() +
                                            "/database/versioninuse", p);

        response->AssertReturnCode(200); // all other codes

        return response->GetJson().asBool();
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::VersionInUse() -- %s", err.what());
        throw err;
    }
}

void
Database::GetUnusedVersions(const TableAddress& ta, MetaDataMap diffs)
{
    DEBUG_LOCAL("Database(client)::GetUnusedVersions %s\n", ta.GetLiteral().c_str());
    try {
        http::ParameterMap p;
        Json::Value v;
        ta.RenderToJson(v);
        p.SetString("address", Json::FastWriter().write(v));

        http::Request rq;
        std::string s;
        diffs->PersistDocument(s);
        rq.SetText(s);
        http::RequestPtr response = rq.Post(mUrl +
                                            "/database/" + GetInstanceName() +
                                            "/database/getunusedversions", p);

        response->AssertReturnCode(200); // all other codes
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::GetUnusedVersions() -- %s", err.what());
        throw err;
    }
}

void
Database::WriteFile(const std::vector<unsigned char>& data,
                       const TableAddress& tableAddress,
                       FileType fileType)
{
    DEBUG_LOCAL("Database(client)::WriteFile %s\n", tableAddress.GetLiteral().c_str());
    try {
        http::ParameterMap p;
        p.SetString("column", tableAddress.GetColumn().GetLiteral('_'));
        p.SetString("path", tableAddress.GetTimePath().GetLiteral('_'));
        p.SetString("version", tableAddress.GetVersion().GetLiteral());
        p.SetString("type", FileTypeToString(fileType));

        http::Request rq;
        rq.SetBinary(data);
        http::RequestPtr response = rq.Post(mUrl +
                                            "/database/" + GetInstanceName() +
                                            "/writefile", p);

        response->AssertReturnCode(200); // all other codes
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::WriteFile() -- %s", err.what());
        throw err;
    }
}

void
Database::ReadFile(std::vector<unsigned char>& data,
                      const TableAddress& tableAddress,
                      FileType fileType) const
{
    DEBUG_LOCAL("Database(client)::GetBranch %s\n", tableAddress.GetLiteral().c_str());
    try {
        http::ParameterMap p;
        p.SetString("column", tableAddress.GetColumn().GetLiteral(':'));
        p.SetString("path", tableAddress.GetTimePath().GetLiteral(':'));
        p.SetString("version", tableAddress.GetVersion().GetLiteral());
        p.SetString("type", FileTypeToString(fileType));

        http::Request rq;
        http::RequestPtr response = rq.Post(mUrl +
                                            "/database/" + GetInstanceName() +
                                            "/readfile", p);
        response->AssertReturnCode(200); // all other codes

        data = response->GetBinary();

    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::ReadFile() -- %s", err.what());
        throw err;
    }
}

bool
Database::GetMetadata(MetaDataMap metaDataMap, const TableAddress& ta) const
{
    DEBUG_LOCAL("Database(client)::GetMetadata %s\n", ta.GetLiteral().c_str());
    try {
        http::BinaryPtr data(new http::Binary);
        ReadFile(*data, ta, eMetaData);

        if (data->empty())
            return false;

        std::string doc(data->begin(), data->end());
        metaDataMap->LoadDocument(doc);

        return true;

    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::GetMetadata() -- %s", err.what());
        throw err;
    }
}


void
Database::PersistMetadata(MetaDataMap metaDataMap, const TableAddress& ta) const
{
    DEBUG_LOCAL("Database(client)::PersistMetadata %s\n", ta.GetLiteral().c_str());
    try {
        http::ParameterMap p;
        p.SetString("column", ta.GetColumn().GetLiteral('_'));
        p.SetString("path", ta.GetTimePath().GetLiteral('_'));
        p.SetString("version", ta.GetVersion().GetLiteral());
        p.SetString("type", FileTypeToString(eMetaData));

        http::Request rq;
        std::string s;
        metaDataMap->PersistDocument(s);
        rq.SetText(s);

        http::RequestPtr response = rq.Post(mUrl +
                                            "/database/" + GetInstanceName() +
                                            "/persistmetadata", p);
        response->AssertReturnCode(200); // all other codes
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::PersistMetadata() -- %s", err.what());
        throw err;
    }
}

std::vector<unsigned int>
Database::GetPeriods() const
{
    DEBUG_LOCAL("Database(client)::GetPeriods %s\n", "");
    try {
        http::ParameterMap p;
        http::Request rq;
        http::RequestPtr response = rq.Post(mUrl +
                                            "/database/" + GetInstanceName() +
                                            "/time_periods", p);
        response->AssertReturnCode(200); // all other codes

        http::JsonPtr v = response->GetJsonPtr();
        if (!v->isArray())
            throw cor::Exception("expected Array response");

        std::vector<unsigned int> r;
        r.resize(v->size());
        for (int i = 0; i < v->size(); i++)
            r[i] = (*v)[i].asUInt();

        return r;
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::GetPeriods() -- %s", err.what());
        throw err;
    }
}

muuid::uuid
Database::GetFingerprint() const
{
    DEBUG_LOCAL("Database(client)::GetFingerprint %s\n", "");
    try {
        http::ParameterMap p;
        http::Request rq;
        http::RequestPtr response = rq.Post(mUrl +
                                            "/database/" + GetInstanceName() +
                                            "/fingerprint", p);
        response->AssertReturnCode(200); // all other codes

        muuid::uuid r;
        r.set(response->GetJson().asString());

        return r;
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::GetFingerprint() -- %s", err.what());
        throw err;
    }
}

int
Database::GetTimeout()
{
    DEBUG_LOCAL("Database(client)::GetTimeout %s\n", "");
    try {
        http::ParameterMap p;
        http::Request rq;
        http::RequestPtr response = rq.Get(mUrl +
                                              "/policy/timeout", p);
        response->AssertReturnCode(200); // all other codes

        http::JsonPtr v = response->GetJsonPtr();
        if (v->isDouble())
            return v->asDouble();
        return v->asInt();
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Database::DeleteColumn() -- %s", err.what());
        throw err;
    }
}


} // end namespaces
}