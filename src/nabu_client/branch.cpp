//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <interweb/request.h>
#include <json/reader.h>
#include <mcor/json_time.h>
#include <json/writer.h>
#include <json/value.h>

#include <mcor/binary.h>
#include <mcor/strutil.h>

#include <nabu/branch_imp.h>
#include <nabu/database.h>


#include "branch.h"
#include "iop.h"
#include "tag.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace nabu {
namespace client {
Branch::Branch(nabu::Database& database,
       const std::string& branchName) :
       nabu::Branch(database, branchName)
{
}

Branch::~Branch()
{
}

cor::TimeRange
Branch::Extents(const MetaKey& column, ExtentsType extentsType)
{
    DEBUG_LOCAL("Branch(client)::Extents%s\n", "");
    try {
        http::ParameterMap p;
        if (extentsType == eWriteable)
            p.SetString("type", "write");
        http::Request rq;

        http::RequestPtr response = rq.Get(mDatabase.GetUrl() +
                "/database/" + mDatabase.GetInstanceName() +
                "/branch/" + mBranchPath.GetLiteral(':') +
                "/extents/" + column.GetLiteral(':'), p);
        response->AssertReturnCode(200);

        return Json::TimeRangeFromJson(response->GetJson());
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::Extents() -- %s", err.what());
        throw err;
    }
}

size_t
Branch::Append(const MetaKey& column, const RecordVector& data)
{
    DEBUG_LOCAL("Branch(client)::Append %s\n", column.GetLiteral().c_str());
    try {
        http::Request rq;

        // ---- turn data into body
        http::BinaryPtr body(new http::Binary);
        // 4 bytes for size
        // 20 bytes for record header, per record
        // record data itself
        body->resize(4 + (20 * data.size()) + data.TotalSize());
        unsigned char* d = body->data();

        cor::Pack32(&d, data.size());
        for (size_t i = 0; i < data.size(); i++)
        {
            cor::Pack64(&d, data[i].GetTime().SecondPortion());
            cor::Pack64(&d, data[i].GetTime().AttoSecondPortion());
            cor::Pack32(&d, data[i].mData->size());
            std::vector<unsigned char>& v = *(data[i].mData);
            for (size_t j = 0; j < v.size(); j++, d++)
                *d = v[j];
        }

        http::ParameterMap p;
        rq.SetBinary(body);
        http::RequestPtr response = rq.Post(mDatabase.GetUrl() +
                                              "/database/" + mDatabase.GetInstanceName() +
                                              "/branch/" + mBranchPath.GetLiteral(':') +
                                              "/append/" + column.GetLiteral(':'), p);
        response->AssertReturnCode(200);

        return response->GetJson().asUInt();
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::Append() -- %s", err.what());
        throw err;
    }
}

void
Branch::GetTags(std::set<std::string>& tags)
{
    DEBUG_LOCAL("Branch(client)::GetTags%s\n", "");
    try {
        http::ParameterMap p;
        http::Request rq;

        http::RequestPtr response = rq.Get(mDatabase.GetUrl() +
                                           "/database/" + mDatabase.GetInstanceName() +
                                           "/branch/" + mBranchPath.GetLiteral(':') +
                                           "/tags", p);
        response->AssertReturnCode(200);

        // convert JSON into strings

        // DEFENSIVE!
        if (!response->GetJsonPtr()->isArray())
            throw cor::Exception("expected Array response");

        Json::ValueIterator i = response->GetJsonPtr()->begin();
        for (; i != response->GetJsonPtr()->end(); i++)
        {
            std::string s = (*i).asString();
            tags.insert(s);
        }
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::GetTags() -- %s", err.what());
        throw err;
    }
}

void
Branch::MoveHead(const Version& newHeadVersion)
{
    DEBUG_LOCAL("Branch(client)::MoveHead%s\n", "");
    try {
        http::ParameterMap p;
        http::Request rq;

        http::RequestPtr response = rq.Post(mDatabase.GetUrl() +
                                           "/database/" + mDatabase.GetInstanceName() +
                                           "/branch/" + mBranchPath.GetLiteral(':') +
                                           "/movehead", p);
        response->AssertReturnCode(200);
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::MoveHead() -- %s", err.what());
        throw err;
    }
}

nabu::TagPtr
Branch::CreateTag(const std::string& tagName, TagPolicy policy, const Version& version)
{
    DEBUG_LOCAL("Branch(client)::CreateTag %s\n", tagName.c_str());
    try {
        http::Request rq;

        // encode version as JSON in body
        Json::Value v;
        version.RenderToJson(v);
        rq.SetJson(v);

        http::ParameterMap p;
        p.SetString("name", tagName);
        p.SetString("policy", TagPolicyToString(policy));

        http::RequestPtr response = rq.Post(mDatabase.GetUrl() +
                                            "/database/" + mDatabase.GetInstanceName() +
                                            "/branch/" + mBranchPath.GetLiteral(':') +
                                            "/tag", p);
        response->AssertReturnCode(200);

        Version returnedVersion;
        returnedVersion.ParseFromJson(response->GetJson());

        nabu::TagPtr tag(new nabu::client::Tag(mDatabase.GetUrl(), *this, tagName, returnedVersion));
        return tag;
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::CreateTag() -- %s", err.what());
        throw err;
    }
}

nabu::TagPtr
Branch::GetTag(const std::string& tagName)
{
    DEBUG_LOCAL("Branch(client)::GetTag %s\n", tagName.c_str());
    try {
        http::Request rq;

        http::ParameterMap p;
        http::RequestPtr response = rq.Get(mDatabase.GetUrl() +
                                           "/database/" + mDatabase.GetInstanceName() +
                                           "/branch/" + mBranchPath.GetLiteral(':') +
                                           "/tag/" + tagName, p);
        response->AssertReturnCode(200);

        std::cout << response->GetJson() << std::endl;

        Version version;
        version.ParseFromJson(response->GetJson());

        nabu::TagPtr tag(new nabu::client::Tag(mDatabase.GetUrl(), *this, tagName, version));
        return tag;
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::GetTag() -- %s", err.what());
        throw err;
    }
}

bool
Branch::VersionInUse(const TableAddress& tableAddress)
{
    DEBUG_LOCAL("Branch(client)::VersionInUse%s\n", "");
    try {
        http::Request rq;

        // encode tableAddress as JSON in body
        Json::Value v;
        tableAddress.RenderToJson(v);
        rq.SetJson(v);

        http::ParameterMap p;
        http::RequestPtr response = rq.Post(mDatabase.GetUrl() +
                                            "/database/" + mDatabase.GetInstanceName() +
                                            "/branch/" + mBranchPath.GetLiteral(':') +
                                            "/versioninuse", p);
        response->AssertReturnCode(200);

        Version version;
        version.ParseFromJson(response->GetJson()["version"]);

        return response->GetJson().asBool();
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::VersionInUse() -- %s", err.what());
        throw err;
    }
}

void
Branch::GetUnusedVersions(const TableAddress& ta, MetaDataMap diffs)
{
}

bool
Branch::DeleteTag(const std::string& tagName)
{
    DEBUG_LOCAL("Branch(client)::DeleteTag %s\n", tagName.c_str());
    try {
        http::Request rq;

        http::ParameterMap p;
        http::RequestPtr response = rq.Delete(mDatabase.GetUrl() +
                                            "/database/" + mDatabase.GetInstanceName() +
                                            "/branch/" + mBranchPath.GetLiteral(':') +
                                            "/tag/" + tagName, p);
        response->AssertReturnCode(200);

        return response->GetJson().asBool();
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::DeleteTag() -- %s", err.what());
        throw err;
    }
}

void
Branch::TrimTags(const std::set<std::string>& tagSet)
{
    DEBUG_LOCAL("Branch(client)::TrimTags%s\n", "");
    try {
        http::Request rq;

        // convert set of strings into JSON object
        http::JsonPtr send = rq.GetJsonPtr();
        std::set<std::string>::const_iterator i = tagSet.begin();
        for (; i != tagSet.end(); i++)
            (*send)[*i];

        http::ParameterMap p;
        http::RequestPtr response = rq.Post(mDatabase.GetUrl() +
                                            "/database/" + mDatabase.GetInstanceName() +
                                            "/branch/" + mBranchPath.GetLiteral(':') +
                                            "/trimtags", p);
        response->AssertReturnCode(200);
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::TrimTags() -- %s", err.what());
        throw err;
    }
}

void
Branch::DeleteColumn(const MetaKey& column)
{
    DEBUG_LOCAL("Branch(client)::DeleteColumn %s\n", column.GetLiteral().c_str());
    try {
        http::Request rq;

        http::ParameterMap p;
        http::RequestPtr response = rq.Delete(mDatabase.GetUrl() +
                                              "/database/" + mDatabase.GetInstanceName() +
                                              "/branch/" + mBranchPath.GetLiteral(':') +
                                              "/column/" + column.GetLiteral(':'), p);
        response->AssertReturnCode(200);
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::DeleteColumn() -- %s", err.what());
        throw err;
    }
}

void
Branch::GetColumns(const MetaKey& column, std::vector<MetaKey>& columns)
{
    DEBUG_LOCAL("Branch(client)::GetColumns%s\n", "");
    try {
        http::Request rq;

        http::ParameterMap p;
        http::RequestPtr response = rq.Get(mDatabase.GetUrl() +
                                              "/database/" + mDatabase.GetInstanceName() +
                                              "/branch/" + mBranchPath.GetLiteral(':') +
                                              "/columns", p);
        response->AssertReturnCode(200);

        // convert JSON into Metakeys
        http::JsonPtr v = response->GetJsonPtr();
        // DEFENSIVE!
        if (!v->isArray())
            throw cor::Exception("expected Array response");
        Json::ValueIterator i = v->begin();
        for (; i != v->end(); i++)
        {
            std::string s = (*i).asString();
            columns.push_back(nabu::MetaKey(s)); // constructor parses slash-delimited string
        }
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::GetColumns() -- %s", err.what());
        throw err;
    }
}

bool
Branch::HasColumn(const nabu::MetaKey &column)
{
    DEBUG_LOCAL("Branch(client)::HasColumn %s\n", column.GetLiteral().c_str());
    try {
        http::Request rq;

        http::ParameterMap p;
        http::RequestPtr response = rq.Get(mDatabase.GetUrl() +
                                           "/database/" + mDatabase.GetInstanceName() +
                                           "/branch/" + mBranchPath.GetLiteral(':') +
                                           "/column/" + column.GetLiteral(':'), p);
        response->AssertReturnCode(200);

        return response->GetJson().asBool();
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::HasColumn() -- %s", err.what());
        throw err;
    }
}

size_t
Branch::TotalSize()
{
    DEBUG_LOCAL("Branch(client)::TotalSize%s\n", "");
    try {
        http::Request rq;

        http::ParameterMap p;
        http::RequestPtr response = rq.Get(mDatabase.GetUrl() +
                                           "/database/" + mDatabase.GetInstanceName() +
                                           "/branch/" + mBranchPath.GetLiteral(':') +
                                           "/size", p);
        response->AssertReturnCode(200);

        return response->GetJson().asUInt();
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Branch::TotalSize() -- %s", err.what());
        throw err;
    }
}


} // end namespace
}
