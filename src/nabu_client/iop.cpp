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
#include <string.h>


#include "access.h"
#include "iop.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {
namespace client {


IOOperation::IOOperation(const cor::Url& url,
                         nabu::Access* access,
                         const cor::TimeRange& timeRange,
                         IterationDirection direction,
                         const muuid::uuid& handle) :
    nabu::IOOperation(timeRange, direction, handle),
    mUrl(url),
    mAccess(access)
{
}

/*IOOperation::IOOperation(const Json::Value &v)
{
    mUrl = v["url"].asString();
    mAccessHandle = v["access_uuid"].asString();
    mHandle.set(v["uuid"].asString());
    mTimeRange = Json::TimeRangeFromJson(v["timerange"]);
}*/

IOOperation::~IOOperation()
{
    // must eat exceptions that Close() could throw
    // if this call fails, because network connectivity was lost for instance,
    // then the far end will eventually time out and discard the IOOperation
    // itself
    try {
        Close();
    } catch (...)
    {}
}

void
IOOperation::Read(const MetaKey& column,
                 RecordVector& records,
                 size_t count,
                 cor::Time until)
{
    DEBUG_LOCAL("IOOperation(client)::Read %s\n", column.GetLiteral().c_str());
    try {

        http::ParameterMap p;
        p.SetInteger("count", count);
        if (until.IsNegativeInfinite())
            throw cor::Exception("Negative infinity invalid value for 'until'");
        if (until.IsOrdinary())
            p.SetTime("until", until);
        // default for 'until' is positive infinite

        const size_t timeoutSeconds = 60;
        http::Request rq;
        http::RequestPtr response = rq.Post(mUrl +
                        "/iop/" + mHandle.string() +
                        "/read/" + column.GetLiteral(':'), p, timeoutSeconds);
        response->AssertReturnCode(200);

        const unsigned char* d = (unsigned char*)response->GetBinaryPtr()->data();
        size_t dSize = response->GetBinaryPtr()->size();
        if (dSize < 4)
            throw cor::Exception("Bad response: size of payload is too small (%ld)", dSize);
        size_t n = cor::Unpack32(&d);

        records.resize(n);
        dSize -= 4;

        // 16 bytes for start
        // 16 bytes for final
        // 4 bytes for JSON size
        // 4 bytes for data payload size
        const size_t headerSize = 40;
        for (size_t i = 0; i < n; i++)
        {
            if (dSize < headerSize)
                throw cor::Exception("Bad response: parse error reading record header at record %ld (remaining size %ld)", n, dSize);
            uint64_t seconds = cor::Unpack64(&d);
            uint64_t attoseconds = cor::Unpack64(&d);
            records[i].mTimeRange.First() = cor::Time(seconds, attoseconds);
            seconds = cor::Unpack64(&d);
            attoseconds = cor::Unpack64(&d);
            records[i].mTimeRange.Final() = cor::Time(seconds, attoseconds);

            uint32_t jsonSize = cor::Unpack32(&d);
            uint32_t recordSize = cor::Unpack32(&d);

            dSize -= headerSize;

            if (dSize < jsonSize)
            {
                throw cor::Exception("Bad response: parse error reading JSON data at record %ld (size = %ld, remaining size %ld)",
                                     i, jsonSize,  dSize);
            }

            if (jsonSize != 0)
            {
                Json::Reader reader;
                if (!reader.parse((char*)d, (char*)d + jsonSize, records[i].mDataObject))
                {
                    throw cor::Exception("Error in searchable JSON in record %ld: %s\n",
                                         i,
                                         reader.getFormattedErrorMessages().c_str());
                }
                d += jsonSize;
            }

            if (dSize < recordSize)
                throw cor::Exception("Bad response: parse error reading record data at record %ld (size = %ld, remaining size %ld)",
                                     i, recordSize,  dSize);

            records[i].mData->resize(recordSize);
            std::vector<unsigned char>& v = *(records[i].mData);
            for (size_t j = 0; j < recordSize; j++)
                v[j] = d[j];
            d += recordSize;
            dSize -= recordSize;
        }
    }
    catch(cor::Exception& err)
    {
        err.SetWhat("IOOperation::Read() -- %s", err.what());
        throw err;
    }
}

void
IOOperation::Render(Json::Value& v)
{
    v["url"] = mUrl.GetLiteral();
    v["uuid"] = mHandle.string();
    v["access_uuid"] = mAccess->GetHandle().string();
    Json::TimeRangeToJson(mTimeRange, v["timerange"]);
}

void
IOOperation::Close()
{
    DEBUG_LOCAL("IOOperation(client)::Close%s\n","");
    if (mHandle.is_nil())
        return;

    try {
        http::ParameterMap p;
        http::Request rq;

        http::RequestPtr response = rq.Delete(mUrl +
                                            "/iop/" + mHandle.string() , p);
        response->AssertReturnCode(200);

        mHandle.string() = "";
    }
    catch(cor::Exception& err)
    {
        err.SetWhat("IOOperation::Close() -- %s", err.what());
        throw err;
    }
}

bool
IOOperation::ReadSeek(const MetaKey& column, const cor::Time& whence)
{
    DEBUG_LOCAL("IOOperation(client)::ReadSeek %s\n", column.GetLiteral().c_str());
    if (mHandle.is_nil())
        throw cor::Exception("IOOperationPtr has not been opened");

    try {
        http::ParameterMap p;
        p.SetTime("whence", whence);
        http::Request rq;

        http::RequestPtr response = rq.Post(mUrl +
                                              "/iop/" + mHandle.string() +
                                              "/read_seek/" + column.GetLiteral(':'), p);
        response->AssertReturnCode(200);

        return response->GetJson().asBool();
    }
    catch(cor::Exception& err)
    {
        err.SetWhat("IOOperation::ReadSeek() -- %s", err.what());
        throw err;
    }
}

void
IOOperation::Search(const MetaKey& column,
                  const std::string& predicate,
                  nabu::SearchResultVector& results)
{
    DEBUG_LOCAL("IOOperation(client)::Search %s\n", column.GetLiteral().c_str());
    if (mHandle.is_nil())
        throw cor::Exception("IOOperationPtr has not been opened");

    try {

        http::ParameterMap p;
        http::Request rq;
        rq.SetText(predicate);

        http::RequestPtr response = rq.Post(mUrl +
                                            "/iop/" + mHandle.string() +
                                            "/search/" + column.GetLiteral(':'), p);
        response->AssertReturnCode(200);

        results.clear();
        if (!response->Empty())
        {
            results.Parse(response->GetJson());
        }
    }
    catch(cor::Exception& err)
    {
        err.SetWhat("IOOperation::Search() -- %s", err.what());
        throw err;
    }
}


void
IOOperation::Commit()
{
    DEBUG_LOCAL("IOOperation(client)::Commit %s\n", "");
    CommitImp(false);
}

void
IOOperation::Abort()
{
    DEBUG_LOCAL("IOOperation(client)::Abort %s\n", "");
    CommitImp(true);
}

void
IOOperation::Write(const MetaKey& column, const RecordVector& records)
{
    DEBUG_LOCAL("IOOperation(client)::Write %s\n", column.GetLiteral().c_str());
    try
    {
        http::Request rq;
        http::BinaryPtr s(new http::Binary);
        s->resize(4);

        cor::Pack32(s->data(), records.size());
        // n is the offset of next byte written
        size_t n = 4;
        // 16 bytes for start
        // 16 bytes for final
        // 4 bytes for JSON size
        // 4 bytes for data payload size
        const size_t headerSize = 40;
        for (size_t i = 0; i < records.size(); i++)
        {
            nabu::Record r = records[i];
            size_t sz = r.mData->size();

            // searchable JSON
            std::string jsonString;
            if (!r.mDataObject.isNull())
            {
                Json::FastWriter writer;
                jsonString = writer.write(r.mDataObject);
            }

            s->resize(s->size() + headerSize + jsonString.size() + sz);

            unsigned char* c = s->data() + n;
            cor::Pack64(&c, r.mTimeRange.First().SecondPortion());
            cor::Pack64(&c, r.mTimeRange.First().AttoSecondPortion());
            cor::Pack64(&c, r.mTimeRange.Final().SecondPortion());
            cor::Pack64(&c, r.mTimeRange.Final().AttoSecondPortion());
            cor::Pack32(&c, jsonString.size());
            cor::Pack32(&c, r.mData->size());
            n += headerSize;

            // searchable JSON
            if (!jsonString.empty())
            {
                memcpy(c, jsonString.data(), jsonString.size());
            }
            n += jsonString.size();

            std::vector<unsigned char>& v = *(r.mData);
            for (size_t j = 0; j < r.mData->size(); j++, n++)
                (*s)[n] = v[j];
        }

        http::ParameterMap p;
        rq.SetBinary(s);
        http::RequestPtr response = rq.Post(mUrl +
                                            "/iop/" + mHandle.string() +
                                            "/write/" + column.GetLiteral(':'), p);
        response->AssertReturnCode(200);
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("IOOperation::Write() -- %s", err.what());
        throw err;
    }
}

void
IOOperation::CommitImp(bool abort)
{
    try {

        http::ParameterMap p;
        p.SetString("action", abort? "abort" : "commit");
        http::Request rq;

        http::RequestPtr response = rq.Post(mUrl +
                                            "/iop/" + mHandle.string() +
                                            "/commit", p);
        response->AssertReturnCode(200);
    }
    catch(cor::Exception& err)
    {
        err.SetWhat("IOOperation::Commit() -- %s", err.what());
        throw err;
    }
}

} // end namespaces
}
