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

#include <nabu/branch_imp.h>

#include "uri_iop.h"


namespace nabu {

#define JSON_SIZE_LIMIT 1e5
#define RECORD_SIZE_LIMIT 100e6

void
NewIopHandler::Process(fcgi::Request& request,
                        const fcgi::KeyedFieldMap& keyedFields,
                        const fcgi::ParameterMap& parameters)
{
    std::vector<nabu::MetaKey> columns;
    cor::TimeRange tr;
    tr.First() = parameters.GetTime("start", cor::Time::NegativeInf());
    tr.Final() = parameters.GetTime("end", cor::Time::PositiveInf());
    IterationDirection direction = parameters.GetBool("backwards_iteration") ? eIterateBackwards : eIterateForwards;

    std::string accessUuid;
    try {
        accessUuid = keyedFields.at("access_id");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing operation ID");
    }

    IOAccess ac;
    try {
        ac = mAccessCache.Get(accessUuid);
    } catch (...)
    {
        throw fcgi::Exception(404, "unknown or expired access '%s'", accessUuid.c_str());
    }

    //printf("IOP: access %s, %s\n", accessUuid.c_str(), tr.Print().c_str());

    IOOperationPtr iop = (*ac).CreateIOOperation(tr, direction);
    mIOPCache.Put(iop->GetHandle().string(), iop, ac->GetHandle().string(), &mAccessCache);

    Json::Value newUuid = iop->GetHandle().string();

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, newUuid);
}


void
ReadIopHandler::Process(fcgi::Request& request,
             const fcgi::KeyedFieldMap& keyedFields,
             const fcgi::ParameterMap& parameters)
{
    std::string iopUuid;
    std::string columnString;

    try {
        iopUuid = keyedFields.at("operation_id");
        columnString = keyedFields.at("column");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing operation ID or column name");
    }

    int count = parameters.GetInteger("count", 0);
    cor::Time until = parameters.GetTime("until", cor::Time::PositiveInf());

    IOP op;
    try {
        op = mIOPCache.Get(iopUuid);
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(404, "unknown or expired operation ID");
    }

    nabu::RecordVector records;

    MetaKey column(columnString, ':');

    op->Read(column, records, count, until);

    // ---- Data

    size_t totalSize = 0;
    const size_t recordHeaderSize =
            (4 * 8) + // start and stop timestamps
            (2 * 4);  // json size and data size
    std::vector<std::string> jsonSerialized(records.size());
    for (size_t i = 0; i < records.size(); i++)
    {
        nabu::Record r = records[i];

        if (!r.mDataObject.isNull())
        {
            Json::FastWriter writer;
            jsonSerialized[i] = writer.write(r.mDataObject);
        }

        totalSize += recordHeaderSize + jsonSerialized[i].size() + r.mData->size();
    }
    totalSize += sizeof (uint32_t); // 4 bytes for the number of records, below
    std::vector<char> dataReturned;
    dataReturned.resize(totalSize);

    // number of records
    cor::Pack32(reinterpret_cast<unsigned char *>(dataReturned.data()), records.size());

    // the data
    unsigned char* d = (unsigned char*)dataReturned.data() + sizeof(uint32_t);
    for (size_t i = 0; i < records.size(); i++)
    {
        nabu::Record r = records[i];
        const cor::Time& tfirst = r.mTimeRange.First();
        cor::Pack64(&d, tfirst.SecondPortion());
        cor::Pack64(&d, tfirst.AttoSecondPortion());
        const cor::Time& tfinal = r.mTimeRange.Final();
        cor::Pack64(&d, tfinal.SecondPortion());
        cor::Pack64(&d, tfinal.AttoSecondPortion());

        cor::Pack32(&d, jsonSerialized[i].size());
        cor::Pack32(&d, r.mData->size());
        
        memcpy(d, jsonSerialized[i].data(), jsonSerialized[i].size());
        d += jsonSerialized[i].size();
        memcpy(d, r.mData->data(), r.mData->size());
        d += r.mData->size();
    }

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, dataReturned);
}


void
ReadSeekIopHandler::Process(fcgi::Request& request,
                             const fcgi::KeyedFieldMap& keyedFields,
                             const fcgi::ParameterMap& parameters)
{
    std::string iopUuid;
    std::string columnString;
    try {
        iopUuid = keyedFields.at("operation_id");
        columnString = keyedFields.at("column");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(404, "missing operation ID or column name");
    }
    MetaKey column(columnString, ':');

    cor::Time whence = parameters.GetTime("whence");


    IOP op;
    try {
        op = mIOPCache.Get(iopUuid);
    } catch (...)
    {
        throw fcgi::Exception(404, "unknown or expired operation ID");
    }

    const cor::TimeRange& tr = op->GetTimeRange();
    if (!tr.In(whence))
        throw fcgi::Exception(400, "Cannot seek to %s, outside of IOP time range (%s)", whence.Print().c_str(), tr.Print().c_str());

    bool b = op->ReadSeek(column, whence);

    // payload
    Json::Value v = b;
    fcgi::Header responseHeader;
    request.Reply(200, responseHeader, v);
}

void
WriteIopHandler::Process(fcgi::Request& request,
                        const fcgi::KeyedFieldMap& keyedFields,
                        const fcgi::ParameterMap& parameters)
{
    std::string iopUuid;
    std::string columnString;

    try {
        iopUuid = keyedFields.at("operation_id");
        columnString = keyedFields.at("column");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing operation ID or column name");
    }


    IOP op;
    try {
        op = mIOPCache.Get(iopUuid);
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(404, "unknown or expired operation ID");
    }

    // read number of records sent
    uint32_t size;
    size = request.GetReader().Read32();
    RecordVector records;
    records.resize(size);

    for (size_t i = 0; i < size; i++)
    {
        Record r;
        int64_t sec = request.GetReader().Read64();
        int64_t asec = request.GetReader().Read64();
        r.mTimeRange.First() = cor::Time(sec, asec);
        sec = request.GetReader().Read64();
        asec = request.GetReader().Read64();
        r.mTimeRange.Final() = cor::Time(sec, asec);
        uint32_t jsonSize = request.GetReader().Read32();
        uint32_t dataSize = request.GetReader().Read32();

        // DEFENSIVE
        // these are intended to stop corrupted packets from causing
        // faults due to stack overflow, etc.  This is not a policy.
        if (jsonSize > JSON_SIZE_LIMIT)
            throw cor::Exception("JSON payload too large (%ld)", jsonSize);
        if (dataSize > RECORD_SIZE_LIMIT)
            throw cor::Exception("Record too large (%ld)", dataSize);

        // searchable JSON
        if (jsonSize != 0)
        {
            char buffer[jsonSize];
            size_t n = request.GetReader().Read(buffer, jsonSize);
            if (n != jsonSize)
            {
                throw fcgi::Exception(400,
                                      "Unexpected EOF in searchable JSON, record %ld of %ld",
                                      i + 1,
                                      size);
            }
            Json::Reader reader;
            if (!reader.parse(buffer, buffer + jsonSize, r.mDataObject))
            {
                throw fcgi::Exception(400,
                                      "Badly formatted searchable JSON, record %ld of %ld: %s",
                                      i + 1,
                                      size,
                                      reader.getFormattedErrorMessages().c_str());
            }
        }

        // data payload
        r.mData.reset(new std::vector<unsigned char>);
        r.mData->resize(dataSize);
        size_t n = request.GetReader().Read((char*)r.mData->data(), dataSize);
        if (n != dataSize)
            throw fcgi::Exception(400, "Unexpected EOF in data payload, record %ld of %ld", i + 1, size);
        records[i] = r;
    }


    MetaKey column(columnString, ':');
    op->Write(column, records);

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader);

}


void
CloseIopHandler::Process(fcgi::Request& request,
                         const fcgi::KeyedFieldMap& keyedFields,
                         const fcgi::ParameterMap& parameters)
{
    std::string iopUuid;

    try {
        iopUuid = keyedFields.at("operation_id");
    } catch (...)
    {
        throw fcgi::Exception(400, "missing operation ID");
    }

    try {
        mIOPCache.Remove(iopUuid);
    } catch (...)
    {
        throw fcgi::Exception(404, "unknown or expired operation ID");
    }

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader);
}


void
CommitIopHandler::Process(fcgi::Request& request,
                         const fcgi::KeyedFieldMap& keyedFields,
                         const fcgi::ParameterMap& parameters)
{
    std::string iopUuid;

    try {
        iopUuid = keyedFields.at("operation_id");
    } catch (...)
    {
        throw fcgi::Exception(400, "missing operation ID");
    }

    std::string action = parameters.GetString("action", "commit");
    bool abort = false;
    if (cor::IEquals(action, "abort"))
        abort = true;
    else if (cor::IEquals(action, "commit"))
        abort = false;
    else
        throw fcgi::Exception(400, "invalid value for parameter 'action', '%s'", action.c_str());

    {
        IOP op;
        try {
            op = mIOPCache.Get(iopUuid);
        } catch (...)
        {
            throw fcgi::Exception(404, "unknown or expired operation ID");
        }

        if (abort)
            op->Abort();
        else
            op->Commit();
    }

    fcgi::Header responseHeader;
    request.Reply(200, responseHeader);

}
    
} // end namespace
