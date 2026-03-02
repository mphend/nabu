//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//


#include <mfcgi/exception.h>

#include "uri_append.h"

#include <nabu/branch.h>

namespace nabu {


void
AppendHandler::Process(fcgi::Request& request,
                        const fcgi::KeyedFieldMap& keyedFields,
                        const fcgi::ParameterMap& parameters)
{
    std::string iopUuid;
    std::string columnString;

    try {
        columnString = keyedFields.at("column");
    } catch (const cor::Exception&)
    {
        throw fcgi::Exception(400, "missing column name");
    }


    // read number of records sent
    uint32_t size;
    size = request.GetReader().Read32();
    RecordVector records;
    records.resize(size);

    for (size_t i = 0; i < size; i++)
    {
        int64_t sec = request.GetReader().Read64();
        int64_t asec = request.GetReader().Read64();
        uint32_t length = request.GetReader().Read32();

        Record r;
        r.SetTime(cor::Time(sec, asec));
        r.mData.reset(new std::vector<unsigned char>);
        r.mData->resize(length);

        size_t n = request.GetReader().Read((char*)r.mData->data(), length);
        if (n != length)
            throw fcgi::Exception(400, "Unexpected EOF");
        records[i] = r;
    }


    MetaKey column(columnString, ':');
    size_t r = 0;
    {
        nabu::BranchPtr master;
        master = GetDatabase(keyedFields)->GetBranch(nabu::MetaKey::MainBranch());
        r = master->Append(column, records);
    }

    Json::Value v;
    v = (int)r;
    fcgi::Header responseHeader;

    request.Reply(200, responseHeader, v);
}


} // end namespace
