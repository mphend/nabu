//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_METADATA_H
#define GICM_NABU_METADATA_H

#include <memory>

#include <nabu/version.h>

namespace nabu
{

//class MetaDataTableImp;
//typedef std::shared_ptr<MetaDataTableImp> MetaDataTable;

struct MetaData
{
    MetaData(const TimePath& path, const muuid::uuid& version);
    MetaData();
    virtual ~MetaData();

    TimePath mPath;
    Version mVersion;

    std::string GetLiteral() const;

    bool operator==(const MetaData& other) const;
    bool operator!=(const MetaData& other) const;
};



} // end namespace

#endif
