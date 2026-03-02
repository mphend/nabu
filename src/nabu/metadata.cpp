//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <fstream>

#include <mcor/mfile.h>
#include <openssl/md5.h>

#include "filenamer.h"
#include "garbage.h"
#include "metadata.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu
{

MetaData::MetaData(const TimePath& path, const muuid::uuid& version) :
    mPath(path),
    mVersion(version)
{}

MetaData::MetaData()
{}

MetaData::~MetaData()
{
}

std::string
MetaData::GetLiteral() const
{
    std::ostringstream oss;
    oss << mVersion;
    return oss.str();
}

bool
MetaData::operator==(const MetaData& other) const
{
    if (other.mPath != mPath)
        return false;
    return (other.mVersion == mVersion);
}

bool
MetaData::operator!=(const MetaData& other) const
{
    return !((*this) == other);
}

} //  end namespace
