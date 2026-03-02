//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <algorithm>
#include <errno.h>
#include <ftw.h>
#include <unistd.h>

#include "branch.h"
#include "database.h"
#include "leak_detector.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu
{


Database::Database(const cor::Url& url) :
    mUrl(url)
{
    LeakDetector::Get().Increment("Database");
}

Database::~Database()
{
    LeakDetector::Get().Decrement("Database");
}

std::string
Database::GetLiteral() const
{
    return GetUrl().GetLiteral();
}

void
Database::CopyFileTo(Database& destination,
                        const TableAddress& tableAddress,
                        FileType fileType) const
{
    DEBUG_LOCAL("Database::CopyFileTo : to %s from %s (%s)\n",
                destination.GetUrl().GetLiteral().c_str(),
                tableAddress.GetLiteral().c_str(),
                FileTypeToString(fileType).c_str());
    std::vector<unsigned char> buffer;
    ReadFile(buffer, tableAddress, fileType);
    destination.WriteFile(buffer, tableAddress, fileType);
}

void
Database::CopyFileFrom(const Database& source,
                          const TableAddress& tableAddress,
                          FileType fileType)
{
    DEBUG_LOCAL("Database::CopyFileFrom : from %s to %s (%s)\n",
                source.GetUrl().GetLiteral().c_str(),
                tableAddress.GetLiteral().c_str(),
                FileTypeToString(fileType).c_str());
    std::vector<unsigned char> buffer;
    source.ReadFile(buffer, tableAddress, fileType);
    WriteFile(buffer, tableAddress, fileType);
}

void
Database::TrimLabels(Database& source)
{
    DEBUG_LOCAL("Database::TrimLabels : this = %s, source = %s\n",
        GetDescriptiveLiteral().c_str(),
        source.GetDescriptiveLiteral().c_str());
    nabu::BranchPtr main = GetBranch(nabu::MetaKey::MainBranch());
    main->TrimLabels(source);
}


} //  end namespace
