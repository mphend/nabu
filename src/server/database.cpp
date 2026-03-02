//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include "access.h"
#include "database.h"
#include "options.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {
namespace server {

Database::Database(const std::string& rootDirectory, int selectTimeoutSec) :
    DatabaseImp(rootDirectory),
    mAccessTimeoutSeconds(selectTimeoutSec)
{
}

Database::~Database()
{}

AccessPtr
Database::CreateAccess(const MetaKey& branchPath,
                       const cor::TimeRange& timeRange,
                       AccessType type,
                       const std::string& tagName)
{
    // XXX this is mostly a copy of pkg/nabu/database_imp.cpp; not sure if
    // there is a way to factor this out

    DEBUG_LOCAL("Database(server)::CreateAccess %s\n", timeRange.Print().c_str());

    // DEFENSIVE
    if (!mMainBranch)
        throw cor::Exception("DatabaseImp is not initialized");

    AccessPtr newAccess;

    BranchImpPtr branch = GetBranchImp(branchPath);
    if (branch)
    {
        // this will throw if tag does not exist
        try {
            newAccess.reset(new nabu::server::Access(branch, // XXX this is the only change
                                          tagName,
                                          muuid::uuid::random(),
                                          timeRange,
                                          mTimeScheme.Snap(timeRange),
                                          type,
                                          mAccessTimeoutSeconds));
        } catch (const cor::Exception& err)
        {
            printf("Could not create access at tag '%s': tag not found\n", tagName.c_str());
        }
    }
    else
    {
        printf("Could not create access at branch '%s': branch not found\n", branchPath.GetLiteral().c_str());
    }

    return newAccess;
}


} // end namespace
} // end namespace
