//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include "branch.h"
#include "database.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {

Branch::Branch(Database& database, const MetaKey& branchPath) :
    mDatabase(database),
    mBranchPath(branchPath)
{
}

Branch::~Branch()
{
}

const MetaKey& Branch::GetBranchPath() const
{
    return mBranchPath;
}

std::string
Branch::GetLiteral() const
{
    return mBranchPath.GetLiteral();
}

void
Branch::TrimLabels(Database& source)
{
    BranchPtr otherBranch = source.GetBranch(mBranchPath);

    std::set<std::string> tags;
    otherBranch->GetTags(tags);

    TrimTags(tags);
}
    
}
