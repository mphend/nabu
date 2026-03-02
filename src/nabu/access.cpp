//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <stdarg.h>

#include <mcor/json_time.h>

#include "access.h"
#include "io_op.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {


Access::Access() :
    mType(eInvalid)
{}

Access::Access(/*const std::string& branchPath,*/
               std::shared_ptr<Branch> branch,
               const muuid::uuid& handle,
               AccessType type,
               const cor::TimeRange& timeRange,
               const std::string& tagName) :
               /*mBranchPath(branchPath),*/
               mBranch(branch),
               mHandle(handle),
               mType(type),
               mTimeRange(timeRange),
               mTagName(tagName)
{
    DEBUG_LOCAL("Access::Access() %s\n", mHandle.string().c_str());
}

Access::~Access()
{
    DEBUG_LOCAL("Access::~Access() %s\n", mHandle.string().c_str());
}


bool
Access::IsRead() const
{
    return mType == eRead;
}

bool
Access::IsWrite() const
{
    return mType == eWrite;
}

bool
Access::Valid() const
{
    return mType != eInvalid;
}

std::shared_ptr<Branch>
Access::GetBranch()
{
    return mBranch;
}

const std::string
Access::GetBranchPath() const
{
    return mBranch->GetLiteral();
}

muuid::uuid
Access::GetHandle() const
{
    return mHandle;
}

void
Access::Open(const char* format, ... )
{
    char buffer[4056]; // lame length assumption

    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end (args);

    while (!Open(1))
    {
        printf("%s...\n", buffer);
    }
}

void
Access::Extents(const MetaKey& column, cor::TimeRange& timeRange)
{
    std::set<MetaKey> columnSet;
    columnSet.insert(column);

    WrittenColumnsMap wcm;
    Extents(columnSet, wcm);

    timeRange = cor::TimeRange(); // invalid

    // wcm should hold, at most, a single timerange in the set, but be
    // safe against it not holding anything
    if (!wcm.empty())
    {
        if (!wcm[column].empty())
            timeRange = *(wcm[column].begin());
    }
}

SelectResult
Access::Select(WrittenColumnsMap& writtenColumnsMap, int waitSeconds)
{
    return Select(std::set<MetaKey>(), writtenColumnsMap, waitSeconds);
}

IOOperationPtr
AccessPtr::CreateIOOperation(const cor::TimeRange& timeRange,
                             IterationDirection direction)
{
    IOOperationPtr iop = operator->()->CreateIOOperation(timeRange, direction);
    iop->SetAccessLifetime(*this);

    return iop;
}

}
