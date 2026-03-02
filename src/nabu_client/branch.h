//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_CLIENT_BRANCH_H
#define GICM_NABU_CLIENT_BRANCH_H

#include <memory>

#include <nabu/branch.h>
#include <nabu/interface_types.h>
#include <nabu/io_op.h>
#include <nabu/metakey.h>

#include <mcor/profiler.h>
#include <mcor/mtimer.h>
#include <mcor/url.h>


namespace nabu {

class Database;

namespace client {

class Access;
class IOOperation;

/** class Branch :
 */
class Branch : public nabu::Branch
{
public:

    Branch(nabu::Database& database,
           const std::string& branchName);
    virtual ~Branch();


    cor::TimeRange Extents(const MetaKey& column, ExtentsType extentsType);

    size_t Append(const MetaKey& column, const RecordVector& newData);

    void GetTags(std::set<std::string>& tags);

    void MoveHead(const Version& newHeadVersion);
    TagPtr CreateTag(const std::string& tagName, TagPolicy policy, const Version& version);

    TagPtr GetTag(const std::string& tagName);

    bool VersionInUse(const TableAddress& nodeAddress);

    void GetUnusedVersions(const TableAddress& ta, MetaDataMap diffs);

    bool DeleteTag(const std::string& tagName);

    void TrimTags(const std::set<std::string>& tagSet);

    // remove the given column from the database
    void DeleteColumn(const MetaKey& column);

    // return the columns
    void GetColumns(const MetaKey& column, std::vector<MetaKey>& columns);
    bool HasColumn(const MetaKey& column);

    // iterates head, tags, and child branches to return the total node
    // count
    size_t TotalSize();

protected:
};

typedef std::shared_ptr<Branch> BranchPtr;

}

}

#endif
