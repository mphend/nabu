//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_DATABASE_H
#define GICM_NABU_DATABASE_H

#include <memory>
#include <string>

#include <mcor/url.h>

#include "access.h"
#include "metadata_map.h"
#include "metakey.h"
#include "private_types.h"
#include "table_address.h"

namespace nabu
{

class Branch;

/** class Database
 *
 */
class Database
{
public:
    // note this is URL proper: it can be 'file://' as well as 'http://'
    Database(const cor::Url& url);
    virtual ~Database();

    std::string GetLiteral() const;

    const cor::Url& GetUrl() const { return mUrl; }
    virtual const std::string& GetInstanceName() const = 0;

    // this returns the full descriptive name of the database:
    //   URL + instance for a remote instance
    //   pathname for a concrete instance
    // This has no formal use, it is for logging, etc.
    virtual std::string GetDescriptiveLiteral() const = 0;

    // returns null pointer if no such branch exists
    virtual std::shared_ptr<Branch> GetBranch(const MetaKey& branchPath) = 0;
    virtual std::shared_ptr<Branch> GetMain() = 0;
    virtual std::shared_ptr<Branch> CreateBranch(const MetaKey& branchPath) = 0;
    virtual void DeleteBranch(const MetaKey& branchPath) = 0;

    virtual bool VersionInUse(const TableAddress& nodeAddress) = 0;

    // filter out the versions in 'diffs' that are in use in this database,
    // returning only those that are missing.
    virtual void GetUnusedVersions(const TableAddress& ta, MetaDataMap diffs) = 0;

    virtual void CopyFileTo(Database& destination,
                            const TableAddress& tableAddress,
                            FileType fileType) const;
    virtual void CopyFileFrom(const Database& source,
                              const TableAddress& tableAddress,
                              FileType fileType);
    virtual void WriteFile(const std::vector<unsigned char>& data,
                           const TableAddress& tableAddress,
                           FileType fileType) = 0;
    virtual void ReadFile(std::vector<unsigned char>& data,
                          const TableAddress& tableAddress,
                          FileType fileType) const = 0;

    // loads metaDataMap
    virtual bool GetMetadata(MetaDataMap metaDataMap, const TableAddress& ta) const = 0;
    virtual void PersistMetadata(MetaDataMap metaDataMap, const TableAddress& ta) const = 0;

    virtual std::vector<unsigned int> GetPeriods() const = 0;
    virtual muuid::uuid GetFingerprint() const = 0;

    // remove any labels that exist in this but not in source
    void TrimLabels(Database& source);

    // Create an access of the given type on the branch; a tag can be provided
    // to allow creation of IO Operations for a particular tag.
    // Although data for tags can't be written, the tag itself can be moved or
    // deleted, which is a write operation and follows read/write access rules.
    // If the branch or tag does not exist, then a NULL object is returned
    virtual AccessPtr CreateAccess(const MetaKey& branchPath,
                                   const cor::TimeRange& timeRange,
                                   AccessType type,
                                   const std::string tagName = "") = 0;

    // Get information about a column
    //virtual ColumnInfo GetColumnInfo(const MetaKey& column) const = 0;

    // Ensure a column with the given column info exists. If a column already
    // exists with different information, an error is thrown
    //virtual void AddColumn(const MetaKey& column, const ColumnInfo& columnInfo) = 0;

    //virtual void DeleteColumn(const MetaKey& column) = 0;

protected:
    // note this is URL proper: it can be 'file://' as well as 'http://'
    const cor::Url mUrl;

private:
    Database(const Database&);
    void operator=(const Database&);
};

typedef std::shared_ptr<Database> DatabasePtr;

} // end namespace

#endif
