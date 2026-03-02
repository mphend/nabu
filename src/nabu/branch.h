//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_NABU_BRANCH__
#define __PKG_NABU_BRANCH__

#include <string>

#include <mcor/timerange.h>

#include "access.h"
#include "database.h"
#include "exception.h"
#include "interface_types.h"
#include "metakey.h"
#include "record.h"
#include "table_address.h"
#include "tag.h"
#include "version.h"


namespace nabu
{

class DatabaseImp;

/** class Branch
 *
 */

class Branch
{
public:
    Branch(Database& database, const MetaKey& branchPath);
    virtual ~Branch();

    const MetaKey& GetBranchPath() const;
    std::string GetLiteral() const;

    enum ExtentsType { eReadable, eWriteable};
    virtual cor::TimeRange Extents(const MetaKey& column, ExtentsType extentsType) = 0;

    // append newData to column, ignoring data that is already present (has
    // identical or later time)
    // Returns number of items appended.
    virtual size_t Append(const MetaKey& column, const RecordVector& newData) = 0;

    virtual void GetTags(std::set<std::string>& tags) = 0;

    virtual void MoveHead(const Version& newHeadVersion) = 0;
    // create the tag at current head; if 'move' is true, move any
    // existing tag by this name, otherwise throw an exception if the
    // tag is not new
    // If Version is Valid, then that version is used for the tag; otherwise
    // the current head is used
    virtual TagPtr CreateTag(const std::string& tagName, TagPolicy policy, const Version& version) = 0;

    // return the given Tag; if no such tag exists, object will be false
    virtual TagPtr GetTag(const std::string& tagName) = 0;

    virtual bool VersionInUse(const TableAddress& nodeAddress) = 0;

    // filter out the versions in 'diffs' that are in use in this branch,
    // returning only those that are missing.
    virtual void GetUnusedVersions(const TableAddress& ta, MetaDataMap diffs) = 0;

    // delete the tag of the given name; returns whether tag was found.
    // This puts the root of the tag in the garbage, which will result
    // in all files unique to the tag being deleted.
    virtual bool DeleteTag(const std::string& tagName) = 0;
    bool DeleteTag(TagPtr tag) { return DeleteTag(tag->GetLiteral()); }

    // remove all labels (recursively) found in this that are not in source
    void TrimLabels(Database& source);

    // remove all tags from this that are not in tagSet
    virtual void TrimTags(const std::set<std::string>& tagSet) = 0;

    // remove the given column from the database
    virtual void DeleteColumn(const MetaKey& column) = 0;

    // return the columns under 'column'
    virtual void GetColumns(const MetaKey& column, std::vector<MetaKey>& columns) = 0;
    virtual bool HasColumn(const MetaKey& column) = 0;

    // iterates head, tags, and child branches to return the total node
    // count
    virtual size_t TotalSize() = 0;

    Database& GetDatabase() { return mDatabase; }
    const Database& GetDatabase() const { return mDatabase; }

    // Returns the database implementation
    // this should only be invoked on BranchImp objects, which override this
    virtual DatabaseImp& GetDatabaseImp()
    {
        throw nabu::FatalException(mDatabase.GetLiteral(), "GetDatabaseImp() called on a non-concrete Branch instance");
    }
    // Returns the database implementation
    // this should only be invoked on BranchImp objects, which override this
    virtual const DatabaseImp& GetDatabaseImp() const
    {
        throw nabu::FatalException(mDatabase.GetLiteral(), "GetDatabaseImp() called on a non-concrete Branch instance");
    }

protected:
    const MetaKey mBranchPath;
    Database& mDatabase;
};


typedef std::shared_ptr<Branch> BranchPtr;

}

#endif
