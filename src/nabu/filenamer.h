//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_FILENAMER_H
#define GICM_NABU_FILENAMER_H

#include <set>
#include <string>

#include <mcor/muuid.h>

#include "metakey.h"
#include "private_types.h"
#include "table_address.h"
#include "time_path.h"
#include "version.h"

namespace nabu
{


/** class FileNamer : codifies the names of files
 *
 */
class FileNamer
{
public:
    FileNamer(const std::string& root);
    virtual ~FileNamer();

    // produce name of given node; if path or version are not applicable for
    // the particular file type, they are ignored.
    std::string NameOf(const TableAddress& ga, FileType type, int fileVersion = 2) const;
    std::string NameOf(const MetaKey& column,
                       const TimePath& timePath,
                       const Version& version,
                       FileType type,
                       int fileVersion = 2) const;

    // return the name of a random datafile in the given column; returns empty
    // string if no such file exists
    // (this is used to deduce column information if the column file is missing)
    std::string FindADataFile(const MetaKey& column) const;

    // inverse of NameOf; parse filename into column, path, version, and type
    bool Parse(const std::string& fileName, TableAddress& ga, FileType& type) const;

    // return name of JSON file that describes all branches and tags in the database
    std::string BranchDescriptor(const Version& version) const;

    // produce the glob of the journal file for the given branch and column
    std::string JournalGlob(const MetaKey& branchPath, const MetaKey& column) const;

    // produce the glob for all root files on the given branch
    // should return "/opt/mag_nabu/root.BRANCH_A.*.met", etc.
    std::string GetBranchGlob(const MetaKey& branchPath) const;

    // produce the glob for all the label description files
    // should return "/opt/mag_nabu/.nabu/labels.*.json", etc.
    std::string GetLabelFileGlob() const;

    // produce the glob for all column description files
    // should return "/opt/mag_nabu/.nabu/columns.*.json", etc.
    std::string GetColumnInfoFileGlob() const;

    // return name of JSON file that describes all columns in the database
    std::string ColumnDescriptor(const Version& version) const;

    // returns true if candidate is a valid tag or branch label, or false
    // if not and provides reason in 'problem'
    static bool ValidateLabelName(const std::string& candidate, std::string& problem);

    // create a unique tag name that complies with label restrictions
    static std::string UniqueLabelName();

    // return the extension of the given type
    static std::string Extension(FileType t);

    // inverse of Extension; return Type based on string
    static FileType TypeOf(const std::string& extension);

    // return the root directory
    std::string GetRoot() const;

    // returns the integer version expressed in the filename at path, or throws if
    // the path does not contain such a version
    static muuid::uuid ExtractVersion(const std::string& path);


private:

    std::string mRoot;
    size_t mRootDepth; // number of directories in mRoot

};

} // end namespace

#endif
