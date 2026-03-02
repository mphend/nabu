//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __GICM_PKG_NABU_TAG_IMP__
#define __GICM_PKG_NABU_TAG_IMP__

#include <map>
#include <memory>

#include <mcor/locking_accessor.h>
#include <mcor/mmutex.h>

#include "access_table.h"
#include "journal.h"
#include "metadata_table.h"
#include "tag.h"
#include "version.h"


namespace nabu
{

class BranchImp;

/** class Tag
 *
 */

class TagImp : public Tag
{
public:
    TagImp(BranchImp& branch,
           const std::string& tagName,
           const Version& tagVersion,
           MetaDataTable root,
           bool temporary);
    virtual ~TagImp();

    BranchImp& GetBranch() { return mBranch; }
    MetaDataTable GetRoot();

    AccessTable& GetAccessTable() { return mAccessTable; }

    MetaDataTable GetHeadTable();

    bool IsTemporary() const { return mTemporary; }
    bool IsDeleted() const;

    void Delete();

private:

    BranchImp& mBranch;
    MetaDataTable mRoot;

    AccessTable mAccessTable;
    const bool mTemporary;

    mutable cor::MMutex mMutex;
    bool mDeleted;
};


typedef std::shared_ptr<TagImp> TagImpPtr;

}

#endif
