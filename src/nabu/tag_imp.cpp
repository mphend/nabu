//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include "database_imp.h"
#include "branch_imp.h"
#include "tag_imp.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {

TagImp::TagImp(BranchImp& branch,
               const std::string& tagName,
               const Version& tagVersion,
               MetaDataTable root,
               bool temporary) :
    Tag(branch, tagName, tagVersion),
    mBranch(branch),
    mRoot(root),
    mAccessTable(branch.GetLiteral() + "/" + tagName),
    mTemporary(temporary),
    mDeleted(false),
    mMutex(std::string("tag_") + tagName + "_mutex")
{
}

TagImp::~TagImp()
{
    // this object will only be destroyed if the tag has been
    // deleted, and only when the last user of the tag has
    // stopped using it.
    // During the period when "Delete" was called and this moment,
    // the tag is no longer available to open new Accesses, but
    // the data is still around.

    TableAddress ta(MetaKey(), TimePath(), GetVersion());
    mBranch.GetDatabaseImp().GetGarbage().Insert(Garbage::Entry(ta, FileType::eRoot));
}

MetaDataTable
TagImp::GetRoot()
{
    return mRoot;
}

bool
TagImp::IsDeleted() const
{
    cor::ObjectLocker ol(mMutex);
    return mDeleted;
}

void TagImp::Delete()
{
    cor::ObjectLocker ol(mMutex);
    mDeleted = true;
}


MetaDataTable
TagImp::GetHeadTable()
{
    MetaData metaData;
    metaData.mVersion = GetRoot()->GetCurrentVersion(mBranch.GetDatabaseImp(), MetaKey(), TimePath());

    return GetRoot()->GetTable(mBranch.GetDatabaseImp(), MetaKey(), metaData);
}

}
