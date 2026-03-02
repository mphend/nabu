//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __GICM_PKG_NABU_TAG__
#define __GICM_PKG_NABU_TAG__

#include <map>
#include <memory>

#include <mcor/locking_accessor.h>
#include <mcor/mmutex.h>

#include "access_table.h"
#include "journal.h"
#include "metadata_table.h"
#include "version.h"

namespace nabu
{

class BranchImp;

/** class Tag
 *
 */

class Tag
{
public:
    Tag(Branch& branch, const std::string& tagName, const Version& tagVersion);
    virtual ~Tag();

    const std::string& GetLiteral() const { return mTagName; }
    Version GetVersion() const { return mVersion; }
    void MoveVersion(const Version& version) { mVersion = version; }

    void RenderToJson(Json::Value& v);

    Branch& GetBranch() { return mBranch; }

    virtual MetaDataTable GetHeadTable() = 0;

protected:
    const std::string mTagName;
    Version mVersion;

    Branch& mBranch;
};


typedef std::shared_ptr<Tag> TagPtr;

}

#endif
