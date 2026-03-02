//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include "branch_imp.h"
#include "tag.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {

Tag::Tag(Branch& branch, const std::string& tagName, const Version& tagVersion) :
    mTagName(tagName),
    mVersion(tagVersion),
    mBranch(branch)
{
}

Tag::~Tag()
{
}

void
Tag::RenderToJson(Json::Value& v)
{
    mVersion.RenderToJson(v);
}


}
