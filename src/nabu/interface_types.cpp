//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <algorithm>
#include <fstream>
#include <sstream>

#include <mcor/mfile.h>
#include <mcor/strutil.h>

#include "interface_types.h"

namespace nabu {

TagPolicy
StringToTagPolicy(const std::string& s)
{
    if (cor::IEquals(s, "newonly"))
        return eNewOnly;
    if (cor::IEquals(s, "move"))
        return eMove;
    if (cor::IEquals(s, "temporary"))
        return eTemporary;
    throw cor::Exception("StringToTagPolicy -- '%s' not valid", s.c_str());
}

std::string
TagPolicyToString(TagPolicy policy)
{
    if (policy == eTemporary)
        return "temporary";
    if (policy == eMove)
        return "move";
    if (policy == eNewOnly)
        return "newonly";
    throw cor::Exception("TagPolicyToString -- '%d' not valid", (int)policy);
}

std::string
SelectResultToString(SelectResult r)
{
    if (r == eAbort)
        return "abort";
    else if (r == eTimeout)
        return "timeout";
    else if (r == eWaitAgain)
        return "waitagain";
    else
        return "complete";
}

void
CopyStats::operator+=(const nabu::CopyStats &other)
{
    mCopied += other.mCopied;
    mCompared += other.mCompared;
}

std::string
CopyStats::Print() const
{
    std::ostringstream oss;
    oss << mCompared << " compared, " << mCopied << " copied";
    return oss.str();
}

} // end namespace