//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_NABU_CLIENT_TAG_H
#define GICM_NABU_CLIENT_TAG_H

#include <memory>

#include <nabu/branch.h>
#include <nabu/interface_types.h>
#include <nabu/io_op.h>
#include <nabu/metakey.h>

#include <mcor/profiler.h>
#include <mcor/mtimer.h>
#include <mcor/url.h>


namespace nabu {

namespace client {

class Access;
class IOOperation;

/** class Tag :
 */
class Tag : public nabu::Tag
{
public:
    Tag(const cor::Url& url,
        nabu::Branch& branch,
        const std::string& tagName,
        const Version& tagVersion);
    virtual ~Tag();

    MetaDataTable GetHeadTable();

protected:
    const cor::Url mUrl;
};

typedef std::shared_ptr<Tag> TagPtr;

}

}

#endif
