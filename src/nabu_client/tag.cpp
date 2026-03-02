//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <interweb/request.h>
#include <json/reader.h>
#include <mcor/json_time.h>
#include <json/writer.h>
#include <json/value.h>

#include <mcor/binary.h>
#include <mcor/strutil.h>

#include <nabu/tag.h>
#include <nabu/database.h>

#include "tag.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace nabu {
namespace client {

Tag::Tag(const cor::Url& url,
         nabu::Branch& branch,
         const std::string& tagName,
         const Version& tagVersion) :
         nabu::Tag(branch, tagName, tagVersion),
         mUrl(url)
{
}

Tag::~Tag()
{
}

MetaDataTable
Tag::GetHeadTable()
{
    DEBUG_LOCAL("Tag(client)::GetHeadTable tag=%s\n", GetLiteral().c_str());
    try {

        http::ParameterMap p;
         http::Request rq;

        http::RequestPtr response = rq.Post(mUrl +
                                            "/database/" + GetBranch().GetDatabase().GetInstanceName() +
                                            "/branch/" + GetBranch().GetLiteral() +
                                            "/tag/" + mTagName +
                                            "/head", p);
        response->AssertReturnCode(200);

        MetaDataTable r(new MetaDataTableImp);
        r->LoadJson(*(response->GetJsonPtr()));
        return r;
    }
    catch(cor::Exception& err)
    {
        err.SetWhat("Tag::GetHeadTable() -- %s", err.what());
        throw err;
    }
}

} // end namespace
}
