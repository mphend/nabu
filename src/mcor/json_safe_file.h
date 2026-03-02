//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef __PKG_JSONCPP_JSON_SAFE_FILE_
#define __PKG_JSONCPP_JSON_SAFE_FILE_

#include <json/value.h>

#include "safe_file.h"

namespace Json
{

class SafeJsonFile : public cor::SafeFile
{
public:
    SafeJsonFile(const std::string& glob) :
            cor::SafeFile(glob)
    {}

    void Parse(cor::File& file, size_t size);
    void Render(cor::File& file);

    Json::Value mRoot;
};

}

#endif
