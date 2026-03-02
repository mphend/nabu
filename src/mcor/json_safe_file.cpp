//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <json/reader.h>
#include <json/writer.h>

#include "json_safe_file.h"


#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace Json {

void
SafeJsonFile::Parse(cor::File& file, size_t size)
{
    std::string document;
    file.ReadAll(document);

    Json::Reader reader;
    if (!reader.parse(document, mRoot))
    {
        throw cor::Exception("%s", reader.getFormattedErrorMessages().c_str());
    }
}

void
SafeJsonFile::Render(cor::File &file)
{
    Json::StyledWriter writer;
    std::string s = writer.write(mRoot);
    file.WriteAll(s.c_str(), s.size());
}
    
}
