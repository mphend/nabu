//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//



#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include "private_types.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace nabu
{


std::string
FileTypeToString(nabu::FileType type)
{
    if (type == eGarbage)
        return "garbage";
    else if (type == eData)
        return "data";
    else if (type == eMetaData)
        return "metadata";
    else if (type == eJournal)
        return "journal";
    else if (type == eRoot)
        return "root";

    throw cor::Exception("FileTypeToString -- type %d unknown", (int)type);
}

FileType
StringToFileType(const std::string& s)
{
    std::string sc = cor::ToLower(s);
    if (sc == "garbage")
        return eGarbage;
    else if (sc == "data")
        return eData;
    else if (sc == "metadata")
        return eMetaData;
    else if (sc == "journal")
        return eJournal;
    else if (sc == "root")
        return eRoot;

    throw cor::Exception("StringToFileType -- string '%s' unknown", s.c_str());
}


} //  end namespace
