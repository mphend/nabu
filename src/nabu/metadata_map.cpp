//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <fstream>

#include <mcor/mfile.h>
#include <openssl/md5.h>

#include "filenamer.h"
#include "garbage.h"
#include "leak_detector.h"
#include "metadata_map.h"
#include "profiler.h"
#include "version.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu
{

const uint16_t currentVersion = 4;

/* File format
 *
 * (version > 1)
 *    MAGIC                         <string> aeieo
 *    VERSION                       <string> 2
 *
 * (version < 4)
 * NUMBER_OF_ENTRIES       <integer>
 * (version >= 4)
 * NUMBER_OF_ENTRIES  <integer> TYPE <"chunk"|"address">
 *
 * Record Format (version 1)
 *      metakey     version
 *
 * Record Format (version 2)
 *      metakey     branch      version (32-bit unsigned)
 *
 * Record Format (version 3)
 *      metakey     branch      version (16-byte UUID)
 *
  * Record Format (version > 3)
 *      metakey    version (16-byte UUID)
 *
 */

MetaDataMapImp::MetaDataMapImp() : mType(MetaKey::eNone)
{
    LeakDetector::Get().Increment("MetaDataMapImp");
}

MetaDataMapImp::MetaDataMapImp(MetaKey::Type type) : mType(type)
{
    LeakDetector::Get().Increment("MetaDataMapImp");
}

MetaDataMapImp::~MetaDataMapImp()
{
    LeakDetector::Get().Decrement("MetaDataMapImp");
}

const std::string magic = "aeieo";

void
MetaDataMapImp::PersistDocument(std::string& document) const
{
    std::ostringstream oss(document);

    PersistDocument(oss);
    document = oss.str();
}

void
MetaDataMapImp::Persist(const std::string& filename) const
{
    // create any necessary directories
    cor::File::MakePath(filename);

    std::ofstream ofs(filename);

    if (ofs.fail() || ofs.bad())
        throw cor::Exception("Could not open file '%s'", filename.c_str());

    try {
        PersistDocument(ofs);
    }
    catch (cor::Exception& err)
    {
        err.SetWhat("Error with %s: %s", filename.c_str(), err.what());
        throw err;
    }

    // persistence of metadata to disk is ordered to avoid incoherency in the
    // event of an interruption. Do not let the file system cache this and
    // potentially delay this or allow operations to get out of order.
    cor::File::Sync(filename);
}

void
MetaDataMapImp::PersistDocument(std::ostream& os) const
{
    os << magic << std::endl;
    os << currentVersion << std::endl;
    os << Base::size() << " " << std::string(mType == MetaKey::eChunk ? "chunk" : "address") << std::endl;
    if (os.fail() || os.bad())
        throw cor::Exception("Could not write file header");

    // compute md5
    MD5_CTX md5Ctx;
    MD5_Init(&md5Ctx);
    unsigned char md[MD5_DIGEST_LENGTH];

    size_t line = 4;
    Base::const_iterator i = Base::begin();
    for (; i != Base::end(); i++, line++)
    {
        os << i->first << " " << i->second.mVersion.mNumber.string() << std::endl;
        if (os.fail() || os.bad())
            throw cor::Exception("Could not write line %ld", line);

        i->second.mVersion.MD5_Update(md5Ctx);
        i->first.MD5Update(md5Ctx);
    }

    MD5_Final(md, &md5Ctx);
    char md5buffer[MD5_DIGEST_LENGTH * 2];
    char* md5ptr = md5buffer;
    for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
    {
        snprintf(md5ptr, 3, "%02x", md[i]);
        md5ptr += 2;
    }
    os << std::string(md5buffer) << std::endl;

    if (os.fail() || os.bad())
        throw cor::Exception("Could not write file  line %ld (md5)", line);

}

void
MetaDataMapImp::LoadDocument(const std::string &document)
{
    std::istringstream iss(document);
    LoadDocument(iss, false);
}

void
MetaDataMapImp::LoadImp(const std::string& filename, bool permissive)
{
    GetProfiler().Count("MetaDataMap::Read");
    DEBUG_LOCAL("MetaDataMapImp::Load -- loading '%s'\n", filename.c_str());
    std::ifstream ifs(filename);

    LoadDocument(ifs, permissive);
}


void
MetaDataMapImp::LoadDocument(std::istream& ifs, bool permissive)
{
    int formatVersion;
    std::string clump;
    ifs >> clump;

    mType = MetaKey::eNone; // TBD below

    size_t lineNo = 1;

    if (ifs.fail() || ifs.bad())
        throw cor::Exception("incomplete (or corrupted) line %ld", lineNo);

    size_t size;
    if (clump == magic)
    {
        ifs >> formatVersion;
        lineNo++;
        if (ifs.fail() || ifs.bad())
            throw cor::Exception("bad version number on line %ld", lineNo);

        ifs >> size;

        // type is on same line as size, like:
        //    13 address
        if (formatVersion >= 4)
        {
            std::string s;
            ifs >> s;
            if (s == "chunk")
                mType = MetaKey::eChunk;
            else if (s == "address")
                mType = MetaKey::eAddress;
            else // DEFENSIVE!
                throw cor::Exception("Unknown type '%s' in metadata file\n", s.c_str());
        }

        lineNo++;
        if (ifs.fail() || ifs.bad())
            throw cor::Exception("incomplete (or corrupted) size on line %ld", lineNo);
    }
    else
    {
        formatVersion = 1;
        // 'clump' contains length rather than magic, so parse it
        std::istringstream  iss(clump);
        iss >> size;
        if (iss.fail() || iss.bad())
            throw cor::Exception("bad length on line %ld", lineNo);
    }

    DEBUG_LOCAL("MetaDataMapImp::Load -- format version %d, %ld entries\n", formatVersion, size);

    MD5_CTX md5Ctx;
    MD5_Init(&md5Ctx);

    if (permissive)
    {
        char c;
        std::string whitespace(" \n\r\t");
        while(true) {
            c = ifs.peek();
            if (whitespace.find(c) != std::string::npos)
            {
                c = ifs.get(); // consume it
            }
            else
            {
                break;
            }
        }

        if (c == '"')
            mType = MetaKey::eAddress;
        else
            mType = MetaKey::eChunk;
    }

    std::map<MetaKey, MetaData> incoming;
    for (size_t i = 0; i < size; ++lineNo, i++)
    {
        MetaKey key;

        Version version;

        key.Parse(ifs, mType);

        if (formatVersion == 1)
        {
            uint32_t n;
            ifs >> n;
            version.AsNumber(n);
            MD5_Update(&md5Ctx, &n, sizeof(uint32_t));
        }
        else if (formatVersion == 2)
        {
            uint32_t n;
            std::string branchIgnored;
            ifs >> branchIgnored >> n;
            version.AsNumber(n);
            MD5_Update(&md5Ctx, &n, sizeof(uint32_t));
        }
        else if (formatVersion == 3)
        {
            std::string suuid;
            std::string branchIgnored;
            ifs >> branchIgnored >> suuid;
            if (!muuid::uuid::is_valid(suuid))
                throw cor::Exception("line %d: invalid GUID '%s'", lineNo, suuid.c_str());
            version.mNumber.set(suuid);
            version.MD5_Update(md5Ctx);
        }
        else
        {
            std::string suuid;
            ifs >> suuid;
            if (!muuid::uuid::is_valid(suuid))
                throw cor::Exception("line %d: invalid GUID '%s'", lineNo, suuid.c_str());
            version.mNumber.set(suuid);
            version.MD5_Update(md5Ctx);
        }
        if (ifs.fail() || ifs.bad())
            throw cor::Exception("incomplete or corrupted line %d", lineNo);
        MetaData md;
        md.mVersion = version;
        incoming[key] = md;

        key.MD5Update(md5Ctx);
    }

    unsigned char md5[MD5_DIGEST_LENGTH];
    MD5_Final(md5, &md5Ctx);

    std::string md5check;
    ifs >> md5check;
    if (ifs.fail() || ifs.bad())
        throw cor::Exception("incomplete or corrupted md5 in line %d", lineNo + 1);

    // print the parsed and computed MD5s
    /*
    printf("file MD5 is %s\n", md5check.c_str());
    for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
    {
        printf("%02x", md5[i]);
    }
    printf("\n");
    */

    if (!permissive)
    {
        const char* md5ptr = md5check.c_str();
        for (size_t i = 0; i < MD5_DIGEST_LENGTH; i++)
        {
            unsigned int digit;
            sscanf(md5ptr, "%02x", &digit);
            md5ptr += 2;
            if (md5[i] != digit)
                throw cor::Exception("bad md5");
        }
    }

    DEBUG_LOCAL("MetaDataMapImp::Load -- complete%s\n", "");

    Base::swap(incoming);
}

std::string
MetaDataMapImp::Print() const
{
    std::ostringstream oss;
    std::map<MetaKey, MetaData>::const_iterator i = begin();
    for (; i != end(); i++)
    {
        oss << i->first.GetLiteral() << " : " << i->second.GetLiteral() << std::endl;
        //printf("%s : %s\n", i->first.GetLiteral().c_str(), i->second.GetLiteral().c_str());
    }

    return oss.str();;
}


} //  end namespace
