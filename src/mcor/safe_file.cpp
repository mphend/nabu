//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <stdio.h>
#include <sstream>
#include <set>
#include <vector>

#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include <openssl/md5.h>

#include "safe_file.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace cor {


SafeFile::SafeFile(const std::string& pathGlob) :
    mPathGlob(pathGlob),
    mCurrentVersion(0)
{}

SafeFile::~SafeFile()
{}

bool
SafeFile::Load()
{
    DEBUG_LOCAL("SafeFile::Load()%s", "");
    
    std::vector<std::string> candidateFiles;

    cor::File::FindFilenames(mPathGlob, candidateFiles);
    DEBUG_LOCAL("SafeFile::Load() -- Found %zu candidates from search '%s'\n",
                candidateFiles.size(),
                mPathGlob.c_str());

    // find highest-versioned, valid metadata file
    std::vector<uint64_t> versions;
    std::vector<std::string>::iterator ri = candidateFiles.begin();
    while (ri != candidateFiles.end())
    {
        try {
            versions.push_back(ExtractVersion(*ri));
        }
        catch(const cor::Exception& err)
        {
            DEBUG_LOCAL("SafeFile::Load() -- ignoring versionless-named file %s : %s\n", ri->c_str(), err.what());
            ri = candidateFiles.erase(ri);
            continue;
        }

        // test integrity of candidate via CRC
        MD5_CTX md5Ctx;
        MD5_Init(&md5Ctx);

        cor::File file(*ri, O_RDONLY);
        size_t fs = file.Size();
        char buffer[fs + 1];
        size_t n = file.ReadAll(buffer, fs, 0);
        if ((fs < MD5_DIGEST_LENGTH) || (n != fs))
        {
            DEBUG_LOCAL("SafeFile::Load() -- ignoring invalid or corrupted file %s\n",
                        ri->c_str());
            versions.erase(versions.end() - 1);
            ri = candidateFiles.erase(ri);
            continue;
        }

        MD5_Update(&md5Ctx, buffer, fs - MD5_DIGEST_LENGTH);

        unsigned char md[MD5_DIGEST_LENGTH];
        MD5_Final(md, &md5Ctx);

        size_t i;
        for (i = 0; i < MD5_DIGEST_LENGTH; i++)
        {
            if (md[i] != (unsigned char)buffer[fs - MD5_DIGEST_LENGTH + i])
            {
                //printf("    Read CS %s", cor::HexDump(buffer, MD5_DIGEST_LENGTH).c_str());
                //printf("Computed CS %s", cor::HexDump(md, MD5_DIGEST_LENGTH).c_str());
                DEBUG_LOCAL("SafeFile::Load() -- ignoring file %s with bad CRC\n",
                            ri->c_str());
                versions.erase(versions.end() - 1);
                ri = candidateFiles.erase(ri);
                break;
            }
        }
        if (i != MD5_DIGEST_LENGTH)
            continue;

        ri++;
    }

    // SANITY
    if (candidateFiles.size() != versions.size())
        throw cor::Exception("SafeFile::Load() -- error parsing versions");

    if (candidateFiles.empty())
    {
        printf("SafeFile::Load() -- No valid files found\n");
        return false;
    }

    // DEFENSIVE : check for duplicate versions
    {
        std::set<uint64_t> vset;
        for (size_t i = 0; i < versions.size(); i++)
        {
            if (vset.find(versions[i]) != vset.end())
                throw cor::Exception("SafeFile::Load() -- Duplicate versioned (%ld) and valid files found in '%s'",
                                     versions[i], mPathGlob.c_str());
            vset.insert(versions[i]);
        }
    }

    // find most recent version
    uint64_t vmax = 0;
    size_t maxIndex = 0;
    for (size_t i = 0; i < versions.size(); i++)
    {
        if (versions[i] > vmax)
        {
            vmax = versions[i];
            maxIndex = i;
        }
    }


    DEBUG_LOCAL("SafeFile::Load() -- Latest version is %lu out of %zu valid files\n",
                vmax, versions.size());

    // parse the chosen file for real now
    if (!versions.empty())
    {
        cor::File file(candidateFiles[maxIndex], O_RDONLY);
        Parse(file, file.Size() - MD5_DIGEST_LENGTH);
    }

    // clean up any files found that were valid, but not the max
    for (size_t i = 0; i < candidateFiles.size(); i++)
    {
        if (i != maxIndex)
        {
            cor::File::Delete(candidateFiles[i]);
        }
    }

    mCurrentVersion = vmax;
    mCurrentFileName = candidateFiles[maxIndex];
    return true;
}

void
SafeFile::Save()
{
    DEBUG_LOCAL("SafeFile::Save()%s", "");

    uint64_t newVersion = mCurrentVersion + 1;

    cor::File newFile(ProduceVersion(newVersion), O_RDWR | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IROTH);

    try {
        Render(newFile);
    } catch (cor::Exception& err)
    {
        err.SetWhat("SafeFile::Save() -- %s", err.what());
        throw err;
    }

    newFile.Seek(0, SEEK_SET);

    MD5_CTX md5Ctx;
    MD5_Init(&md5Ctx);

    char buffer[10000];
    while (true)
    {
        size_t n = newFile.ReadAll(buffer, sizeof(buffer), 0);
        MD5_Update(&md5Ctx, buffer, n);

        if (n != sizeof(buffer))
            break; // done
    }

    unsigned char md[MD5_DIGEST_LENGTH];
    MD5_Final(md, &md5Ctx);
    newFile.Write((const char*)md, sizeof(md));

    newFile.Sync(); // go to disk

    cor::File::Delete(mCurrentFileName);

    mCurrentFileName = newFile.GetName();
    mCurrentVersion = newVersion;


    DEBUG_LOCAL("SafeFile::Save() -- saved version %lu\n", mCurrentVersion);
}
    
uint64_t
SafeFile::ExtractVersion(const std::string& candidateIn)
{
    std::string::size_type slash = candidateIn.find_last_of('/');
    if (slash == std::string::npos)
    {
        slash = 0; // path is optional; if no slash, assume entire string is filename
    }
    std::string candidate = candidateIn.substr(slash, std::string::npos);

    // find final '.' after the path
    std::string::size_type dotFinal = candidate.find_last_of('.');
    if (dotFinal == std::string::npos)
    {
        throw cor::Exception("Ill-formed file name '%s', ignoring", candidate.c_str());
    }
    // find prior '.'
    std::string::size_type dotPrior = candidate.find_last_of('.', dotFinal - 1);
    if (dotPrior == std::string::npos)
    {
        throw cor::Exception("Ill-formed file name '%s', ignoring", candidate.c_str());
    }
    //printf("dotPrior=%d, dotFinal=%d\n", dot1, dot2);

    // extract version
    std::istringstream iss(candidate.substr(dotPrior + 1, dotFinal - dotPrior - 1));
    //printf("Candidate version = '%s'\n", iss.str().c_str());
    uint64_t version = 0;
    iss >> version;
    if (iss.bad() || iss.fail())
        throw cor::Exception("Unable to interpret apparent version field '%s' to an integer", iss.str().c_str());
    
    return version;
}

std::string
SafeFile::ProduceVersion(uint64_t version)
{
    std::vector<std::string> vs;
    cor::Split(mPathGlob, "*", vs);

    if (vs.size() != 2)
        throw cor::Exception("Search path '%s' does not have exactly one wildcard for version field");

    std::ostringstream oss;
    oss << vs[0] << version << vs[1];

    return oss.str();
}

}
