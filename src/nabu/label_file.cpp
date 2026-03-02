//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <json/writer.h>

#include "database_imp.h"
#include "filenamer.h"
#include "label_file.h"
#include "leak_detector.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)


namespace nabu {


LabelFile::LabelFile(DatabaseImp& database) :
    mDataBase(database)
{
    LeakDetector::Get().Increment("LabelFile");
}

LabelFile::~LabelFile()
{
    LeakDetector::Get().Decrement("LabelFile");
}

void
LabelFile::Load()
{
    DEBUG_LOCAL("LabelFile::Load()%s", "");

    // DEFENSIVE; This should be called only once
    if (mCurrentVersion.Valid())
    {
        //printf("LabelFile::Load() -- labels already loaded\n");
        return;
    }

    // should return "/opt/mag_nabu/labels.*.json", etc.
    std::string searchPath = mDataBase.GetFileNamer().GetLabelFileGlob();

    std::vector<std::string> candidateFiles;

    cor::File::FindFilenames(searchPath, candidateFiles);
    DEBUG_LOCAL("LabelFile::Load() -- Found %ld candidates from search '%s'\n",
                candidateFiles.size(),
                searchPath.c_str());

    // find highest-versioned, valid metadata file
    std::vector<Version> versions;
    std::vector<std::string>::iterator ri = candidateFiles.begin();
    while (ri != candidateFiles.end())
    {
        try {
            versions.push_back(FileNamer::ExtractVersion(*ri));
        }
        catch(const cor::Exception& err)
        {
            DEBUG_LOCAL("LabelFile::Load() -- ignoring versionless-named file %s : %s\n", ri->c_str(), err.what());
            ri = candidateFiles.erase(ri);
            continue;
        }

        Json::Value v;
        try {
            Json::Reader r;
            std::ifstream ifs(*ri);
            if (ifs.bad() || ifs.fail())
                throw cor::Exception("File '%s' is empty", ri->c_str());
            else
            {
                if (!r.parse(ifs, v))
                    throw cor::Exception("%s", r.getFormattedErrorMessages().c_str());
            }
        } catch (const cor::Exception& err)
        {
            DEBUG_LOCAL("LabelFile::Load() -- ignoring corrupted file %s : %s\n",
                        ri->c_str(),
                        err.what());
            versions.erase(versions.end() - 1);
            ri = candidateFiles.erase(ri);
            continue;
        }

        ri++;
    }

    // DEFENSIVE
    if (candidateFiles.size() != versions.size())
        throw cor::Exception("LabelFile::Load() -- error parsing versions");

    if (candidateFiles.empty())
    {
        printf("LabelFile::Load() -- No valid labels or branches found, using default 'main'\n");
    }

    // DEFENSIVE : check for duplicate versions
    {
        std::set<Version> vset;
        for (size_t i = 0; i < versions.size(); i++)
        {
            if (vset.find(versions[i]) != vset.end())
                throw cor::Exception("LabelFile::Load() -- Duplicate versioned (%s) and valid files found in '%s'",
                                     versions[i].GetLiteral().c_str(), searchPath.c_str());
            vset.insert(versions[i]);
        }
    }

    // find most recent version
    uint64_t max = 0;
    size_t maxIndex = 0;
    for (size_t i = 0; i < versions.size(); i++)
    {
        if (versions[i].ToNumber() > max)
        {
            max = versions[i].ToNumber();
            maxIndex = i;
        }
    }
    Version vmax;
    vmax.AsNumber(max);
    DEBUG_LOCAL("LabelFile::Load() -- Latest version is %s out of %ld valid files\n",
                vmax.GetLiteral().c_str(), versions.size());

    // parse the chosen file for real now
    if (!versions.empty())
    {
        try {
            std::ifstream ifs(mDataBase.GetFileNamer().BranchDescriptor(vmax));
            if (ifs.bad() || ifs.fail())
                throw cor::Exception("File is empty");
            else
            {
                Json::Value v;
                Json::Reader r;
                if (!r.parse(ifs, v))
                    throw cor::Exception("%s", r.getFormattedErrorMessages().c_str());

                mDataBase.GetMainImp()->Parse(v);
            }
        } catch (const cor::Exception& err)
        {
            throw cor::Exception("LabelFile::Load() -- Error constructing database branches: %s", err.what());
        }
    }

    // clean up any files found that were valid, but not the max
    for (size_t i = 0; i < versions.size(); i++)
    {
        if (i != maxIndex)
        {
            std::string filename = mDataBase.GetFileNamer().BranchDescriptor(versions[i]);
            printf("Removing label file '%s' (used '%s' instead)\n",
                filename.c_str(),
                mDataBase.GetFileNamer().BranchDescriptor(vmax).c_str());
            cor::File::Delete(filename);
        }
    }

    mCurrentVersion = vmax;
}

void
LabelFile::Save()
{
    DEBUG_LOCAL("LabelFile::Save()%s", "");

    Version newVersion = mCurrentVersion.NextNumber();

    try {
        Json::Value v;
        mDataBase.GetMainImp()->Render(v);

        std::string filename = mDataBase.GetFileNamer().BranchDescriptor(newVersion);
        std::ofstream ofs(filename, std::ios_base::out | std::ios_base::trunc);

        Json::StyledStreamWriter writer;
        writer.write(ofs, v);
    } catch (cor::Exception& err)
    {
        err.SetWhat("LabelFile::Save() -- %s", err.what());
        throw err;
    }

    const std::string oldFileName = mDataBase.GetFileNamer().BranchDescriptor(mCurrentVersion);
    mCurrentVersion = newVersion;
    cor::File::Delete(oldFileName);

    DEBUG_LOCAL("LabelFile::Save() -- saved version %lu\n", mCurrentVersion.ToNumber());
}
    
}
