//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <sstream>
#include <stdio.h>

#include <mcor/mfile.h>
#include <mcor/mexception.h>
#include <mcor/strutil.h>
#include <mcor/mtime.h>

#include "filenamer.h"

#define DEBUG 0
#define DEBUG_LOCAL(fmt, ...) \
            do { if (DEBUG) fprintf(stderr, fmt, __VA_ARGS__); } while (0)

namespace nabu
{

FileNamer::FileNamer(const std::string& root) : mRoot(root)
{
    // validate the root
    if (root.empty())
        throw cor::Exception("FileNamer: empty root");

    // make sure this exists as a directory, or not at all
    if (!cor::File::Exists(root))
    {
        cor::File::MakePath(root + "/"); // create it
    }
    else
    {
        if (!cor::File::IsDirectory(root))
            throw  cor::Exception("FileNamer: Root '%s' exists but is not a directory", root.c_str());
    }

    std::vector<std::string> v;
    mRoot = cor::File::CanonicalPath(root, v);

    if (mRoot.empty())
        throw cor::Exception("FileNamer: empty root");

    mRootDepth = v.size();
}

FileNamer::~FileNamer()
{
}


std::string
FileNamer::NameOf(const TableAddress& ta, FileType type, int fileVersion) const
{
    // DEFENSIVE
    if (mRoot.empty())
        throw cor::Exception("FileNamer used before initialization");

    if (type == eJournal)
    {
        if (ta.GetColumn().IsUnknown())
            throw cor::Exception("FileNamer::NameOf() -- 'Invalid' column");

        // XXX journal names will need to include more to support per-branch and
        // per-column appending
        return mRoot + "/" + ta.GetColumn().GetLiteral('/') + "/" +
                "journal." +
                ta.GetVersion().GetLiteral() + "." + Extension(eJournal);
    }

    // for the one root node, avoid an empty name, so 'root.1.met' instead of '.1.met'
    if (ta.GetColumn().IsUnknown())
        return mRoot + "/root." + ta.GetVersion().GetLiteral() + "." + Extension(type);

    std::string pathName = ta.GetTimePath().GetLiteral('_');

    return mRoot + "/" + ta.GetColumn().GetLiteral('/') + "/" + pathName + "." +
        ta.GetVersion().GetLiteral() + "." + Extension(type);
}

std::string
FileNamer::NameOf(const MetaKey& column, const TimePath& timePath, const Version& version, FileType type, int fileVersion) const
{
    TableAddress ta(column, timePath, version);
    return NameOf(ta, type, fileVersion);
}

std::string
FileNamer::FindADataFile(const MetaKey& column) const
{
    if (column.IsUnknown())
        return "";

    std::vector<std::string> files;
    std::string pattern = mRoot + "/" + column.GetLiteral() + "/*." + Extension(eData);
    cor::File::FindFilenames(pattern, files);

    return files.empty() ? "" : files.front();
}

bool
FileNamer::Parse(const std::string& filePath, TableAddress& ta, FileType& type) const
{
    DEBUG_LOCAL("FileNamer::Parse() %s\n", filePath.c_str());
    // DEFENSIVE
    if (mRoot.empty())
        throw cor::Exception("FileNamer used before initialization");

    // split filename into directory components, including final filename
    std::vector<std::string> pathFields;
    cor::Split(cor::Trim(filePath, "/"), "/", pathFields);

    // DEFENSIVE; make sure the file is under the root directory
    if (pathFields.size() <= mRootDepth)
    {
        printf("%s is not inside database root '%s'\n", filePath.c_str(), mRoot.c_str());
    }

    // ignore all files that have '.nabu' as part of their path; they are internal
    for (size_t i = 0; i < pathFields.size(); i++)
    {
        if (pathFields[i] == ".nabu")
        {
            DEBUG_LOCAL("This is a database internal file; ignoring\n%s", "");
            return false;
        }
    }
    // remove leading directories that are part of the root path
    pathFields.erase(pathFields.begin(), pathFields.begin() + mRootDepth);

    DEBUG_LOCAL("   %ld path fields\n", pathFields.size());

    MetaKey column;
    // use the rest minus the final clump (filename) to produce the Address
    for (size_t i = 0; i < pathFields.size() - 1; i++)
    {
        column += pathFields[i];
    }

    DEBUG_LOCAL("   column = %s\n", column.GetLiteral().c_str());

    std::vector<std::string> dotFields;
    DEBUG_LOCAL("   splitting %s with '.'\n", pathFields.back().c_str());
    cor::Split(pathFields.back(), ".", dotFields);

    // DEFENSIVE
    if (dotFields.size() != 3)
    {
        printf("Could not find data/metadata filename components in '%s'", pathFields.back().c_str());
        return false;
    }

    if (dotFields.back() == "json")
        return false;

    // final clump is extension and determines how to parse what precedes it
    type = TypeOf(dotFields.back());

    DEBUG_LOCAL("   type = %s\n", Extension(type).c_str());

    // parse
    //    root.7.met, 0.7.met, 0_0.7.met, 0_0_0.7.dat

    Version version;
    TimePath timePath;
    if ((type == eData) || (type == eMetaData))
    {
        std::vector<std::string> timeFields;
        DEBUG_LOCAL("   splitting %s with '_'\n", dotFields.front().c_str());
        cor::Split(dotFields.front(), "_", timeFields);

        DEBUG_LOCAL("   %ld '_' fields\n", timeFields.size());

        // DEFENSIVE -- sanity
        if (timeFields.empty())
        {
            throw cor::Exception("Split returned empty");
        }

        for (int i = timeFields.size() - 1; i >= 0; i--)
        {
            if (i == 0)
            {
                std::vector<std::string> colonFields;
                cor::Split(timeFields[i], ":", colonFields);
                DEBUG_LOCAL("   %ld ':' fields in timefield %d, '%s'\n", colonFields.size(), i, timeFields[i].c_str());
                if (colonFields.size() == 2)
                {
                    //version.mBranch = colonFields[0];
                }
                if (colonFields.size() == 1)
                {
                    DEBUG_LOCAL("   this is on 'main'%s\n", "");
                    //version.mBranch = "main";
                    if (colonFields[0] == "root")
                    {
                        DEBUG_LOCAL("   this is a root%s\n", "");
                        // DEFENSIVE
                        if (timeFields.size() != 1)
                        {
                            printf("Unexpected filename component 'root' appearing in '%s'\n", dotFields.front().c_str());
                            return false;
                        }
                        timePath = TimePath();
                    }
                    else
                    {
                        DEBUG_LOCAL("   parsing timefield '%s' into integer\n", timeFields[i].c_str());
                        //uint32_t chunk = std::stoul(timeFields[i]);
                        // stoul not available before C++11
                        uint32_t chunk;
                        std::istringstream iss(timeFields[i]);
                        iss >> chunk;
                        timePath.push_back(chunk);
                        DEBUG_LOCAL("      %u\n", chunk);
                    }
                }
                else
                {
                    printf("Illegal branch component '%s' in filename '%s'\n", timeFields[0].c_str(), filePath.c_str());
                    return false;
                }
            }
            else
            {
                try {
                    DEBUG_LOCAL("   parsing timefield %d (%s) into integer\n", i, timeFields[i].c_str());
                    //uint32_t chunk = std::stoul(timeFields[i]);
                    // stoul not available before C++11
                    uint32_t chunk;
                    std::istringstream iss(timeFields[i]);
                    iss >> chunk;
                    timePath.push_back(chunk);
                    DEBUG_LOCAL("      %u\n", chunk);
                }
                catch (...)
                {
                    printf("Could not parse chunk '%s' in '%s'\n", timeFields[i].c_str(), pathFields.front().c_str());
                    return false;
                }
            }
        }

        // get version
        try
        {
            DEBUG_LOCAL("   parsing dot field 1 (%s) into integer\n", dotFields[1].c_str());
            if (!muuid::uuid::is_valid(dotFields[1]))
            {
                throw cor::Exception("Invalid UUID");
            }
            else
            {
                version.mNumber.set(dotFields[1]);
            }
            DEBUG_LOCAL("      %s\n", version.mNumber.string().c_str());
        }
        catch (...)
        {
            printf("Could not parse version field '%s' in '%s'\n", dotFields[1].c_str(), filePath.c_str());
            return false;
        }

        //printf("Parse: %s\n", filePath.c_str());
        //printf("   timepath = %s\n", timePath.GetLiteral().c_str());
        //printf("    version = %s\n", version.GetLiteral().c_str());
    }
    else if (type == eJournal)
    {
        // parsing "journal.00000000-0000-0001-0000-000000000000.jou" or
        //         "journal.00000000-0000-0002-0000-000000000000.jou"
        // and nothing else

        // DEFENSIVE
        if (dotFields[0] != "journal")
        {
            printf("Unexpected filename '%s' for journal file '%s'\n", dotFields[0].c_str(), pathFields.front().c_str());
            return false;
        }

        // get version
        try
        {
            if (!muuid::uuid::is_valid(dotFields[1]))
                throw cor::Exception("Invalid version field '%s' in %s", dotFields[1].c_str(), filePath.c_str());

            version.mNumber.set(dotFields[1]);
            uint64_t n = version.ToNumber();
            // DEFENSIVE
            if ((n < 1) || (n > 2))
            {
                printf("Unexpected version %lu in journal file '%s'\n", n, pathFields.front().c_str());
                return false;
            }
        }
        catch (...)
        {
            printf("Could not parse version field '%s' in '%s'\n", dotFields[1].c_str(), filePath.c_str());
            return false;
        }
    }
    ta = TableAddress(column, timePath, version);
    return true;
}

std::string
FileNamer::BranchDescriptor(const Version& version) const
{
    std::ostringstream oss;
    oss << mRoot << "/.nabu/labels." << version.ToNumber() << ".json";
    return oss.str();
}

std::string
FileNamer::JournalGlob(const MetaKey& branchPath, const MetaKey& column) const
{
    // DEFENSIVE
    if (mRoot.empty())
        throw cor::Exception("FileNamer used before initialization");

    if (column.IsUnknown())
        throw cor::Exception("FileNamer::NameOf() -- 'Invalid' column");

    if (branchPath.IsUnknown())
        return mRoot + "/" + column.GetLiteral('/') + "/journal." + branchPath.GetLiteral('.') + "*.jou";

    // main
    return mRoot + "/" + column.GetLiteral('/') + "/journal.*.jou";
}

// produce the glob for all root files on the given branch
// should return "/opt/mag_nabu/root.BRANCH_A.*.met", etc.
std::string
FileNamer::GetBranchGlob(const MetaKey& branchPath) const
{
    // DEFENSIVE
    if (mRoot.empty())
        throw cor::Exception("FileNamer used before initialization");

    if (!branchPath.IsUnknown())
        return mRoot + "/branches/" + branchPath.GetLiteral('.') + ".head";

    // main
    return mRoot + "/branches/null.head";
}

// produce the glob for all the label description files
// should return "/opt/mag_nabu/labels.*.json", etc.
std::string
FileNamer::GetLabelFileGlob() const
{
    // DEFENSIVE
    if (mRoot.empty())
        throw cor::Exception("FileNamer used before initialization");

    return mRoot + "/.nabu/labels.*.json";
}

// produce the glob for all column description files
// should return "/opt/mag_nabu/.nabu/columns.*.json", etc.
std::string
FileNamer::GetColumnInfoFileGlob() const
{
    // DEFENSIVE
    if (mRoot.empty())
        throw cor::Exception("FileNamer used before initialization");

    return mRoot + "/.nabu/columns.*.json";
}

// return name of JSON file that describes all columns in the database
std::string
FileNamer::ColumnDescriptor(const Version& version) const
{
    std::ostringstream oss;
    oss << mRoot << "/.nabu/columns." << version.ToNumber() << ".json";
    return oss.str();
}

bool
FileNamer::ValidateLabelName(const std::string& candidate, std::string& problem)
{
    // this is not a file name itself, but expectations about it are related
    // to the naming scheme encoded by this class

    if (candidate.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != std::string::npos)
    {
        problem = "non alphanumeric, underscore, or hyphen character(s)";
        return false;
    }
    if (cor::IEquals(candidate, "main"))
    {
        problem = "label 'main' not allowed";
        return false;
    }
    if (cor::IEquals(candidate, "null"))
    {
        problem = "label 'null' not allowed";
        return false;
    }
    if (cor::IEquals(candidate, "nabu"))
    {
        problem = "label 'nabu' not allowed";
        return false;
    }
    if (candidate.empty())
    {
        problem = "empty label not allowed";
        return false;
    }

    return true;
}

std::string
FileNamer::UniqueLabelName()
{
    cor::Time t;
    // want 2PM on March 15, 1971 to be:
    // Mon_Mar_15_1971_2PM_00_00
    const std::string format = "%a_%b_%d_%Y_%I%p_%M_%S";
    std::string s = t.ToString(format.c_str());
    muuid::uuid random = muuid::uuid::random();
    s += "_" + random.string().substr(0,7);
    return s;
}

std::string
FileNamer::Extension(FileType t)
{
    if (t == eData)
        return "dat";
    else if (t == eGarbage)
        return "gar";
    else if (t == eJournal)
        return "jou";
    else // eMetaData or eRoot
        return "met";
}

FileType
FileNamer::TypeOf(const std::string& extension)
{
    if (cor::IEquals(extension, "dat"))
        return eData;
    else if (cor::IEquals(extension, "gar"))
        return eGarbage;
    else if (cor::IEquals(extension, "jou"))
        return eJournal;
    else if (cor::IEquals(extension, "met"))
        return eMetaData;

    throw cor::Exception("Type of extension '%s' is not known", extension.c_str());
}

std::string
FileNamer::GetRoot() const
{
    return mRoot;
}

muuid::uuid
FileNamer::ExtractVersion(const std::string& candidate)
{
    std::string::size_type slash = candidate.find_last_of('/');
    if (slash == std::string::npos)
    {
        slash = 0; // path is optional; if no slash, assume entire string is filename
    }
    // find first '.' after the path
    std::string::size_type dot1 = candidate.find('.', slash);
    if (dot1 == std::string::npos)
    {
        throw cor::Exception("Ill-formed file name '%s', ignoring", candidate.c_str());
    }
    // find second '.'
    std::string::size_type dot2 = candidate.find('.', dot1 + 1);
    if (dot2 == std::string::npos)
    {
        throw cor::Exception("Ill-formed file name '%s', ignoring", candidate.c_str());
    }
    //printf("dot1=%d, dot2=%d\n", dot1, dot2);

    // extract version
    muuid::uuid r;
    std::string versionString(candidate.substr(dot1 + 1, dot2 - dot1 - 1));
    if (muuid::uuid::is_valid(versionString))
    {
        //printf("Candidate version = '%s'\n", iss.str().c_str());
        r.set(versionString);
        //printf("Candidate version = %d\n", version);
    }
    else
    {
        std::istringstream iss(candidate.substr(dot1 + 1, dot2 - dot1 - 1));
        //printf("Candidate version = '%s'\n", iss.str().c_str());
        uint64_t version = 0;
        iss >> version;
        if (iss.bad() || iss.fail())
            throw cor::Exception("Unable to interpret apparent version field '%s' to an integer", iss.str().c_str());
        Version v;
        v.AsNumber(version);
        r = v.mNumber;
    }
    return r;
}

} //  end namespace
