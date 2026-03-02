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

#include "config.h"
#include "filenamer.h"
#include "leak_detector.h"
#include "time_scheme.h"

namespace nabu {

Config::Config(const std::string& rootDirectory) : mRootDirectory(rootDirectory)
{
    LeakDetector::Get().Increment("Config");
}

Config::~Config()
{
    LeakDetector::Get().Decrement("Config");
}

bool
Config::Exists() const
{
    return cor::File::Exists(GetConfigFileName());
}

// locates and reads the configuration file, and sets up singletons
// like FileNamer
void
Config::Parse()
{
    const std::string fileName = GetConfigFileName();
    Json::Value v;
    try {
        Json::Reader r;
        std::ifstream ifs(fileName);
        if (!r.parse(ifs, mValue))
            throw cor::Exception("%s", r.getFormattedErrorMessages().c_str());
    } catch (const std::exception& err)
    {
        throw cor::Exception("Error reading config file '%s': %s", fileName.c_str(), err.what());
    }
}

const std::string tab = "    ";
std::string
CommentLine(const std::string& s)
{
    // indent, add '#' as key, and four dashes
    //return tab + "\"#\" : \"---- " + s + "\",\n";

    // strict JSON does not allow multiple members with the same key, so
    // do not try to comment the document for futureproofing in case a
    // different JSON parser is used.
    return std::string();
}

std::string
ValueLine(const std::string& key, int value)
{
    std::ostringstream oss;
    oss << value;
    return tab + "\"" + key + "\" : " + oss.str() + ",\n";
}

std::string
ValueLine(const std::string& key, const std::string& value)
{
    return tab + "\"" + key + "\" : \"" + value + "\",\n";
}

void
Config::Create(const UtcTimeScheme& timeScheme, const muuid::uuid& fingerprint)
{
    if (!TryCreate(timeScheme, fingerprint))
        throw cor::Exception("Refuse to overwrite config file '%s'", GetConfigFileName().c_str());
}

bool
Config::TryCreate(const UtcTimeScheme& timeScheme, const muuid::uuid& fingerprint)
{
    std::string configPath = GetConfigFileName();

    if (cor::File::Exists(configPath))
    {
        //printf("config file '%s' already exists\n", configPath.c_str());
        return false;
    }

    // Because there are psuedo-comment lines that are not part
    // of JSON, and this will be manually edited forever after,
    // don't use JSON writer to render this; just emit (carefully)
    // to the file directly.

    Json::Value ts;
    for (size_t i = 0; i < timeScheme.Size(); i++)
    {
        ts.append(timeScheme.GetPeriods()[i]);
    }

    // make sure if anything exists at root, that it is a directory;
    // otherwise create the directory
    if (cor::File::Exists(mRootDirectory) && !cor::File::IsDirectory(mRootDirectory))
    {
        throw cor::Exception("Non-directory already exists at new root '%s'", mRootDirectory.c_str());
    }
    cor::File::MakePath(mRootDirectory+"/.nabu/");

    {
        std::ofstream ofs(configPath);

        ofs << "{" << std::endl;
        ofs << CommentLine("Nabu configuration file");
        ofs << std::endl;
        ofs << CommentLine("Time scheme");
        ofs << tab + "\"TimeScheme\" : ";
        Json::FastWriter jssw;
        std::string s =  jssw.write(ts);
        s = cor::Trim(s);
        ofs << s << "," << std::endl;
        ofs << std::endl;

        ofs << CommentLine("Fingerprint");
        ofs << ValueLine("Fingerprint", fingerprint.string());
        ofs << std::endl;

        ofs << CommentLine("-------------------------------------------------");
        ofs << CommentLine("     DO NOT CHANGE ANYTHING ABOVE THIS POINT");
        ofs << CommentLine("-------------------------------------------------");
        ofs << std::endl;

        ofs << tab + "\"#\" : \"EOF\"\n";
        ofs << "}" << std::endl;

        // make sure this worked
        if (ofs.bad() || ofs.fail())
            throw cor::Exception("Failed to write initial configuration file at '%s'\n", configPath.c_str());
    }

    // make sure we actually wrote valid JSON
    Json::Value v;
    Json::Reader r;
    std::ifstream ifs(configPath);
    if (!r.parse(ifs, v))
        throw cor::Exception("Internal error rendering new config file: %s", r.getFormattedErrorMessages().c_str());

    return true;
}


UtcTimeScheme
Config::GetTimeScheme() const
{
    std::vector<unsigned int> periods;
    const std::string key = "TimeScheme";
    if (mValue[key].isNull())
        throw cor::Exception("Missing configuration key '%s'", key.c_str());
    const Json::Value& entry = mValue[key];

    if (!entry.isArray())
        throw cor::Exception("Expected Array type for item '%s'", key.c_str());

    size_t length = entry.size();
    if (length == 0)
        throw cor::Exception("Zero length Array for item '%s' isn't allowed", key.c_str());

    periods.resize(length);

    for (int i = 0; i < length; i++)
    {
        periods[i] = entry[i].asDouble();
        if (periods[i] == 0)
            throw cor::Exception("Illegal period of %d found in config\n", periods[i]);
    }

    return nabu::UtcTimeScheme(periods);
}

muuid::uuid
Config::GetFingerprint() const
{
    const std::string key = "Fingerprint";
    muuid::uuid r;
    r.set(ReadString(key));
    return r;
}

std::string
Config::GetConfigFileName() const
{
    return mRootDirectory + "/.nabu/instance.cfg";
}

std::string
Config::ReadString(const std::string &key) const
{
    if (mValue.isMember(key))
    {
        const Json::Value& v = mValue[key];
        if (!v.isString())
            throw cor::Exception("Expected a string as value for '%s'", key.c_str());
        return v.asString();
    }
    throw cor::Exception("Missing configuration key '%s'", key.c_str());
}
} // end namespace