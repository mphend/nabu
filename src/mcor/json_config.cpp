//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <fstream>
#include <sstream>

#include <mcor/mexception.h>
#include <mcor/mfile.h>

#include "json_config.h"


namespace cor
{

JsonConfig::JsonConfig()
{}

JsonConfig::JsonConfig(const Json::Value& v)
{
    Merge(v);
}

void
JsonConfig::Merge(const std::string& file)
{
    Json::Value v;

    try
    {
        Json::Reader r;
        std::ifstream ifs(file);
        if (!r.parse(ifs, v))
            throw cor::Exception("%s", r.getFormattedErrorMessages().c_str());

        Merge(v);

    } catch (const std::exception &err)
    {
        throw cor::Exception("Error reading JSON file '%s': %s", file.c_str(), err.what());
    }
}

bool
JsonConfig::MergeIfExists(const std::string& file)
{
    if (cor::File::Exists(file))
    {
        Merge(file);
        return true;
    }
    return false;
}


void
JsonConfig::Merge(const Json::Value& v)
{
    // add these to the structure, clobbering if necessary
    Json::Value::const_iterator vi = v.begin();
    for (; vi != v.end(); vi++)
        mImp[vi.key().asString()] = *vi;
}

std::string
JsonConfig::Print()
{
    Json::StyledStreamWriter jsw;
    std::ostringstream oss;
    jsw.write(oss, mImp);

    return oss.str();
}

int
JsonConfig::ReadInt(const std::string &key) const
{
    return cor::ReadInt(key, mImp);
}

int
ReadInt(const std::string &key, const Json::Value& at)
{
    if (at[key].isNull())
        throw cor::Exception("Missing configuration key '%s'", key.c_str());
    const Json::Value& v = at[key];
    if (!v.isInt() && !v.isDouble()) // tolerate double, will truncate
        throw cor::Exception("Expected an integer value for '%s'", key.c_str());
    return v.asInt();
}

bool
JsonConfig::ReadBool(const std::string &key) const
{
    return cor::ReadBool(key, mImp);
}

bool
ReadBool(const std::string &key, const Json::Value& at)
{
    if (at[key].isNull())
        throw cor::Exception("Missing configuration key '%s'", key.c_str());
    const Json::Value& v = at[key];
    if (!v.isBool() && !v.isInt()) // allow "0" and "1" as well as "false" and "true"
        throw cor::Exception("Expected a boolean as value for '%s'", key.c_str());
    if (v.isBool())
        return v.asBool();
    return (v.asInt() != 0);
}

double
JsonConfig::ReadDouble(const std::string &key) const
{
    return cor::ReadDouble(key, mImp);
}

double
ReadDouble(const std::string &key, const Json::Value& at)
{
    if (at[key].isNull())
        throw cor::Exception("Missing configuration key '%s'", key.c_str());
    const Json::Value& v = at[key];
    if (!v.isDouble() && !v.isInt()) // JSON demands "4.0", not just "4", but tolerate this
        throw cor::Exception("Expected a double as value for '%s'", key.c_str());
    return v.asDouble();
}

std::string
JsonConfig::ReadString(const std::string &key) const
{
    return cor::ReadString(key, mImp);
}

std::string
ReadString(const std::string &key, const Json::Value& at)
{
    if (at[key].isNull())
        throw cor::Exception("Missing configuration key '%s'", key.c_str());
    const Json::Value& v = at[key];
    if (!v.isString())
        throw cor::Exception("Expected a string as value for '%s'", key.c_str());
    return v.asString();
}

cor::Time
JsonConfig::ReadDate(const std::string& key) const
{
    return cor::ReadDate(key, mImp);
}

bool
JsonConfig::HasMember(const std::string& key) const
{
    return mImp.isMember(key);
}

JsonConfig
JsonConfig::SubConfig(const std::string& key) const
{
    if (HasMember(key))
    {
        Json::Value& v = mImp[key];
        return JsonConfig(v);
    }
    throw cor::Exception("Missing subconfig at '%s'", key.c_str());
}

const Json::Value&
JsonConfig::GetJson(const std::string& key) const
{
    if (mImp[key].isNull())
        throw cor::Exception("Missing configuration key '%s'", key.c_str());

    return mImp[key];
}

const Json::Value&
JsonConfig::GetJson() const
{
    return mImp;
}

cor::Time
ReadDate(const std::string& key, const Json::Value& at)
{
    std::string s = ReadString(key, at);

    cor::Time t;
    try {
        t.FromString(s, "%Y-%m-%d %H:%M:%S");
    }
    catch (const cor::Exception& err)
    {
        throw cor::Exception("Option '%s' must be in format Y-m-d H:M:S", key.c_str());
    }
    return t;
}

} //  end namespace
