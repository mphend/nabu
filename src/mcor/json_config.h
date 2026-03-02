//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_JOPT_JCONFIG_H
#define GICM_JOPT_JCONFIG_H

#include <json/json.h>

#include <mcor/mtime.h>


namespace cor
{

/** Class JsonConfig
 *
 */
class JsonConfig
{
public:
    JsonConfig();
    JsonConfig(const Json::Value& value);

    virtual ~JsonConfig()
    {}

    // Overwrites or adds keys
    void Merge(const std::string& file);
    void Merge(const Json::Value& value);
    // returns whether file was found
    bool MergeIfExists(const std::string& file);

    std::string Print();

    // Convenience methods to fetch a value; will throw if value is not at
    // expected key, or is not of the expected type.

    int ReadInt(const std::string& key) const;
    bool ReadBool(const std::string& key) const;
    double ReadDouble(const std::string& key) const;
    std::string ReadString(const std::string& key) const;
    // format is %Y-%m-%d %H:%M:%S
    cor::Time ReadDate(const std::string& key) const;

    // return if key exists
    bool HasMember(const std::string& key) const;
    // Create a sub-configuration from JSON at given key
    JsonConfig SubConfig(const std::string& key) const;
    // Get the JSON node at the key
    const Json::Value& GetJson(const std::string& key) const;
    // Get the JSON at root
    const Json::Value& GetJson() const;

protected:
    mutable Json::Value mImp;
};

// Convenience methods to fetch a value; will throw if value is not at
// expected key, or is not of the expected type.
int ReadInt(const std::string& key, const Json::Value& v);
bool ReadBool(const std::string& key, const Json::Value& v);
double ReadDouble(const std::string& key, const Json::Value& v);
std::string ReadString(const std::string& key, const Json::Value& v);
// format is %Y-%m-%d %H:%M:%S
cor::Time ReadDate(const std::string& key, const Json::Value& v);


} // end namespace

#endif
