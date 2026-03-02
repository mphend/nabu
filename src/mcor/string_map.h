//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#ifndef GICM_MCOR_STRING_MAP_H
#define GICM_MCOR_STRING_MAP_H

#include <map>
#include <string>


namespace cor {


/** class StringMap : keyed map of strings with convenience methods to
 *  convert to types
 *
 */
class StringMap : public std::map<std::string, std::string>
{
public:
    StringMap();
    virtual ~StringMap();

    // insert a key/value pair in a case-insensitive manner
    virtual void InsertString(const std::string& key, const std::string& value);
    virtual void InsertInt(const std::string& key, int& value);
    virtual void InsertBool(const std::string& key, bool& value); // zero or non-zero
    virtual void InsertDouble(const std::string& key, double& value);

    // lookup of keys; throws if not found or of not expected type
    virtual std::string ReadString(const std::string& key) const;
    virtual int ReadInt(const std::string& key) const;
    virtual bool ReadBool(const std::string& key) const;
    virtual double ReadDouble(const std::string& key) const;

    // convert to string
    virtual std::string Print() const;

    // convert to string representation of a JSON array of key=value strings
    // ala,
    // [ "foo=bar", "baz=goo", ... ]
    virtual std::string ToQuasiJson() const;
    // insert a single "foo=bar" string
    virtual void InsertQuasiJson(const std::string& item);
};

}

#endif
