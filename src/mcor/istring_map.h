//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef GICM_MCOR_ISTRING_MAP_H
#define GICM_MCOR_ISTRING_MAP_H

#include <map>
#include <string>


namespace cor {


/** class IStringMap : case-insensitive-string keyed map of strings
 *
 *  In other words, a map of string,string where the key is case-insensitive
 */
class IStringMap : public std::map<std::string, std::string>
{
public:
    ~IStringMap();

    // insert a key/value pair in a case-insensitive manner
    virtual void InsertString(const std::string& key, const std::string& value);
    virtual void InsertInt(const std::string& key, int& value);
    virtual void InsertDouble(const std::string& key, double& value);

    // lookup of keys; throws if not found or of not expected type
    virtual std::string ReadString(const std::string& key) const;
    virtual int ReadInt(const std::string& key) const;
    virtual double ReadDouble(const std::string& key) const;

    // convert to string
    virtual std::string Print() const;

private:
    // don't allow public use of this operator; instead, use the Insert*
    // methods to enforce case
    std::string& operator[](const std::string& key)
    {
        return std::map<std::string, std::string>::operator[](key);
    }
};

}

#endif
