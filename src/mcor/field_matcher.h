//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#ifndef PKG_MCOR_FIELD_MATCHER_INCLUDED
#define PKG_MCOR_FIELD_MATCHER_INCLUDED

#include <map>
#include <string>
#include <vector>

#include <mcor/mexception.h>


namespace cor
{


/** class KeyedFieldMap
 *
 */
class KeyedFieldMap : public std::map<std::string, std::string>
{
public:
    void Print() const;

    // lower-case compare, provide a better error message,
    // and make sure exception type is cor::Exception
    const std::string& at(const std::string& key) const;
};


/** class FieldMatcher : matches fields
 *
 *  input is mix of static and keyed components, such
 *  as might be found in a URL. However, the input is a map of these
 *  fields, so the parsing must be done previously. However, using the
 *  URL as an example, and the <> notation designating keyed
 *  components:
    ex)
       "/static1/static2/<key1>/static3/<key2>"

       matched against

       "/static1/static2/asdf/static3/qwer"

       yields a "true" result, with the keyed map containing:
           key1 = "asdf"
           key2 = "qwer"

       static components are case-sensitive
       keyed components are forced to lowercase (this is the keyname, not the
          value, so /<DirEcTion>/ will be found at 'direction' but when
          evaluated, /North/ will be 'North',
          i.e., keyed["direction"] = "North")
 *
 */

// key of this map is the position in the field
// In the example above,
//   "static1" is at 0
//   "static3" is at 3
//   "key1" is at 2
//   etc.
typedef std::map<size_t, std::string> ComponentMap;

// The vector of components
class ComponentVector : public std::vector<std::string>
{
public:
    ComponentVector() : std::vector<std::string>() {}
    ComponentVector(const std::string& literal, const char delimiter = '/');
    ComponentVector(const std::vector<std::string>& other)
    {
        std::vector<std::string>::operator=(other);
    }
    virtual ~ComponentVector() {}

    void SetByLiteral(const std::string& literal, const char delimiter = '/');

    // this is not formal, just a debug string
    std::string GetLiteral() const;
};

class FieldMatcher
{
public:
    FieldMatcher();
    FieldMatcher(const ComponentMap& staticComponents,
                 const ComponentMap& keyedComponents);
    virtual ~FieldMatcher();

    void SetComponents(const ComponentMap& staticComponents,
                       const ComponentMap& keyedComponents);

    // if components match this, returns true and fills
    // in any keyed components into 'keyed'
    // otherwise returns false
    virtual bool Equals(const ComponentVector& components, KeyedFieldMap& keyed) const;

    // used for container key value sorting only
    virtual bool operator<(const FieldMatcher& other) const;
    virtual bool operator==(const FieldMatcher& other) const;

protected:
    ComponentMap mStatic;
    ComponentMap mKeyed;
};

    
}

#endif
