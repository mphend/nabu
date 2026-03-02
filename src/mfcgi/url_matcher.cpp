//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <errno.h>
#include <stdio.h>

#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include "url_matcher.h"
#include "exception.h"

namespace fcgi {

const std::string&
KeyedFieldMap::at(const std::string& key) const
{
    try {
        return cor::KeyedFieldMap::at(key);
    } catch (const cor::Exception& err)
    {
        throw fcgi::Exception(400, "%s", err.what());
    }
}


UrlMatcher::UrlMatcher(const std::string& url, const std::string& methods) :
    mUrl(url),
    mMethodsLiteral(methods)
{
    Parse(mUrl, mMethodsLiteral);
}

UrlMatcher::~UrlMatcher()
{
}

void
UrlMatcher::Parse(const std::string& url, const std::string& methods)
{
    cor::ComponentMap keyed;
    cor::ComponentMap staticComponents;

    std::vector<std::string> fields;
    cor::Split(url, "/", fields);
    for (size_t i = 0; i < fields.size(); i++)
    {
        std::string key;
        if (cor::Bookended(fields[i], "<>", key))
        {
            keyed[i] = cor::ToLower(key);
        }
        else
        {
            staticComponents[i] = fields[i];
        }
    }

    mFieldMatcher.SetComponents(staticComponents, keyed);

    if (!methods.empty())
    {
        std::vector<std::string> m;
        cor::Split(methods, "/", m);

        for (size_t i = 0; i < m.size(); i++)
        {
            mMethods.insert(cor::ToUpper(m[i]));
        }
    }
}

bool
UrlMatcher::Equals(const std::string& url, const std::string& method, fcgi::KeyedFieldMap& keyed) const
{
    // if we have specified methods, confirm this is one of them
    if (!mMethods.empty())
    {
        if (mMethods.find(method) == mMethods.end())
            return false;
    }

    std::vector<std::string> fields;
    cor::Split(url,"/", fields);

    return mFieldMatcher.Equals(fields, keyed);
}

bool
UrlMatcher::operator<(const UrlMatcher& other) const
{
    if (mMethods < other.mMethods)
        return true;
    if (mMethods > other.mMethods)
        return false;

    return mFieldMatcher < other.mFieldMatcher;
}

bool
UrlMatcher::operator==(const UrlMatcher& other) const
{
    if (mMethods != other.mMethods)
        return false;

    return mFieldMatcher == other.mFieldMatcher;
}


std::string
UrlMatcher::GetLiteral() const
{
    return mMethodsLiteral + " " + mUrl;
}


}
