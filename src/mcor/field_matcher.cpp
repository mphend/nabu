//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//
#include <stdio.h>

#include <mcor/strutil.h>

#include "field_matcher.h"


namespace cor {

ComponentVector::ComponentVector(const std::string &literal, const char delimiter) :
    std::vector<std::string>()
{
    SetByLiteral(literal, delimiter);
}

void
ComponentVector::SetByLiteral(const std::string& literal, const char delimiter)
{
    clear();
    std::string s;
    s.push_back(delimiter);
    cor::Split(literal, s, *this);
}

std::string
ComponentVector::GetLiteral() const
{
    std::string r;
    for (size_t i = 0; i < size(); i++)
    {
        r += (*this)[i];
        if (i != size() - 1)
            r += "/";
    }
    return r;
}

void
KeyedFieldMap::Print() const
{
    KeyedFieldMap::const_iterator i = begin();
    for (; i != end(); i++)
    {
        printf("%s = %s\n", i->first.c_str(), i->second.c_str());
    }
}

const std::string&
KeyedFieldMap::at(const std::string& key) const
{
    try {
        return std::map<std::string, std::string>::at(cor::ToLower(key));
    } catch (const std::exception& err)
    {
        throw cor::Exception("Missing keyed field '%s'", key.c_str());
    }
}


FieldMatcher::FieldMatcher()
{}

FieldMatcher::FieldMatcher(const ComponentMap& staticComponents,
                           const ComponentMap& keyedComponents) :
        mStatic(staticComponents),
        mKeyed(keyedComponents)
{
}

FieldMatcher::~FieldMatcher()
{
}

void
FieldMatcher::SetComponents(const cor::ComponentMap &staticComponents,
                            const cor::ComponentMap &keyedComponents)
{
    mStatic = staticComponents;
    mKeyed = keyedComponents;
}

bool
FieldMatcher::Equals(const ComponentVector& components, KeyedFieldMap& keyed) const
{
    if (components.size() != mStatic.size() + mKeyed.size())
        return false;

    std::map<size_t, std::string>::const_iterator si = mStatic.begin();
    for (; si != mStatic.end(); si++)
    {
        if (components[si->first] != si->second)
            return false;
    }

    // this is a match; fill in keyed
    std::map<size_t, std::string>::const_iterator ki = mKeyed.begin();
    for (; ki != mKeyed.end(); ki++)
    {
        keyed[ki->second] = components[ki->first];
    }

    return true;
}

bool
FieldMatcher::operator<(const FieldMatcher& other) const
{
    if (mStatic.size() < other.mStatic.size())
        return true;
    if (mStatic.size() > other.mStatic.size())
        return false;

    if (mKeyed.size() < other.mKeyed.size())
        return true;
    if (mKeyed.size() > other.mKeyed.size())
        return false;

    //printf("%s < %s\n", GetLiteral().c_str(), other.GetLiteral().c_str());
    //printf("%ld,%ld vs %ld,%ld\n", mStatic.size(), mKeyed.size(), other.mStatic.size(), other.mKeyed.size());

    // static and keyed are same size, iterate static
    {
        std::map<size_t, std::string>::const_iterator i = mStatic.begin();
        std::map<size_t, std::string>::const_iterator j = other.mStatic.begin();
        for (; i != mStatic.end(); i++, j++)
        {
            if (i->second < j->second)
                return true;
            if (i->second > j->second)
                return false;
        }
    }

    // now keyed
    {
        std::map<size_t, std::string>::const_iterator i = mKeyed.begin();
        std::map<size_t, std::string>::const_iterator j = other.mKeyed.begin();
        for (; i != mKeyed.end(); i++, j++)
        {
            if (i->second < j->second)
                return true;
            if (i->second > j->second)
                return false;
        }
    }

    // equal
    return false;
}

bool
FieldMatcher::operator==(const FieldMatcher& other) const
{
    if (mStatic.size() != other.mStatic.size())
        return false;

    if (mKeyed.size() != other.mKeyed.size())
        return false;

    //printf("%s == %s\n", GetLiteral().c_str(), other.GetLiteral().c_str());
    //printf("%ld,%ld vs %ld,%ld\n", mStatic.size(), mKeyed.size(), other.mStatic.size(), other.mKeyed.size());

    // static and keyed are same size, iterate static
    {
        std::map<size_t, std::string>::const_iterator i = mStatic.begin();
        std::map<size_t, std::string>::const_iterator j = other.mStatic.begin();
        for (; i != mStatic.end(); i++, j++)
        {
            if (i->second != j->second)
                return false;
        }
    }

    // now keyed
    {
        std::map<size_t, std::string>::const_iterator i = mKeyed.begin();
        std::map<size_t, std::string>::const_iterator j = other.mKeyed.begin();
        for (; i != mKeyed.end(); i++, j++)
        {
            if (i->second != j->second)
                return false;
        }
    }

    // equal
    return true;
}

}
