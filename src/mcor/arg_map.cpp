//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <iostream>
#include <sstream>

#include <mcor/mexception.h>


#include "arg_map.h"

namespace cor
{

ArgMap::ArgMap()
{
    mRow = mImp.end();
    mNext = 0;
}

ArgMap::~ArgMap()
{}

void
ArgMap::Parse(const char* const argv[], int argc, size_t firstArgIndex)
{
    // step 1; create whitespace-delimited clumps
    std::vector<std::string> clumps;
    for (size_t i = firstArgIndex; i < argc; i++)
    {
        clumps.push_back(argv[i]);
    }


    // step 2; find index of clumps that start with '--'
    std::vector<size_t> flags;
    for (size_t i = 0; i < clumps.size(); i++)
    {
        if (clumps[i].find("--") == 0)
        {
            flags.push_back(i);
        }
    }

    // step 3; assign any clumps between flags as args to the preceding flag
    for (size_t i = 0; i < flags.size(); i++)
    {
        std::string flag = clumps[flags[i]];
        // strip hyphen-hyphen from flag
        flag.erase(0, 2);

        mImp[flag]; // make sure flag exists in map, in case it has no args

        size_t nextFlag;
        if (i != flags.size() - 1)
            nextFlag = flags[i+1];
        else
            nextFlag = clumps.size(); // remainder of clumps on line

        size_t first = flags[i] + 1;

        for (size_t j = first; j < nextFlag; j++)
            mImp[flag].push_back(clumps[j]);
    }
}

size_t
ArgMap::Find(const std::string& key)
{
    mNext = 0;
    mRow = mImp.find(key);
    if (mRow == mImp.end())
        return 0;
    return mRow->second.size() + 1;
}

const std::string&
ArgMap::Get(size_t index)
{
    if (mRow == mImp.end()) // DEFENSIVE
        throw ArgException(cor::Exception("ArgMap: no valid --arg"));

    if ((index + 1) > mRow->second.size())
        throw ArgException(cor::Exception("ArgMap : %d out-of-range for flag '%s'\n", index, mRow->first.c_str()));

    return mRow->second[index];
}

const std::string&
ArgMap::Next()
{
    return Get(mNext++);
}

int
ArgMap::NextInteger()
{
    const std::string s = Next();
    std::istringstream iss(s);
    int i;
    iss >> i;
    if (iss.fail() || iss.bad())
    {
        mNext--;
        throw ArgException(cor::Exception("Expected an integer"));
    }
    return i;
}

double
ArgMap::NextDouble()
{
    std::string s = Next();
    std::istringstream iss(s);
    double d;
    iss >> d;
    if (iss.fail() || iss.bad())
    {
        mNext--;
        throw ArgException(cor::Exception("Expected a double"));
    }
    return d;
}

cor::Time
ArgMap::NextDate()
{
    std::string s = Next();
    return cor::Time(s, "%Y-%m-%d");
}

cor::Time
ArgMap::NextTime()
{
    std::string s = Next();
    return cor::Time(s, "%H:%M:%S");
}

cor::Time
ArgMap::NextDateAndTime()
{
    std::string s = Next();
    s += " " + Next();
    return cor::Time(s, "%Y-%m-%d %H:%M:%S");
}

std::ostream&
ArgMap::StreamIt(std::ostream& os) const
{
    BaseT::const_iterator bci = mImp.begin();
    for (; bci != mImp.end(); bci++)
    {
        os << bci->first;
        const std::vector<std::string>& row = bci->second;
        size_t n = row.size();
        if (n != 0)
            os << " : ";
        for (size_t i = 0; i < row.size(); i++)
        {
            os << row[i] << " ";
        }
        os << std::endl;
    }

    return os;
}

std::ostream& operator<<(std::ostream& os, const ArgMap& am)
{
    return am.StreamIt(os);
}

}
